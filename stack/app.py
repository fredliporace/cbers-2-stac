"""App construction"""

from test.conftest import (  # pylint: disable=no-name-in-module, import-error
    create_lambda_layer_from_dir,
)
from typing import Any, Dict, List, Optional

# from aws_cdk import aws_events, aws_events_targets
# from aws_cdk import aws_s3_notifications as s3n
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cw_actions
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as sns_subscriptions
from aws_cdk import aws_sqs as sqs
from aws_cdk import core
from aws_cdk.aws_cloudwatch import ComparisonOperator
from aws_cdk.aws_lambda_event_sources import SqsEventSource

from cbers2stac.layers.common.dbtable import DBTable
from stack.config import StackSettings

# import docker


settings = StackSettings()


class CBERS2STACStack(core.Stack):
    """CBERS2STACStack"""

    lambdas_env_: Dict[str, str] = dict()

    def create_queue(
        self,
        queue_name: str,
        visibility_timeout: Optional[core.Duration] = None,
        dead_letter_queue: Optional[sqs.DeadLetterQueue] = None,
    ) -> sqs.Queue:
        """
        Create a queue with appropriate permissions.
        """
        queue = sqs.Queue(
            self,
            queue_name,
            visibility_timeout=visibility_timeout,
            dead_letter_queue=dead_letter_queue,
        )
        # queue.add_to_resource_policy(
        #     iam.PolicyStatement(
        #         actions=["sqs:*"], principals=[iam.ArnPrincipal(settings.developer_arn)]
        #     )
        # )
        self.lambdas_env_[f"{queue_name}_url"] = queue.queue_url
        self.queues_.append(queue)
        return queue

    def __init__(  # pylint: disable=too-many-locals,too-many-statements
        self,
        scope: core.Construct,
        stack_id: str,
        description: str,
        env: Dict[str, str],
        **kwargs: Any,
    ) -> None:
        """Ctor."""
        super().__init__(scope, stack_id, description=description, *kwargs)

        # All stack queues
        self.queues_: List[sqs.Queue] = list()

        # All stack topics
        self.topics_: List[sns.Topic] = list()

        # Parameters that will not typically change and thus
        # are defined as fixed ENVs
        self.lambdas_env_.update(
            {
                # The CBERS PDS bucket (COGs, metadata)
                "CBERS_PDS_BUCKET": "cbers-pds",
                # The CBERS metadata bucket (metadata only)
                "CBERS_META_PDS_BUCKET": "cbers-meta-pds",
                # If 1 then processed messages are deleted from queues
                "DELETE_MESSAGES": "1",
            }
        )

        # Internal topics
        # General alarm topic to signal problems in stack execution
        # and e-mail subscription
        alarm_topic = sns.Topic(self, "alarm_topic")
        self.topics_.append(alarm_topic)
        alarm_topic.add_subscription(
            sns_subscriptions.EmailSubscription(settings.operator_email)
        )
        # Public STAC item topic for new STAC items
        stac_item_topic = sns.Topic(self, "stac_item_topic")
        self.topics_.append(stac_item_topic)
        sit_policy = iam.PolicyDocument(
            assign_sids=True,
            statements=[
                iam.PolicyStatement(
                    actions=["SNS:Subscribe", "SNS:Receive"],
                    principals=[iam.AnyPrincipal()],
                    resources=[stac_item_topic.topic_arn],
                )
            ],
        )
        sit_policy.add_statements(
            iam.PolicyStatement(
                actions=[
                    "SNS:GetTopicAttributes",
                    "SNS:SetTopicAttributes",
                    "SNS:AddPermission",
                    "SNS:RemovePermission",
                    "SNS:DeleteTopic",
                    "SNS:Subscribe",
                    "SNS:ListSubscriptionsByTopic",
                    "SNS:Publish",
                    "SNS:Receive",
                ],
                principals=[iam.AccountPrincipal(self.account)],
                resources=[stac_item_topic.topic_arn],
            )
        )
        # We could add the document directly to stac_item_policy
        sns.TopicPolicy(
            self,
            "sns_public_topic_policy",
            topics=[stac_item_topic],
            policy_document=sit_policy,
        )
        # Reconcile topic, used internally for reconciliation operations
        reconcile_stac_item_topic = sns.Topic(self, "reconcile_stac_item_topic")
        self.topics_.append(reconcile_stac_item_topic)

        # Update lambdas_env_ with environment options defined
        # in settings
        self.lambdas_env_.update(env)

        # Lambdas list and permissions to be applied
        lambda_perms = list()
        lambdas = list()

        # Create STAC bucket if not configured
        if settings.stac_bucket_name:
            # Use external STAC bucket name
            self.lambdas_env_.update({"CBERS_STAC_BUCKET": settings.stac_bucket_name})
        else:
            # Create and use internal STAC bucket
            stac_working_bucket = s3.Bucket(self, "stac_working_bucket")
            self.lambdas_env_.update(
                {"CBERS_STAC_BUCKET": stac_working_bucket.bucket_name}
            )
            lambda_perms.append(
                iam.PolicyStatement(
                    actions=["s3:PutObject", "s3:PutObjectAcl"],
                    resources=[
                        stac_working_bucket.bucket_arn,
                        f"{stac_working_bucket.bucket_arn}/*",
                    ],
                )
            )

        # General DLQs for lambdas
        general_dlq = self.create_queue("dead_letter_queue")
        general_dlq_alarm = cloudwatch.Alarm(
            self,
            "DLQAlarm",
            metric=general_dlq.metric("ApproximateNumberOfMessagesVisible"),
            evaluation_periods=1,
            threshold=0.0,
            comparison_operator=ComparisonOperator.GREATER_THAN_THRESHOLD,
        )
        general_dlq_alarm.add_alarm_action(cw_actions.SnsAction(alarm_topic))

        # This queue subscribe to CBERS 4/4A quicklooks notifications. The
        # STAC items are generated from the original INPE metadata file as
        # soon as the quicklooks are created in the PDS bucket
        process_new_scenes_queue_dlq = sqs.Queue(
            self,
            "process_new_scenes_queue_dlq",
            retention_period=core.Duration.seconds(1209600),
        )
        self.queues_.append(process_new_scenes_queue_dlq)
        process_new_scenes_queue_alarm = cloudwatch.Alarm(
            self,
            "ProcessNewScenesQueueAlarm",
            metric=process_new_scenes_queue_dlq.metric(
                "ApproximateNumberOfMessagesVisible"
            ),
            evaluation_periods=1,
            threshold=0.0,
            comparison_operator=ComparisonOperator.GREATER_THAN_THRESHOLD,
        )
        process_new_scenes_queue_alarm.add_alarm_action(
            cw_actions.SnsAction(alarm_topic)
        )

        new_scenes_queue = sqs.Queue(
            self,
            "new_scenes_queue",
            visibility_timeout=core.Duration.seconds(385),
            retention_period=core.Duration.seconds(1209600),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3, queue=process_new_scenes_queue_dlq
            ),
        )
        self.queues_.append(new_scenes_queue)
        # Add subscriptions for each CB4 camera
        sns.Topic.from_topic_arn(
            self,
            id="CB4MUX",
            topic_arn="arn:aws:sns:us-east-1:599544552497:NewCB4MUXQuicklook",
        ).add_subscription(sns_subscriptions.SqsSubscription(new_scenes_queue))
        sns.Topic.from_topic_arn(
            self,
            id="CB4AWFI",
            topic_arn="arn:aws:sns:us-east-1:599544552497:NewCB4AWFIQuicklook",
        ).add_subscription(sns_subscriptions.SqsSubscription(new_scenes_queue))
        sns.Topic.from_topic_arn(
            self,
            id="CB4PAN10M",
            topic_arn="arn:aws:sns:us-east-1:599544552497:NewCB4PAN10MQuicklook",
        ).add_subscription(sns_subscriptions.SqsSubscription(new_scenes_queue))
        sns.Topic.from_topic_arn(
            self,
            id="CBPAN5M",
            topic_arn="arn:aws:sns:us-east-1:599544552497:NewCB4PAN5MQuicklook",
        ).add_subscription(sns_subscriptions.SqsSubscription(new_scenes_queue))

        # DynamoDB table
        db_table_schema = DBTable.schema()
        catalog_update_table = dynamodb.Table(
            self,
            db_table_schema["TableName"],
            partition_key=dynamodb.Attribute(
                name=DBTable.pk_attr_name_, type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )
        self.lambdas_env_.update(
            {"CATALOG_UPDATE_TABLE": catalog_update_table.table_name}
        )

        # Common utils lambda layer
        create_lambda_layer_from_dir(
            output_dir="./stack",
            tag="common",
            layer_dir="./cbers2stac/layers/common",
            prefix="python",
        )
        common_layer_asset = aws_lambda.Code.asset("./stack/common.zip")
        common_layer = aws_lambda.LayerVersion(
            self,
            "common_layer",
            code=common_layer_asset,
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_7],
            description="Common utils",
        )

        # Lambdas

        process_new_scene_lambda = aws_lambda.Function(
            self,
            "process_new_scene_lambda",
            code=aws_lambda.Code.from_asset(path="cbers2stac/process_new_scene_queue"),
            handler="code.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            environment={
                **self.lambdas_env_,
                **{
                    "SNS_TARGET_ARN": stac_item_topic.topic_arn,
                    "SNS_RECONCILE_TARGET_ARN": reconcile_stac_item_topic.topic_arn,
                    # This is used for testing, number of messages read from queue
                    # when manually invoking lambda
                    "MESSAGE_BATCH_SIZE": "1",
                },
            },
            timeout=core.Duration.seconds(55),
            dead_letter_queue=process_new_scenes_queue_dlq,
            layers=[common_layer],
            description="Process new scenes from quicklook queue",
        )
        lambdas.append(process_new_scene_lambda)
        process_new_scene_lambda.add_event_source(
            SqsEventSource(queue=new_scenes_queue, batch_size=10)
        )

        # Common lambda permissions
        # Full access to all queues within stack
        lambda_perms.append(
            iam.PolicyStatement(
                actions=["sqs:*"],
                resources=[queue.queue_arn for queue in self.queues_],
            )
        )
        # Full access to all buckets within stack
        if settings.stac_bucket_name:
            lambda_perms.append(
                iam.PolicyStatement(
                    actions=["s3:*"],
                    resources=[
                        stac_working_bucket.bucket_arn,
                        f"{stac_working_bucket.bucket_arn}/*",
                    ],
                )
            )
        # DynamoDB
        lambda_perms.append(
            iam.PolicyStatement(
                actions=["dynamodb:*"],
                resources=[
                    catalog_update_table.table_arn,
                    f"{catalog_update_table.table_arn}/*",
                ],
            )
        )
        # SNS topics
        lambda_perms.append(
            iam.PolicyStatement(
                actions=["sns:*"],
                resources=[topic.topic_arn for topic in self.topics_],
            )
        )

        for perm in lambda_perms:
            for lambda_f in lambdas:
                lambda_f.add_to_role_policy(perm)


app = core.App()

# Tag infrastructure
for key, value in {
    "project": f"{settings.name}-{settings.stage}",
    "cost_center": settings.cost_center,
}.items():
    if value:
        core.Tag.add(app, key, value)

stackname = f"{settings.name}-{settings.stage}"
CBERS2STACStack(
    scope=app,
    stack_id=stackname,
    env=settings.additional_env,
    description=settings.description,
)

app.synth()
