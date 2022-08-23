"""App construction"""

from test.conftest import (  # pylint: disable=no-name-in-module, import-error
    create_lambda_layer_from_dir,
)
from typing import Any, Dict, List

# from aws_cdk import aws_s3_notifications as s3n
from aws_cdk import aws_apigateway as apigateway
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cw_actions
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_elasticsearch as elasticsearch
from aws_cdk import aws_events, aws_events_targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_assets as s3_assets
from aws_cdk import aws_s3_deployment as s3_deployment
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as sns_subscriptions
from aws_cdk import aws_sqs as sqs
from aws_cdk import aws_synthetics as synthetics
from aws_cdk import core
from aws_cdk.aws_cloudwatch import ComparisonOperator
from aws_cdk.aws_lambda_event_sources import SqsEventSource

from cbers2stac.layers.common.dbtable import DBTable
from cbers2stac.local.create_static_catalog_structure import (
    create_local_catalog_structure,
)
from stack.config import StackSettings

settings = StackSettings()


class CBERS2STACStack(core.Stack):
    """CBERS2STACStack"""

    lambdas_env_: Dict[str, str] = {}

    def create_queue(self, **kwargs: Any) -> sqs.Queue:
        """
        Create a queue, keep stack internal references updated:
        include queue in internal queues list
        include queue url in the environment variables to all lambdas
        """
        queue = sqs.Queue(self, **kwargs)
        self.lambdas_env_[f"{kwargs['id']}_url"] = queue.queue_url
        self.queues_[kwargs["id"]] = queue
        return queue

    def create_lambda(self, **kwargs: Any) -> aws_lambda.Function:
        """
        Create a lambda function and keep stack internal references updated.
        """
        lfun = aws_lambda.Function(self, **kwargs)
        self.lambdas_[kwargs["id"]] = lfun
        return lfun

    def create_api_lambda(self, **kwargs: Any) -> aws_lambda.Function:
        """
        Create a lambda function that integrates with API gateway
        These lambdas are kept in a separate list with separate permissions,
        and its IDs are overridden to allow reference in the OpenAPI definition
        file
        """
        assert "_" not in kwargs["id"]
        lfun = aws_lambda.Function(self, **kwargs)
        lel_id = lfun.node.default_child
        lel_id.override_logical_id(kwargs["id"])
        self.api_lambdas_[kwargs["id"]] = lfun

    def create_all_queues(self) -> None:
        """
        Create all STACK queues, attach subscriptions and alarms
        """

        # General DLQs for lambdas (not API)
        self.create_queue(id="dead_letter_queue")
        general_dlq_alarm = cloudwatch.Alarm(
            self,
            "DLQAlarm",
            metric=self.queues_["dead_letter_queue"].metric(
                "ApproximateNumberOfMessagesVisible"
            ),
            evaluation_periods=1,
            threshold=0.0,
            comparison_operator=ComparisonOperator.GREATER_THAN_THRESHOLD,
        )
        general_dlq_alarm.add_alarm_action(
            cw_actions.SnsAction(self.topics_["alarm_topic"])
        )

        # DLQ for API lambdas
        self.create_queue(id="api_dead_letter_queue")
        api_dlq_alarm = cloudwatch.Alarm(
            self,
            "APIDLQAlarm",
            metric=self.queues_["api_dead_letter_queue"].metric(
                "ApproximateNumberOfMessagesVisible"
            ),
            evaluation_periods=1,
            threshold=0.0,
            comparison_operator=ComparisonOperator.GREATER_THAN_THRESHOLD,
        )
        api_dlq_alarm.add_alarm_action(
            cw_actions.SnsAction(self.topics_["alarm_topic"])
        )

        # The new_scenes_queue subscribe to CBERS 4/4A quicklooks notifications. The
        # STAC items are generated from the original INPE metadata file as
        # soon as the quicklooks are created in the PDS bucket
        # This code fragment creates the queue, the associated dlq and
        # subscribe to CBERS 4/4A quicklook notification topics
        self.create_queue(
            id="process_new_scenes_queue_dlq",
            retention_period=core.Duration.seconds(1209600),
        )
        process_new_scenes_queue_alarm = cloudwatch.Alarm(
            self,
            "ProcessNewScenesQueueAlarm",
            metric=self.queues_["process_new_scenes_queue_dlq"].metric(
                "ApproximateNumberOfMessagesVisible"
            ),
            evaluation_periods=1,
            threshold=0.0,
            comparison_operator=ComparisonOperator.GREATER_THAN_THRESHOLD,
        )
        process_new_scenes_queue_alarm.add_alarm_action(
            cw_actions.SnsAction(self.topics_["alarm_topic"])
        )
        self.create_queue(
            id="new_scenes_queue",
            visibility_timeout=core.Duration.seconds(385),
            retention_period=core.Duration.seconds(1209600),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=1, queue=self.queues_["process_new_scenes_queue_dlq"]
            ),
        )
        # Add subscriptions for each CB4 camera
        sns.Topic.from_topic_arn(
            self,
            id="CB4MUX",
            topic_arn="arn:aws:sns:us-east-1:599544552497:NewCB4MUXQuicklook",
        ).add_subscription(
            sns_subscriptions.SqsSubscription(self.queues_["new_scenes_queue"])
        )
        sns.Topic.from_topic_arn(
            self,
            id="CB4AWFI",
            topic_arn="arn:aws:sns:us-east-1:599544552497:NewCB4AWFIQuicklook",
        ).add_subscription(
            sns_subscriptions.SqsSubscription(self.queues_["new_scenes_queue"])
        )
        sns.Topic.from_topic_arn(
            self,
            id="CB4PAN10M",
            topic_arn="arn:aws:sns:us-east-1:599544552497:NewCB4PAN10MQuicklook",
        ).add_subscription(
            sns_subscriptions.SqsSubscription(self.queues_["new_scenes_queue"])
        )
        sns.Topic.from_topic_arn(
            self,
            id="CBPAN5M",
            topic_arn="arn:aws:sns:us-east-1:599544552497:NewCB4PAN5MQuicklook",
        ).add_subscription(
            sns_subscriptions.SqsSubscription(self.queues_["new_scenes_queue"])
        )
        # Subscription for CB4A (all cameras)
        sns.Topic.from_topic_arn(
            self, id="CB4A-AM1", topic_arn=settings.cb4a_am1_topic,
        ).add_subscription(
            sns_subscriptions.SqsSubscription(self.queues_["new_scenes_queue"])
        )

        self.create_queue(
            id="catalog_prefix_update_queue",
            visibility_timeout=core.Duration.seconds(60),
            retention_period=core.Duration.seconds(1209600),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3, queue=self.queues_["dead_letter_queue"]
            ),
        )

        # Reconcile queue for INPE's XML metadata
        self.create_queue(
            id="consume_reconcile_queue_dlq",
            retention_period=core.Duration.seconds(1209600),
        )
        consume_reconcile_queue_alarm = cloudwatch.Alarm(
            self,
            "ConsumeReconcileQueueAlarm",
            metric=self.queues_["consume_reconcile_queue_dlq"].metric(
                "ApproximateNumberOfMessagesVisible"
            ),
            evaluation_periods=1,
            threshold=0.0,
            comparison_operator=ComparisonOperator.GREATER_THAN_THRESHOLD,
        )
        consume_reconcile_queue_alarm.add_alarm_action(
            cw_actions.SnsAction(self.topics_["alarm_topic"])
        )
        self.create_queue(
            id="reconcile_queue",
            visibility_timeout=core.Duration.seconds(1000),
            retention_period=core.Duration.seconds(1209600),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3, queue=self.queues_["consume_reconcile_queue_dlq"]
            ),
        )

        # Reconcile queue for STAC items
        self.create_queue(
            id="consume_stac_reconcile_queue_dlq",
            retention_period=core.Duration.seconds(1209600),
        )
        consume_stac_reconcile_queue_alarm = cloudwatch.Alarm(
            self,
            "ConsumeStacReconcileQueueAlarm",
            metric=self.queues_["consume_stac_reconcile_queue_dlq"].metric(
                "ApproximateNumberOfMessagesVisible"
            ),
            evaluation_periods=1,
            threshold=0.0,
            comparison_operator=ComparisonOperator.GREATER_THAN_THRESHOLD,
        )
        consume_stac_reconcile_queue_alarm.add_alarm_action(
            cw_actions.SnsAction(self.topics_["alarm_topic"])
        )
        self.create_queue(
            id="stac_reconcile_queue",
            visibility_timeout=core.Duration.seconds(1000),
            retention_period=core.Duration.seconds(1209600),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=self.queues_["consume_stac_reconcile_queue_dlq"],
            ),
        )

        # Queue for STAC items to be inserted into Elasticsearch. Subscribe to the
        # topic with new stac items
        self.create_queue(
            id="insert_into_elasticsearch_queue",
            visibility_timeout=core.Duration.seconds(180),
            retention_period=core.Duration.seconds(1209600),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3, queue=self.queues_["dead_letter_queue"]
            ),
        )
        # Subscription for new item topics
        self.topics_["stac_item_topic"].add_subscription(
            sns_subscriptions.SqsSubscription(
                self.queues_["insert_into_elasticsearch_queue"]
            )
        )
        # Subscription for reconciled item topics
        self.topics_["reconcile_stac_item_topic"].add_subscription(
            sns_subscriptions.SqsSubscription(
                self.queues_["insert_into_elasticsearch_queue"]
            )
        )

        # Backup queue for STAC items inserted into Elasticsearch.
        # This holds the same items received by "insert_into_elasticsearch_queue",
        # simply holding them for some time to allow recover from ES
        # cluster failures (see #78)
        # This queue subscribe only to new item topics
        self.create_queue(
            id="backup_insert_into_elasticsearch_queue",
            visibility_timeout=core.Duration.seconds(180),
            retention_period=core.Duration.days(settings.backup_queue_retention_days),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3, queue=self.queues_["dead_letter_queue"]
            ),
        )
        # Subscription for new item topics
        self.topics_["stac_item_topic"].add_subscription(
            sns_subscriptions.SqsSubscription(
                self.queues_["backup_insert_into_elasticsearch_queue"]
            )
        )

    def create_all_topics(self) -> None:
        """
        Create all stack topics
        """
        # Internal topics
        # General alarm topic to signal problems in stack execution
        # and e-mail subscription
        self.topics_["alarm_topic"] = sns.Topic(self, "alarm_topic")
        self.topics_["alarm_topic"].add_subscription(
            sns_subscriptions.EmailSubscription(settings.operator_email)
        )
        # Public STAC item topic for new STAC items
        self.topics_["stac_item_topic"] = sns.Topic(self, "stac_item_topic")
        core.CfnOutput(
            self,
            "stac_item_topic_output",
            value=self.topics_["stac_item_topic"].topic_arn,
            description="STAC item topic",
        )
        sit_policy = iam.PolicyDocument(
            assign_sids=True,
            statements=[
                iam.PolicyStatement(
                    actions=["SNS:Subscribe", "SNS:Receive"],
                    principals=[iam.AnyPrincipal()],
                    resources=[self.topics_["stac_item_topic"].topic_arn],
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
                resources=[self.topics_["stac_item_topic"].topic_arn],
            )
        )
        # We could add the document directly to stac_item_policy
        sns.TopicPolicy(
            self,
            "sns_public_topic_policy",
            topics=[self.topics_["stac_item_topic"]],
            policy_document=sit_policy,
        )
        # Reconcile topic, used internally for reconciliation operations
        self.topics_["reconcile_stac_item_topic"] = sns.Topic(
            self, "reconcile_stac_item_topic"
        )

    def create_all_layers(self) -> None:
        """
        All stack layers
        """
        # Common utils lambda layer
        create_lambda_layer_from_dir(
            output_dir="./stack",
            tag="common",
            layer_dir="./cbers2stac/layers/common",
            prefix="python",
        )
        common_layer_asset = aws_lambda.Code.asset("./stack/common.zip")
        self.layers_["common_layer"] = aws_lambda.LayerVersion(
            self,
            "common_layer",
            code=common_layer_asset,
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_7],
            description="Common utils",
        )

    def create_all_lambdas(self) -> None:
        """
        Create all lambda functions and associated triggers
        """
        self.create_lambda(
            id="process_new_scene_lambda",
            code=aws_lambda.Code.from_asset(path="cbers2stac/process_new_scene_queue"),
            handler="code.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            environment={
                **self.lambdas_env_,
                **{
                    "SNS_TARGET_ARN": self.topics_["stac_item_topic"].topic_arn,
                    "SNS_RECONCILE_TARGET_ARN": self.topics_[
                        "reconcile_stac_item_topic"
                    ].topic_arn,
                    # This is used for testing, number of messages read from queue
                    # when manually invoking lambda
                    "MESSAGE_BATCH_SIZE": "1",
                },
            },
            timeout=core.Duration.seconds(55),
            dead_letter_queue=self.queues_["process_new_scenes_queue_dlq"],
            layers=[self.layers_["common_layer"]],
            description="Process new scenes from quicklook queue",
        )
        self.lambdas_["process_new_scene_lambda"].add_event_source(
            SqsEventSource(queue=self.queues_["new_scenes_queue"], batch_size=10)
        )
        # See comment below on using from_bucket_name to
        # create a CDK bucket
        read_cbers_pds_permissions = iam.PolicyStatement(
            actions=["s3:ListObjectsV2", "s3:ListBucket", "s3:Get*"],
            resources=["arn:aws:s3:::cbers-pds", "arn:aws:s3:::cbers-pds/*",],
        )
        self.lambdas_["process_new_scene_lambda"].add_to_role_policy(
            read_cbers_pds_permissions
        )

        self.create_lambda(
            id="generate_catalog_levels_to_be_updated_lambda",
            code=aws_lambda.Code.from_asset(
                path="cbers2stac/generate_catalog_levels_to_be_updated"
            ),
            handler="code.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            environment={
                **self.lambdas_env_,
                **{
                    "CATALOG_PREFIX_UPDATE_QUEUE": self.queues_[
                        "catalog_prefix_update_queue"
                    ].queue_url
                },
            },
            timeout=core.Duration.seconds(900),
            dead_letter_queue=self.queues_["dead_letter_queue"],
            layers=[self.layers_["common_layer"]],
            description="Generate levels into output table from input table",
        )

        self.create_lambda(
            id="update_catalog_prefix_lambda",
            code=aws_lambda.Code.from_asset(path="cbers2stac/update_catalog_tree"),
            handler="code.trigger_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            environment={**self.lambdas_env_,},
            timeout=core.Duration.seconds(55),
            dead_letter_queue=self.queues_["dead_letter_queue"],
            layers=[self.layers_["common_layer"]],
            description="Update catalog from prefix",
        )
        self.lambdas_["update_catalog_prefix_lambda"].add_event_source(
            SqsEventSource(
                queue=self.queues_["catalog_prefix_update_queue"], batch_size=10
            )
        )

        self.create_lambda(
            id="populate_reconcile_queue_lambda",
            code=aws_lambda.Code.from_asset(path="cbers2stac/populate_reconcile_queue"),
            handler="code.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            environment={
                **self.lambdas_env_,
                **{"RECONCILE_QUEUE": self.queues_["reconcile_queue"].queue_url},
            },
            timeout=core.Duration.seconds(300),
            dead_letter_queue=self.queues_["dead_letter_queue"],
            layers=[self.layers_["common_layer"]],
            description="Populates reconcile queue with S3 keys from a common prefix",
        )

        # I'm using the bucket ARN directly here just to make sure that I don't
        # mess with the cbers-pds bucket... creating it from_bucket_name should
        # be safe but I'll not take my chances
        # cbers_pds_bucket = s3.Bucket.from_bucket_name(self, "cbers-pds", "cbers-pds")
        list_cbers_pds_permissions = iam.PolicyStatement(
            actions=["s3:ListObjectsV2", "s3:ListBucket"],
            resources=["arn:aws:s3:::cbers-pds", "arn:aws:s3:::cbers-pds/*",],
        )
        self.lambdas_["populate_reconcile_queue_lambda"].add_to_role_policy(
            list_cbers_pds_permissions
        )

        self.create_lambda(
            id="consume_reconcile_queue_lambda",
            code=aws_lambda.Code.from_asset(path="cbers2stac/consume_reconcile_queue"),
            handler="code.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            environment={
                **self.lambdas_env_,
                **{"NEW_SCENES_QUEUE": self.queues_["new_scenes_queue"].queue_url},
            },
            timeout=core.Duration.seconds(900),
            dead_letter_queue=self.queues_["consume_reconcile_queue_dlq"],
            layers=[self.layers_["common_layer"]],
            description="Consume dirs from reconcile queue, populating "
            "new_scenes_queue with quicklooks to be processed",
        )
        self.lambdas_["consume_reconcile_queue_lambda"].add_to_role_policy(
            list_cbers_pds_permissions
        )
        self.lambdas_["consume_reconcile_queue_lambda"].add_event_source(
            SqsEventSource(queue=self.queues_["reconcile_queue"], batch_size=5)
        )

        # Section with lambdas used to support STAC API. Specific lambdas integrated
        # with API GW are defined in create_api_lambdas()
        if settings.enable_api:

            self.create_lambda(
                id="create_elastic_index_lambda",
                code=aws_lambda.Code.from_asset(path="cbers2stac/elasticsearch"),
                handler="es.create_stac_index_handler",
                runtime=aws_lambda.Runtime.PYTHON_3_7,
                environment={**self.lambdas_env_,},
                layers=[self.layers_["common_layer"]],
                timeout=core.Duration.seconds(30),
                dead_letter_queue=self.queues_["dead_letter_queue"],
                description="Create Elasticsearch stac index",
            )

            self.create_lambda(
                id="insert_into_elastic_lambda",
                code=aws_lambda.Code.from_asset(path="cbers2stac/elasticsearch"),
                handler="es.create_documents_handler",
                runtime=aws_lambda.Runtime.PYTHON_3_7,
                environment={
                    **self.lambdas_env_,
                    **{"ES_STRIPPED": "YES", "BULK_CALLS": "1", "BULK_SIZE": "10"},
                },
                layers=[self.layers_["common_layer"]],
                timeout=core.Duration.seconds(30),
                dead_letter_queue=self.queues_["dead_letter_queue"],
                # Concurrent executions tuned to work with t2.small.elasticsearch
                reserved_concurrent_executions=5,
                description="Consume STAC items from queue, inserting into ES",
            )
            self.lambdas_["insert_into_elastic_lambda"].add_event_source(
                SqsEventSource(
                    queue=self.queues_["insert_into_elasticsearch_queue"], batch_size=10
                )
            )

            self.create_lambda(
                id="consume_stac_reconcile_queue_lambda",
                code=aws_lambda.Code.from_asset(path="cbers2stac/reindex_stac_items"),
                handler="code.consume_stac_reconcile_queue_handler",
                runtime=aws_lambda.Runtime.PYTHON_3_7,
                environment=self.lambdas_env_,
                layers=[self.layers_["common_layer"]],
                timeout=core.Duration.seconds(900),
                description="Reindex STAC items from a prefix",
            )
            # Batch size changed from 5 to 2 to reduce the lambda work and increase
            # the chances to make it fit within the 900s limit.
            self.lambdas_["consume_stac_reconcile_queue_lambda"].add_event_source(
                SqsEventSource(queue=self.queues_["stac_reconcile_queue"], batch_size=2)
            )

            self.create_lambda(
                id="populate_stac_reconcile_queue_lambda",
                code=aws_lambda.Code.from_asset(path="cbers2stac/reindex_stac_items"),
                handler="code.populate_stac_reconcile_queue_handler",
                runtime=aws_lambda.Runtime.PYTHON_3_7,
                environment={**self.lambdas_env_,},
                timeout=core.Duration.seconds(300),
                dead_letter_queue=self.queues_["dead_letter_queue"],
                layers=[self.layers_["common_layer"]],
                description="Populates reconcile queue with STAC items from a common prefix",
            )

    def create_api_lambdas(self) -> None:
        """
        Create lambdas implementing the STAC API. The logical IDs for
        lambdas created here are overriden to allow reference in the OpenAPI
        file, see create_api_gateway
        """

        # Section with lambdas integrated with API GW
        self.create_api_lambda(
            id="LandingEndpointLambda",
            code=aws_lambda.Code.from_asset(path="cbers2stac/stac_endpoint"),
            handler="code.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            environment={**self.lambdas_env_,},
            layers=[self.layers_["common_layer"]],
            timeout=core.Duration.seconds(30),
            dead_letter_queue=self.queues_["api_dead_letter_queue"],
            description="Implement / endpoint (landing page)",
        )
        self.create_api_lambda(
            id="SearchEndpointLambda",
            code=aws_lambda.Code.from_asset(path="cbers2stac/elasticsearch"),
            handler="es.stac_search_endpoint_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            environment={**self.lambdas_env_,},
            layers=[self.layers_["common_layer"]],
            timeout=core.Duration.seconds(55),
            dead_letter_queue=self.queues_["api_dead_letter_queue"],
            description="Implement /search endpoint",
        )

        for _, lambda_f in self.api_lambdas_.items():
            lambda_f.grant_invoke(iam.ServicePrincipal("apigateway.amazonaws.com"))

    def create_api_gateway(self) -> None:
        """
        Create API gateway, lambda integration and canary
        """

        # api_stage = core.CfnParameter(self, id="ApiStage", type=str)
        openapi_asset = s3_assets.Asset(
            self,
            "openapi_asset",
            path="cbers2stac/openapi/core-item-search-query-integrated.yaml",
        )
        data = core.Fn.transform(
            "AWS::Include", {"Location": openapi_asset.s3_object_url}
        )
        definition = apigateway.AssetApiDefinition.from_inline(data)
        apigw = apigateway.SpecRestApi(
            self,
            id="stacapi",
            api_definition=definition,
            deploy_options=apigateway.StageOptions(
                logging_level=apigateway.MethodLoggingLevel.INFO
            ),
        )
        # Canary to check search endpoint
        canary = synthetics.Canary(
            self,
            "SearchEndpointCanary",
            schedule=synthetics.Schedule.rate(core.Duration.hours(1)),
            runtime=synthetics.Runtime.SYNTHETICS_PYTHON_SELENIUM_1_0,
            test=synthetics.Test.custom(
                code=synthetics.Code.from_asset("cbers2stac/canary"),
                handler="api_canary.handler",
            ),
            environment_variables={"ENDPOINT_URL": apigw.url_for_path() + "/search"},
        )
        canary_alarm = cloudwatch.Alarm(
            self,
            "CanaryAlarm",
            metric=canary.metric_failed(period=core.Duration.hours(1)),
            evaluation_periods=1,
            threshold=0,
            comparison_operator=ComparisonOperator.GREATER_THAN_THRESHOLD,
        )
        canary_alarm.add_alarm_action(cw_actions.SnsAction(self.topics_["alarm_topic"]))

    def create_es_domain(self) -> None:
        """
        Create Elasticsearch domain and complete configuration for lambdas
        that uses it.
        """

        es_lambdas: List[aws_lambda.Function] = [
            self.lambdas_["create_elastic_index_lambda"],
            self.lambdas_["insert_into_elastic_lambda"],
            self.api_lambdas_["SearchEndpointLambda"],
        ]

        esd = elasticsearch.Domain(
            self,
            id="cbers2stac",
            # This is the version currently used by localstack
            version=elasticsearch.ElasticsearchVersion.V7_7,
            ebs=elasticsearch.EbsOptions(
                enabled=True, volume_size=settings.es_volume_size
            ),
            capacity=elasticsearch.CapacityConfig(
                data_node_instance_type=settings.es_instance_type,
                data_nodes=settings.es_data_nodes,
            ),
            access_policies=[
                iam.PolicyStatement(
                    actions=["es:*"],
                    principals=[lambda_f.grant_principal for lambda_f in es_lambdas],
                    # No need to specify resource, the domain is implicit
                )
            ],
        )

        # Add environment for lambdas
        for lambda_f in es_lambdas:
            lambda_f.add_environment("ES_ENDPOINT", esd.domain_endpoint)
            lambda_f.add_environment("ES_PORT", "443")
            lambda_f.add_environment("ES_SSL", "YES")

    def __init__(
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
        self.queues_: Dict[str, sqs.Queue] = {}

        # All stack topics
        self.topics_: Dict[str, sns.Topic] = {}

        # All lambda layers
        self.layers_: Dict[str, aws_lambda.LayerVersion] = {}

        # All lambdas and permissions (except API lambdas)
        self.lambdas_: Dict[str, aws_lambda.Function] = {}
        self.lambdas_perms_: List[iam.PolicyStatement] = []

        # All API lambdas and permissions
        self.api_lambdas_: Dict[str, aws_lambda.Function] = {}
        self.api_lambdas_perms_: List[iam.PolicyStatement] = []

        # Parameters that will not typically change and thus
        # are defined as fixed ENVs
        self.lambdas_env_.update(
            {
                # The CBERS metadata bucket (metadata only)
                "COG_PDS_META_PDS": settings.cog_pds_meta_pds,
                # If 1 then processed messages are deleted from queues
                "DELETE_MESSAGES": "1",
            }
        )

        # Create all topics
        self.create_all_topics()

        # Create all queues and attach subscriptions, alarms,etc.
        self.create_all_queues()

        # Update lambdas_env_ with environment options defined
        # in settings
        self.lambdas_env_.update(env)

        # Create STAC bucket if not configured
        if settings.stac_bucket_name is None:
            raise RuntimeError("STACK_STAC_BUCKET_NAME is mandatory")
            # Use external STAC bucket name
            # self.lambdas_env_.update({"STAC_BUCKET": settings.stac_bucket_name})

        # Create and use internal STAC bucket
        stac_working_bucket = s3.Bucket(
            self, "stac_working_bucket", bucket_name=settings.stac_bucket_name
        )
        self.lambdas_env_.update({"STAC_BUCKET": stac_working_bucket.bucket_name})
        self.lambdas_perms_.append(
            iam.PolicyStatement(
                actions=["s3:PutObject", "s3:PutObjectAcl"],
                resources=[
                    stac_working_bucket.bucket_arn,
                    f"{stac_working_bucket.bucket_arn}/*",
                ],
            )
        )
        # Check for public read access
        if settings.stac_bucket_public_read:
            stac_working_bucket.grant_public_access("*", "s3:Get*")
        # Check for CORS
        if settings.stac_bucket_enable_cors:
            stac_working_bucket.add_cors_rule(
                allowed_methods=[s3.HttpMethods.GET, s3.HttpMethods.HEAD],
                allowed_origins=["*"],
                allowed_headers=["*"],
                exposed_headers=[],
                max_age=3000,
            )
        # Deploy static structure
        create_local_catalog_structure(
            root_directory="stack/static_catalog_structure",
            bucket_name=settings.stac_bucket_name,
        )
        s3_deployment.BucketDeployment(
            self,
            "static_catalog",
            sources=[s3_deployment.Source.asset("stack/static_catalog_structure")],
            destination_bucket=stac_working_bucket,
            prune=settings.stac_bucket_prune,
        )

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

        # All layers
        self.create_all_layers()

        # Lambdas
        self.create_all_lambdas()

        aws_events.Rule(
            self,
            "GCLTBU",
            description="Generate catalog levels to be updated every 30 minutes",
            schedule=aws_events.Schedule.cron(minute="*/30"),
            targets=[
                aws_events_targets.LambdaFunction(
                    handler=self.lambdas_[
                        "generate_catalog_levels_to_be_updated_lambda"
                    ],
                    dead_letter_queue=self.queues_["dead_letter_queue"],
                    retry_attempts=0,
                )
            ],
        )

        # Common lambda (non API) permissions
        # Full access to all queues within stack
        self.lambdas_perms_.append(
            iam.PolicyStatement(
                actions=["sqs:*"],
                resources=[queue.queue_arn for _, queue in self.queues_.items()],
            )
        )
        # Full access to all buckets within stack
        if settings.stac_bucket_name:
            self.lambdas_perms_.append(
                iam.PolicyStatement(
                    actions=["s3:*"],
                    resources=[
                        stac_working_bucket.bucket_arn,
                        f"{stac_working_bucket.bucket_arn}/*",
                    ],
                )
            )

        # DynamoDB
        self.lambdas_perms_.append(
            iam.PolicyStatement(
                actions=["dynamodb:*"],
                resources=[
                    catalog_update_table.table_arn,
                    f"{catalog_update_table.table_arn}/*",
                ],
            )
        )
        # SNS topics
        self.lambdas_perms_.append(
            iam.PolicyStatement(
                actions=["sns:*"],
                resources=[topic.topic_arn for _, topic in self.topics_.items()],
            )
        )

        # Permissions for all (non API) lambdas
        for perm in self.lambdas_perms_:
            for _, lambda_f in self.lambdas_.items():
                lambda_f.add_to_role_policy(perm)

        # API
        if settings.enable_api:
            self.create_api_lambdas()
            self.create_api_gateway()
            self.create_es_domain()


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
