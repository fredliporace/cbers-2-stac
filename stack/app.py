"""App construction"""

# import json
# import os
# from test.conftest import create_lambda_layer_from_dir
from typing import Any, Dict, Optional

# from aws_cdk import aws_dynamodb as dynamodb
# from aws_cdk import aws_events, aws_events_targets
# from aws_cdk import aws_iam as iam
# from aws_cdk import aws_lambda
# from aws_cdk import aws_s3 as s3
# from aws_cdk import aws_s3_notifications as s3n
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cw_actions
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as sns_subscriptions
from aws_cdk import aws_sqs as sqs
from aws_cdk import core
from aws_cdk.aws_cloudwatch import ComparisonOperator

# from cbers04aonaws.layers.common.dbtable import DBTable
from stack.config import StackSettings

# import docker


# from aws_cdk.aws_lambda_event_sources import SqsEventSource


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
        return queue

    def __init__(  # pylint: disable=too-many-locals
        self,
        scope: core.Construct,
        stack_id: str,
        description: str,
        env: Dict[str, str],
        **kwargs: Any,
    ) -> None:
        """Ctor."""
        super().__init__(scope, stack_id, description=description, *kwargs)

        self.lambdas_env_.update(env)

        # General alarm topic to signal problems in stack execution
        # and e-mail subscription
        alarm_topic = sns.Topic(self, "AlarmTopic")
        alarm_topic.add_subscription(
            sns_subscriptions.EmailSubscription(settings.operator_email)
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
            "process_new_scenes_queue",
            retention_period=core.Duration.seconds(1209600),
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
