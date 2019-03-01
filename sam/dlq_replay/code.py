"""dlq_replay

Thanks to https://github.com/coveo/sqs-deadletterqueue-replayer-lambda
"""

import boto3

def transfer_messages(source_queue,  # pylint: disable=missing-docstring
                      target_queue):
    total_messages_transferred = 0
    while True:
        messages = gather_messages(source_queue)
        if not messages:
            break
        total_messages_transferred += len(messages)
        send_messages(messages, target_queue)
        delete_messages(messages)
    print("In total " + str(total_messages_transferred) + " were transferred.")


def gather_messages(queue): # pylint: disable=missing-docstring
    messages = queue.receive_messages(MaxNumberOfMessages=10,
                                      WaitTimeSeconds=20)
    print("Collected: " + str(len(messages)) + " messages.")
    return messages

def send_messages(messages, queue): # pylint: disable=missing-docstring
    entries = [dict(Id=str(i + 1),
                    MessageBody=message.body) for i, message in \
               enumerate(messages)]

    queue.send_messages(Entries=entries)

def delete_messages(messages): # pylint: disable=missing-docstring
    for message in messages:
        print("Copied " + str(message.body))
        message.delete()


def handler(event, context): # pylint: disable=missing-docstring,unused-argument
    sqs = boto3.resource(service_name='sqs')

    source_queue_name = event['source_queue']
    target_queue_name = event['target_queue']

    print("From: " + source_queue_name + " To: " + target_queue_name)

    source_queue = sqs.get_queue_by_name(QueueName=source_queue_name)
    target_queue = sqs.get_queue_by_name(QueueName=target_queue_name)

    transfer_messages(source_queue, target_queue)
