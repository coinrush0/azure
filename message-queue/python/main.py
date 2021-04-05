import os, uuid
from azure.storage.queue import (
    QueueClient,
    BinaryBase64EncodePolicy,
    BinaryBase64DecodePolicy
)

#connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
# This connection string is Azurite default connection string
connect_str = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;"
# Create a unique name for the queue
q_name = "queue-" + str(uuid.uuid4())

# Instantiate a QueueClient object which will
# be used to create and manipulate the queue
print("Creating queue: " + q_name)

queue_client = QueueClient.from_connection_string(connect_str, q_name)
queue_client.create_queue()

for i in range(2):
    # base64_queue_client = QueueClient.from_connection_string(
    #                             conn_str=connect_str, queue_name=q_name,
    #                             message_encode_policy = BinaryBase64EncodePolicy(),
    #                             message_decode_policy = BinaryBase64DecodePolicy()
    #                         )

    message_str = "this is message"
    # Create the queue
    queue_client.send_message(message_str + str(i))

messages = queue_client.receive_messages(visibility_timeout=5*60)

for message in messages:
    print("Dequeueing message: " + message.content)
    queue_client.delete_message(message.id, message.pop_receipt)
    print("Deleted message: " + message.content)
            
print("Deleting queue: " + queue_client.queue_name)
queue_client.delete_queue()