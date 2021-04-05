import os, uuid
from azure.storage.queue import (
        QueueClient,
        BinaryBase64EncodePolicy,
        BinaryBase64DecodePolicy
)

connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Create a unique name for the queue
q_name = "queue-" + str(uuid.uuid4())

# Instantiate a QueueClient object which will
# be used to create and manipulate the queue
print("Creating queue: " + q_name)

for i in range(2):
                
        queue_client = QueueClient.from_connection_string(connect_str, q_name + str(i))
        # base64_queue_client = QueueClient.from_connection_string(
        #                             conn_str=connect_str, queue_name=q_name,
        #                             message_encode_policy = BinaryBase64EncodePolicy(),
        #                             message_decode_policy = BinaryBase64DecodePolicy()
        #                         )

        message_str = "this is message" + str(i)
        # Create the queue
        queue_client.create_queue()
        queue_client.send_message(message_str)

# Peek at the first message
messages = queue_client.receive_messages(visibility_timeout=5*60)
message = messages.by_page()

for msg_batch in message:
        for msg in msg_batch:
                print("Dequeueing message: " + msg.content)
                queue_client.delete_message(msg.id, msg.pop_receipt)
                print("Deleted message: " + msg.content)