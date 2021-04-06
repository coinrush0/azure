using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using System;
using System.Threading.Tasks;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace MessageQueueServer.ThridParty.Azure.Storage
{
    public interface IStorageQueueService
    {
        public bool CreateQueue(string queueName);
        public Task InsertMessageAsync(string queueName, string message);
        public Task<PeekedMessage[]> PeekMessagesAsync(string queueName);
        public Task<QueueMessage[]> ReceiveMessagesAsync(string queueName);
        public void UpdateMessage(string queueName, string contents, double timeLimit);
        public void UpdateMessage(string queueName);
        public Task DequeueMessageAsync(string queueName, QueueMessage message);
    }

    public class StorageQueueService : IStorageQueueService
    {
        private string ConnectionString { get; init; }
        public StorageQueueService(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public bool CreateQueue(string queueName)
        {
            try
            {
                // Instantiate a QueueClient which will be used to create and manipulate the queue
                QueueClient queueClient = new(ConnectionString, queueName);

                // Create the queue
                queueClient.CreateIfNotExists();

                if (queueClient.Exists())
                {
                    Console.WriteLine($"Queue created: '{queueClient.Name}'");
                    return true;
                }
                else
                {
                    Console.WriteLine($"Make sure the Azurite storage emulator running and try again.");
                    return false;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception: {ex.Message}\n\n");
                Console.WriteLine($"Make sure the Azurite storage emulator running and try again.");
                return false;
            }
        }

        public async Task InsertMessageAsync(string queueName, string message)
        {
            // Instantiate a QueueClient which will be used to create and manipulate the queue
            QueueClient queueClient = new(ConnectionString, queueName);

            // Create the queue if it doesn't already exist
            await queueClient.CreateIfNotExistsAsync();

            if (queueClient.Exists())
            {
                // Send a message to the queue
                await queueClient.SendMessageAsync(message);
            }

            Console.WriteLine($"Inserted: {message}");
        }

        public async Task<PeekedMessage[]> PeekMessagesAsync(string queueName)
        {
            // Instantiate a QueueClient which will be used to manipulate the queue
            QueueClient queueClient = new(ConnectionString, queueName);

            if (await queueClient.ExistsAsync())
            {
                // Peek at the next message
                PeekedMessage[] peekedMessage = await queueClient.PeekMessagesAsync();
                return peekedMessage;
            }

            return Array.Empty<PeekedMessage>();
        }

        public void UpdateMessage(string queueName, string contents, double timeLimit)
        {
            QueueClient queueClient = new(ConnectionString, queueName);

            if (queueClient.Exists())
            {
                // Get the message from the queue
                QueueMessage[] message = queueClient.ReceiveMessages();

                // Update the message contents
                queueClient.UpdateMessage(message[0].MessageId,
                        message[0].PopReceipt,
                        contents,
                        TimeSpan.FromSeconds(timeLimit)  // Make it invisible for another 60 seconds
                    );
            }
        }

        public void UpdateMessage(string queueName)
        {
            // Instantiate a QueueClient which will be used to manipulate the queue
            UpdateMessage(queueName, "Updated contents", 60);
        }

        public async Task<QueueMessage[]> ReceiveMessagesAsync(string queueName)
        {
            QueueClient queueClient = new(ConnectionString, queueName);

            if (queueClient.Exists())
            {
                QueueMessage[] retrievedMessage = await queueClient.ReceiveMessagesAsync(5, TimeSpan.FromSeconds(60.0));
                return retrievedMessage;
            }

            return Array.Empty<QueueMessage>();
        }

        public async Task DequeueMessageAsync(string queueName, QueueMessage message)
        {
            QueueClient queueClient = new(ConnectionString, queueName);

            if (queueClient.Exists())
            {
                // Delete the message
                var result = await queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt);
            }
        }
    }
}
