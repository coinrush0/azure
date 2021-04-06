using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;
using MessageQueueServer.ThridParty.Azure.Storage;
using System;
using Azure.Storage.Queues.Models;
using System.IO;
using System.Threading;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace MessageQueueServer.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MessageQueueController : ControllerBase
    {
        private static bool isTaskStarted;
        private IStorageQueueService StorageQueueService { get; set; }
        private readonly string ServiceQueueName = "testqueue";
        public MessageQueueController(IStorageQueueService storageQueueService)
        {
            StorageQueueService = storageQueueService;
        }

        private Task GetMessageQueueTask()
        {
            var CancellationTokenSource = new CancellationTokenSource();
            var MessageQueueTask = new Task(async () =>
            {
                var receiveMessages = await StorageQueueService.ReceiveMessagesAsync(ServiceQueueName);

                while (receiveMessages.Length > 0 && !CancellationTokenSource.Token.IsCancellationRequested)
                {
                    Parallel.ForEach(receiveMessages, async (message, status, index) => {
                        using var outputFile = new StreamWriter(@"./New_TEXT_File.txt", true);

                        await outputFile.WriteLineAsync(message.MessageText);
                        await StorageQueueService.DequeueMessageAsync(ServiceQueueName, message);
                    });

                    receiveMessages = await StorageQueueService.ReceiveMessagesAsync(ServiceQueueName);
                }

                isTaskStarted = false;
            }, CancellationTokenSource.Token);

            return MessageQueueTask;
        }

        [HttpGet]
        public async Task<string> StartQueueMessageProcess()
        {
            await StorageQueueService.InsertMessageAsync(ServiceQueueName, "test");

            if (!isTaskStarted)
            {
                isTaskStarted = true;

                GetMessageQueueTask().Start();
            }

            return "test";
        }
    }
}
