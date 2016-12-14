using NServiceBus.Logging;
using NServiceBus.Transport;
using NServiceBus.Transports.Kafka.Connection;
using RdKafka;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Transports.Kafka.Wrapper;
using NServiceBus;
using NServiceBus.Extensibility;
using Janitor;

namespace NServiceBus.Transport.Kafka.Receiving
{
    [SkipWeaving]
    class MessagePump : IPushMessages, IDisposable
    {
        Func<MessageContext, Task> onMessage;
        Func<ErrorContext, Task<ErrorHandleResult>> onError;
        ConsumerFactory consumerFactory;
        EventConsumer consumer;
        string endpointName;
        static TimeSpan StoppingAllTasksTimeout = TimeSpan.FromSeconds(5);

        static ILog Logger = LogManager.GetLogger(typeof(MessagePump));
        ConcurrentDictionary<Task, Task> runningReceiveTasks;
        static readonly TransportTransaction transportTranaction = new TransportTransaction();
        private PushSettings settings;


        // Start
        int maxConcurrency;
        SemaphoreSlim semaphore;
        CancellationTokenSource messageProcessing;
        
        // Stop
        TaskCompletionSource<bool> connectionShutdownCompleted;

        public MessagePump(ConsumerFactory consumerFactory,  string endpointName)
        {
            this.consumerFactory = consumerFactory;            
            this.endpointName = endpointName;
        }

        public Task Init(Func<MessageContext, Task> onMessage, Func<ErrorContext, Task<ErrorHandleResult>> onError, CriticalError criticalError, PushSettings settings)
        {
            this.onMessage = onMessage;
            this.onError = onError;
            this.settings = settings;

            //TODO: circuit breaker?
            //circuitBreaker = new MessagePumpConnectionFailedCircuitBreaker($"'{settings.InputQueue} MessagePump'", timeToWaitBeforeTriggeringCircuitBreaker, criticalError);


            return Task.FromResult(0);
        }

        public void Start(PushRuntimeSettings limitations)
        {
            runningReceiveTasks = new ConcurrentDictionary<Task, Task>();
            messageProcessing = new CancellationTokenSource();

            maxConcurrency = limitations.MaxConcurrency;
            semaphore = new SemaphoreSlim(limitations.MaxConcurrency, limitations.MaxConcurrency);

            consumer = consumerFactory.GetConsumer();

            consumer.OnError += Consumer_OnError;
            consumer.OnMessage += Consumer_OnMessage;

            consumer.AddSubscriptionsBlocking(new List<string>() { endpointName } );
            consumer.CommitSubscriptionsBlocking();
            consumer.Start();
        }

        private void Consumer_OnError(object sender, Handle.ErrorArgs e)
        {
            Logger.Error("Consumer_OnError: " + e.Reason);
            consumer.Stop().Wait(30000);
            consumerFactory.ResetConsumer();
            consumer = consumerFactory.GetConsumer();
            consumer.OnError += Consumer_OnError;
            consumer.OnMessage += Consumer_OnMessage;
            consumer.Start();
        }

        private void Consumer_OnMessage(object sender, Message e)
        {
            try
            {
                Logger.Info($"Consumer_OnMessage");

                var receiveTask = InnerReceive(e);

                

                runningReceiveTasks.TryAdd(receiveTask, receiveTask);

                receiveTask.ContinueWith((t, state) =>
                {                   
                    var receiveTasks = (ConcurrentDictionary<Task, Task>)state;
                    
                    Task toBeRemoved;
                    receiveTasks.TryRemove(t, out toBeRemoved);
                }, runningReceiveTasks, TaskContinuationOptions.ExecuteSynchronously);
            }
            catch (OperationCanceledException)
            {
                // For graceful shutdown purposes
                return;
            }
        }


        async Task InnerReceive(Message retrieved)
        {           
            try
            {
                var message = await retrieved.UnWrap();

                await Process(message);

                await consumer.Commit(retrieved);
            }
            
            catch (Exception ex)
            {
                Logger.Warn("Kafka transport failed pushing a message through pipeline", ex);
            }
            finally
            {
                //concurrencyLimiter.Release();
            }
        }


        async Task Process(MessageWrapper message)
        {
            Dictionary<string, string> headers;

            try
            {
                headers = message.Headers;
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to retrieve headers from poison message. Moving message to queue '{settings.ErrorQueue}'...", ex);
                await MovePoisonMessage(message, settings.ErrorQueue).ConfigureAwait(false);

                return;
            }

            string messageId;

            try
            {
                messageId = message.Id;
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to retrieve ID from poison message. Moving message to queue '{settings.ErrorQueue}'...", ex);
                await MovePoisonMessage(message, settings.ErrorQueue).ConfigureAwait(false);

                return;
            }

            using (var tokenSource = new CancellationTokenSource())
            {
                var processed = false;
                var errorHandled = false;
                var numberOfDeliveryAttempts = 0;

                while (!processed && !errorHandled)
                {
                    try
                    {
                        var messageContext = new MessageContext(messageId, headers, message.Body ?? new byte[0], transportTranaction, tokenSource, new ContextBag());
                        await onMessage(messageContext).ConfigureAwait(false);
                        processed = true;
                    }
                    catch (Exception ex)
                    {
                        ++numberOfDeliveryAttempts;
                        var errorContext = new ErrorContext(ex, headers, messageId, message.Body ?? new byte[0], transportTranaction, numberOfDeliveryAttempts);
                        errorHandled = await onError(errorContext).ConfigureAwait(false) == ErrorHandleResult.Handled;
                    }
                }

                if (processed && tokenSource.IsCancellationRequested)
                {
                    
                }
                else
                {
                  
                }
            }
        }

        async Task MovePoisonMessage(MessageWrapper message, string queue)
        {
            throw new Exception();
        }

        public async Task Stop()
        {
            /* consumer.OnMessage -= Consumer_OnMessage;
             messageProcessing.Cancel();

             while (semaphore.CurrentCount != maxConcurrency)
             {
                 await Task.Delay(50).ConfigureAwait(false);
             }

             connectionShutdownCompleted = new TaskCompletionSource<bool>();

             consumer.Unsubscribe();

             connectionShutdownCompleted.SetResult(true);

             await connectionShutdownCompleted.Task.ConfigureAwait(false);*/

            // cancellationTokenSource.Cancel();

            Logger.Info($"consumer.OnMessage -= Consumer_OnMessage");

            consumer.OnError -= Consumer_OnError;
            consumer.OnMessage -= Consumer_OnMessage;
            messageProcessing.Cancel();
            // ReSharper disable once MethodSupportsCancellation
            var timeoutTask = Task.Delay(StoppingAllTasksTimeout);
            var allTasks = runningReceiveTasks.Values;
            var finishedTask = await Task.WhenAny(Task.WhenAll(allTasks), timeoutTask).ConfigureAwait(false);

            if (finishedTask.Equals(timeoutTask))
            {
                Logger.Error("The message pump failed to stop with in the time allowed(30s)");
            }

            
            runningReceiveTasks.Clear();

            consumerFactory.Dispose();
        }


        public void Dispose()
        {
            consumerFactory.Dispose();
        }
    }
}
