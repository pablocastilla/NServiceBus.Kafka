using NServiceBus.Logging;
using NServiceBus.Transports.Kafka.Connection;
using RdKafka;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Transports.Kafka.Wrapper;
using NServiceBus.Extensibility;


namespace NServiceBus.Transport.Kafka.Receiving
{

    class MessagePump : IPushMessages
    {
        Func<MessageContext, Task> onMessage;
        Func<ErrorContext, Task<ErrorHandleResult>> onError;
        ConsumerFactory consumerFactory;
        EventConsumer consumer;
        static TimeSpan StoppingAllTasksTimeout = TimeSpan.FromSeconds(5);

        static ILog Logger = LogManager.GetLogger(typeof(MessagePump));
        ConcurrentDictionary<Task, Task> runningReceiveTasks = new ConcurrentDictionary<Task, Task>();
        static readonly TransportTransaction transportTranaction = new TransportTransaction();
        private PushSettings settings;
        Dictionary<string, TopicPartitionOffset> assigments = new Dictionary<string, TopicPartitionOffset>();


        // Start
        int maxConcurrency;
        SemaphoreSlim semaphore;
        CancellationTokenSource messageProcessing;
        bool started = false;
        
        // Stop
        TaskCompletionSource<bool> connectionShutdownCompleted;

        public MessagePump(ConsumerFactory consumerFactory)
        {
            this.consumerFactory = consumerFactory;            
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

            consumer = consumerFactory.CreateConsumer();

            consumer.OnError += Consumer_OnError;
            consumer.OnMessage += Consumer_OnMessage;
            consumer.OnPartitionsAssigned += Consumer_OnPartitionsAssigned;
            consumer.OnPartitionsRevoked += Consumer_OnPartitionsRevoked;
            consumer.OnEndReached += Consumer_OnEndReached;

            consumer.AddSubscriptionsBlocking(new List<string> { settings.InputQueue });
            consumer.CommitSubscriptionsBlocking();

            consumer.Start();
        }

        private void Consumer_OnError(object sender, Handle.ErrorArgs e)
        {
            Logger.Error("Consumer_OnError: " + e.Reason);
            /*((EventConsumer)sender).Stop().Wait(30000);
            consumerFactory.ResetConsumer();
            consumer = consumerFactory.CreateConsumer();
            consumer.OnError += Consumer_OnError;
            consumer.OnMessage += Consumer_OnMessage;
            consumerFactory.StartConsumer();*/
        }

        private void Consumer_OnPartitionsAssigned(object sender, List<TopicPartitionOffset> e)
        {
            Logger.Debug($"Assigned partitions: [{string.Join(", ", e)}], member id: {((EventConsumer)sender).MemberId}");

            if (e.Count == 0)
                return;

            //TODO: circuit breaker ok

            foreach (var partition in e)
            {
                var partitionName = partition.Topic + partition.Partition;

                if (!assigments.ContainsKey(partitionName))
                    assigments.Add(partitionName, partition);
                else
                    assigments[partitionName] = partition;

            }

            var partititionsToAssign = assigments.Values.Select(p => p).ToList();
            ((EventConsumer)sender).Assign(partititionsToAssign);
        }


        private void Consumer_OnPartitionsRevoked(object sender, List<TopicPartitionOffset> partitions)
        {
            Logger.Debug($"Revoked partitions: [{string.Join(", ", partitions)}]");
            foreach (var p in partitions)
                assigments.Remove(p.Topic + p.Partition);

            ((EventConsumer)sender).Unassign();
        }

        private void Consumer_OnEndReached(object sender, TopicPartitionOffset e)
        {
            Logger.Debug("EndReached:" + e);
        }

        ConcurrentDictionary<TopicPartitionOffset, bool> OffsetsReceived = new ConcurrentDictionary<TopicPartitionOffset, bool>();
    
        private void Consumer_OnMessage(object sender, Message e)
        {
            try
            {
                Logger.Debug($"message consumed"); 
                var receiveTask = InnerReceive(e);                

                runningReceiveTasks.TryAdd(receiveTask, receiveTask);

                receiveTask.ContinueWith((t, state) =>
                {                   
                    var receiveTasks = (ConcurrentDictionary<Task, Task>)state;

                    OffsetsReceived.TryUpdate(e.TopicPartitionOffset, true, false);

                    List<TopicPartitionOffset> offSetsToCommit = new List<TopicPartitionOffset>();

                    var partitions = from offset in OffsetsReceived.Keys
                                     group offset by new
                                     {
                                         offset.Topic,
                                         offset.Partition
                                     } into keys
                                     select new 
                                     {
                                         offsetNames = keys.Key.Topic + keys.Key.Partition,
                                         offsetValues = keys.ToList()
                                     };

                    foreach (var k in partitions)
                    {
                        var orderedOffsets = k.offsetValues.OrderBy(p => p.Offset);
                        foreach (var o in orderedOffsets)
                        {
                            if (OffsetsReceived[o] != true)
                            {
                                break;
                            }
                            else
                            {
                                offSetsToCommit.Add(o);
                                bool aux;
                                OffsetsReceived.TryRemove(o,out aux);
                            }
                        }
                    }

                    if(offSetsToCommit.Count>0)
                        consumer.Commit(offSetsToCommit).ConfigureAwait(false);

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
                var message = await retrieved.UnWrap().ConfigureAwait(false);

                OffsetsReceived.AddOrUpdate(retrieved.TopicPartitionOffset, false, (a,b)=> false);

                await Process(message).ConfigureAwait(false);

            }
            
            catch (Exception ex)
            {
                Logger.Error("Kafka transport failed pushing a message through pipeline", ex);
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
                        Logger.Error($"error onMessage: "+ex.ToString());
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
            
            Logger.Debug($"consumer.OnMessage -= Consumer_OnMessage");

            consumer.OnError -= Consumer_OnError;
            consumer.OnMessage -= Consumer_OnMessage;
            consumer.OnPartitionsAssigned -= Consumer_OnPartitionsAssigned;
            consumer.OnPartitionsRevoked -= Consumer_OnPartitionsRevoked;
            consumer.OnEndReached -= Consumer_OnEndReached;
            messageProcessing.Cancel();

            await consumer.Stop().ConfigureAwait(false);

            // ReSharper disable once MethodSupportsCancellation
            var timeoutTask = Task.Delay(StoppingAllTasksTimeout);
            var allTasks = runningReceiveTasks.Values;
            var finishedTask = await Task.WhenAny(Task.WhenAll(allTasks), timeoutTask).ConfigureAwait(false);

            if (finishedTask.Equals(timeoutTask))
            {
                Logger.Error("The message pump failed to stop with in the time allowed(30s)");
            }

            
            runningReceiveTasks.Clear();

           
        }
    }
}
