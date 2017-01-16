
using NServiceBus.Logging;
using RdKafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Settings;
using NServiceBus.Transports.Kafka.Administration;
using NServiceBus.Transport;
using System.Collections.Concurrent;
using NServiceBus.Transports.Kafka.Wrapper;
using System.Threading;
using NServiceBus.Extensibility;

namespace NServiceBus.Transports.Kafka.Connection
{

    class ConsumerHolder:IDisposable
    {
        static ILog Logger = LogManager.GetLogger(typeof(ConsumerHolder));

        string connectionString;
        string endpointName;

        EventConsumer consumer;
        static Object o = new Object();

        public bool ConsumerStarted { get; set; }

        private SettingsHolder settingsHolder;
        PushSettings settings;
        private Func<MessageContext, Task> onMessage;
        private Func<ErrorContext, Task<ErrorHandleResult>> onError;
        readonly TransportTransaction transportTransaction = new TransportTransaction();


        public ConsumerHolder(string connectionString, string endpointName, PushSettings settings, SettingsHolder settingsHolder, Func<MessageContext, Task> onMessage, Func<ErrorContext, Task<ErrorHandleResult>> onError) 
        {
            this.onMessage = onMessage;
            this.onError = onError;

            this.settings = settings;
            this.settingsHolder = settingsHolder;
            this.connectionString = connectionString;
            this.endpointName = endpointName;

            if (consumer == null)
            {
                lock (o)
                {
                    if (consumer == null)
                    {
                        CreateConsumer();

                    }
                }
            }
        }

        public void Init()
        {           

            consumer.OnError += Consumer_OnError;
            consumer.OnMessage += Consumer_OnMessage;

            consumer.AddSubscriptionsBlocking(new List<string>() { endpointName });
            consumer.CommitSubscriptionsBlocking();


            StartConsumer();

        }

        public void Stop()
        {
            consumer.OnError -= Consumer_OnError;
            consumer.OnMessage -= Consumer_OnMessage;

        }

        public EventConsumer GetConsumer()
        {                  
           return consumer;
        }

        public void ResetConsumer()
        {
            lock (o)
            {
                ConsumerStarted = false;
                CreateConsumer(consumer.Subscription);
            }

        }

        public void StartConsumer()
        {
            if (!ConsumerStarted)
            {
                lock (o)
                {
                    if (!ConsumerStarted)
                    {
                        ConsumerStarted = true;
                        consumer.Start();
                    }
                }
            }
        }


        private void CreateConsumer(List<string> topics = null)
        {
            var config = new RdKafka.Config() { GroupId = endpointName, EnableAutoCommit = false };

            bool debugEnabled;
            if(settingsHolder.TryGet<bool>(WellKnownConfigurationKeys.KafkaDebugEnabled,out debugEnabled) && debugEnabled)
                config["debug"] = "all";

            var defaultConfig = new TopicConfig();
            defaultConfig["auto.offset.reset"] = "earliest";

            string sessionTimeout;
            if (settingsHolder.TryGet<string>(WellKnownConfigurationKeys.KafkaSessionTimeout, out sessionTimeout) )
                config["session.timeout.ms"] = sessionTimeout;
            else
                config["session.timeout.ms"] = "30000";


            string heartBeatInterval;
            if (settingsHolder.TryGet<string>(WellKnownConfigurationKeys.KafkaHeartBeatInterval, out heartBeatInterval))                
                config["heartbeat.interval.ms"] = heartBeatInterval;
            else
                config["heartbeat.interval.ms"] = "1000";

            config.DefaultTopicConfig = defaultConfig;

            if(consumer!=null)
            {
               // consumer.Dispose();
            }
            
            consumer = new EventConsumer(config, connectionString);

            if (topics != null && consumer != null)
            {               
                consumer.AddSubscriptionsBlocking(topics);
            }

            consumer.OnPartitionsAssigned += Consumer_OnPartitionsAssigned;

            consumer.OnPartitionsRevoked += Consumer_OnPartitionsRevoked;
           
            consumer.OnEndReached += Consumer_OnEndReached;

                                  
        }


        private void Consumer_OnError(object sender, Handle.ErrorArgs e)
        {
            Logger.Error("Consumer_OnError: " + e.Reason);
            /*((EventConsumer)sender).Stop().Wait(30000);
            consumerFactory.ResetConsumer();
            consumer = consumerFactory.GetConsumer();
            consumer.OnError += Consumer_OnError;
            consumer.OnMessage += Consumer_OnMessage;
            consumerFactory.StartConsumer();*/
        }


        ConcurrentDictionary<TopicPartitionOffset, bool> OffsetsReceived = new ConcurrentDictionary<TopicPartitionOffset, bool>();
        ConcurrentDictionary<Task, Task> runningReceiveTasks = new ConcurrentDictionary<Task, Task>();

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
                                OffsetsReceived.TryRemove(o, out aux);
                            }
                        }
                    }

                    if (offSetsToCommit.Count > 0)
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

                OffsetsReceived.AddOrUpdate(retrieved.TopicPartitionOffset, false, (a, b) => false);

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
                        var messageContext = new MessageContext(messageId, headers, message.Body ?? new byte[0], transportTransaction, tokenSource, new ContextBag());
                        await onMessage(messageContext).ConfigureAwait(false);
                        processed = true;
                    }
                    catch (Exception ex)
                    {
                        Logger.Error($"error onMessage: " + ex.ToString());
                        ++numberOfDeliveryAttempts;
                        var errorContext = new ErrorContext(ex, headers, messageId, message.Body ?? new byte[0], transportTransaction, numberOfDeliveryAttempts);
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

        Dictionary<string, TopicPartitionOffset> assigments = new Dictionary<string, TopicPartitionOffset>();

        private void Consumer_OnPartitionsAssigned(object sender, List<TopicPartitionOffset> e)
        {
            Logger.Debug($"Assigned partitions: [{string.Join(", ", e)}], member id: {((EventConsumer)sender).MemberId}");

            if (e.Count == 0 || disposing)
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
            if (disposing || disposedValue)
                return;

            Logger.Debug($"Revoked partitions: [{string.Join(", ", partitions)}]");
            foreach (var p in partitions)
                assigments.Remove(p.Topic + p.Partition);

            var partititionsToAssign = assigments.Values.Select(p => p).ToList();

            //((EventConsumer)sender).Assign(partititionsToAssign);
            ((EventConsumer)sender).Unassign();
        }

        private void Consumer_OnEndReached(object sender, TopicPartitionOffset e)
        {
            Logger.Debug("EndReached:" + e);
        }

        async Task MovePoisonMessage(MessageWrapper message, string queue)
        {
            throw new Exception();
        }


        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls
        private bool disposing = false;


        protected virtual void Dispose(bool disposing)
        {
            this.disposing = disposing;

            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                    if (consumer != null)
                    {
                        Logger.Debug("Disposing " + consumer.Name);

                        consumer.OnPartitionsAssigned -= Consumer_OnPartitionsAssigned;
                        consumer.OnPartitionsRevoked -= Consumer_OnPartitionsRevoked;
                        consumer.OnEndReached -= Consumer_OnEndReached;
                        consumer.Stop();
                        consumer.Dispose();
                    }

                    consumer = null;

                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~ConsumerFactory() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}
