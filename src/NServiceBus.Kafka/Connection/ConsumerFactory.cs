
using NServiceBus.Logging;
using RdKafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Settings;
using NServiceBus.Transports.Kafka.Administration;

namespace NServiceBus.Transports.Kafka.Connection
{

    class ConsumerFactory:IDisposable
    {
        static ILog Logger = LogManager.GetLogger(typeof(ConsumerFactory));

        string connectionString;
        string endpointName;

        EventConsumer consumer;
        static Object o = new Object();

        public bool ConsumerStarted { get; set; }
                      

        public ConsumerFactory(string connectionString, string endpointName, SettingsHolder settings) 
        {
            this.settings = settings;
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
            if(settings.TryGet<bool>(WellKnownConfigurationKeys.KafkaDebugEnabled,out debugEnabled) && debugEnabled)
                config["debug"] = "all";

            var defaultConfig = new TopicConfig();
            defaultConfig["auto.offset.reset"] = "earliest";

            string sessionTimeout;
            if (settings.TryGet<string>(WellKnownConfigurationKeys.KafkaSessionTimeout, out sessionTimeout) )
                config["session.timeout.ms"] = sessionTimeout;
            else
                config["session.timeout.ms"] = "30000";


            string heartBeatInterval;
            if (settings.TryGet<string>(WellKnownConfigurationKeys.KafkaHeartBeatInterval, out heartBeatInterval))                
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

       
        Dictionary<string, TopicPartitionOffset> assigments = new Dictionary<string, TopicPartitionOffset>();

        private void Consumer_OnPartitionsAssigned(object sender, List<TopicPartitionOffset> e)
        {
            Logger.Info($"Assigned partitions: [{string.Join(", ", e)}], member id: {((EventConsumer)sender).MemberId}");

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

            Logger.Info($"Revoked partitions: [{string.Join(", ", partitions)}]");
            foreach (var p in partitions)
                assigments.Remove(p.Topic + p.Partition);

            var partititionsToAssign = assigments.Values.Select(p => p).ToList();

            //((EventConsumer)sender).Assign(partititionsToAssign);
            ((EventConsumer)sender).Unassign();
        }

        private void Consumer_OnEndReached(object sender, TopicPartitionOffset e)
        {
            Logger.Info("EndReached:" + e);
        }




        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls
        private bool disposing = false;
        private SettingsHolder settings;

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
                        Logger.Info("Disposing " + consumer.Name);

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
