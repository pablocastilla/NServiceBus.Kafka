using NServiceBus.Transport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Settings;
using NServiceBus.Transports.Kafka.Administration;
using NServiceBus.Logging;

namespace NServiceBus.Transport.Kafka.Receiving
{
    class QueueCreator : ICreateQueues
    {
        private SettingsHolder settings;
        static ILog Logger = LogManager.GetLogger(typeof(MessagePump));

        public QueueCreator(SettingsHolder settings)
        {
            this.settings = settings;
        }

        public async Task CreateQueueIfNecessary(QueueBindings queueBindings, string identity)
        {
            try
            {
                bool createQueues;
                if (!settings.TryGet<bool>(WellKnownConfigurationKeys.CreateQueues, out createQueues))
                    return;

                string pathToBin;
                if (!settings.TryGet<string>(WellKnownConfigurationKeys.KafkaPathToBin, out pathToBin))
                    pathToBin = @"C:\kafka_2.11-0.10.1.0\bin\windows";

                string zookeeperUrl;
                if (!settings.TryGet<string>(WellKnownConfigurationKeys.KafkaZooKeeperUrl, out zookeeperUrl))
                    zookeeperUrl = @"localhost:2181";

                string numberOfPartitions;
                if (!settings.TryGet<string>(WellKnownConfigurationKeys.NumberOfPartitions, out numberOfPartitions))
                    numberOfPartitions = "1";

                string replicationFactor;
                if (!settings.TryGet<string>(WellKnownConfigurationKeys.ReplicationFactor, out replicationFactor))
                    replicationFactor = "1";

                var process = new System.Diagnostics.Process();

                var startInfo = new System.Diagnostics.ProcessStartInfo
                {
                    WorkingDirectory = pathToBin,
                    WindowStyle = System.Diagnostics.ProcessWindowStyle.Normal,
                    FileName = "cmd.exe",
                    RedirectStandardInput = true,
                    UseShellExecute = false
                };

                process.StartInfo = startInfo;
                process.Start();


                //delete
              /*  string kafkaDeleteTopicCommand = @".\kafka-topics.bat --delete --zookeeper " + zookeeperUrl + "  --topic ";

                foreach (var q in queueBindings.ReceivingAddresses)
                {
                    await process.StandardInput.WriteLineAsync(kafkaDeleteTopicCommand + q).ConfigureAwait(false);
                    
                }

                foreach (var q in queueBindings.SendingAddresses)
                {
                    await process.StandardInput.WriteLineAsync(kafkaDeleteTopicCommand + q).ConfigureAwait(false);
                    
                }*/


                //create
                string kafkaCreateTopicCommand = @".\kafka-topics.bat --create --zookeeper " + zookeeperUrl + " --partitions " + numberOfPartitions + " --replication-factor " + replicationFactor + " --topic ";
                
                foreach (var q in queueBindings.ReceivingAddresses)
                {
                    
                    await process.StandardInput.WriteLineAsync(kafkaCreateTopicCommand + q).ConfigureAwait(false);
                }

                foreach (var q in queueBindings.SendingAddresses)
                {
                    
                    await process.StandardInput.WriteLineAsync(kafkaCreateTopicCommand + q).ConfigureAwait(false);
                }

                
            }
            catch (Exception ex)
            {
                Logger.Warn(ex.ToString());
            }

           

        }

        public async Task CreateQueues(List<string> queues)
        {
            try
            {
                bool createQueues;
                if (!settings.TryGet<bool>(WellKnownConfigurationKeys.CreateQueues, out createQueues))
                    return;

                string pathToBin;
                if (!settings.TryGet<string>(WellKnownConfigurationKeys.KafkaPathToBin, out pathToBin))
                    pathToBin = @"C:\kafka_2.11-0.10.1.0\bin\windows";

                string zookeeperUrl;
                if (!settings.TryGet<string>(WellKnownConfigurationKeys.KafkaZooKeeperUrl, out zookeeperUrl))
                    zookeeperUrl = @"localhost:2181";

                string numberOfPartitions;
                if (!settings.TryGet<string>(WellKnownConfigurationKeys.NumberOfPartitions, out numberOfPartitions))
                    numberOfPartitions = "1";

                string replicationFactor;
                if (!settings.TryGet<string>(WellKnownConfigurationKeys.ReplicationFactor, out replicationFactor))
                    replicationFactor = "1";

                var process = new System.Diagnostics.Process();

                var startInfo = new System.Diagnostics.ProcessStartInfo
                {
                    WorkingDirectory = pathToBin,
                    WindowStyle = System.Diagnostics.ProcessWindowStyle.Normal,
                    FileName = "cmd.exe",
                    RedirectStandardInput = true,
                    UseShellExecute = false
                };

                process.StartInfo = startInfo;
                process.Start();


                //delete
                /*string kafkaDeleteTopicCommand = @".\kafka-topics.bat --delete --zookeeper " + zookeeperUrl + "  --topic ";

                foreach (var q in queues)
                {
                    await process.StandardInput.WriteLineAsync(kafkaDeleteTopicCommand + q).ConfigureAwait(false);

                }     */         


                //create
                string kafkaCreateTopicCommand = @".\kafka-topics.bat --create --zookeeper " + zookeeperUrl + " --partitions " + numberOfPartitions + " --replication-factor " + replicationFactor + " --topic ";

                foreach (var q in queues)
                {

                    await process.StandardInput.WriteLineAsync(kafkaCreateTopicCommand + q).ConfigureAwait(false);
                }
                
                
            }
            catch (Exception ex)
            {
                Logger.Warn(ex.ToString());
            }



        }
    }
}
