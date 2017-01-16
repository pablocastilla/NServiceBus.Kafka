using NServiceBus.Transport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Settings;
using NServiceBus.Transports.Kafka.Administration;

namespace NServiceBus.Transport.Kafka.Receiving
{
    class QueueCreator : ICreateQueues
    {
        private SettingsHolder settings;

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
                    numberOfPartitions = "3";

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

                string kafkaCreateTopicCommand = @".\kafka-topics.bat --create --zookeeper " + zookeeperUrl + " --partitions " + numberOfPartitions + " --replication-factor " + replicationFactor + " --topic ";


                foreach (var q in queueBindings.ReceivingAddresses)
                    await process.StandardInput.WriteLineAsync(kafkaCreateTopicCommand + q).ConfigureAwait(false);

                foreach (var q in queueBindings.SendingAddresses)
                    await process.StandardInput.WriteLineAsync(kafkaCreateTopicCommand + q).ConfigureAwait(false);
            }
            catch (Exception ex)
            {

            }

           

        }
    }
}
