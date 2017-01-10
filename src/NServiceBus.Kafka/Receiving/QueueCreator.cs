using NServiceBus.Transport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NServiceBus.Transport.Kafka.Receiving
{
    class QueueCreator : ICreateQueues
    {
        public Task CreateQueueIfNecessary(QueueBindings queueBindings, string identity)
        {

            var process = new System.Diagnostics.Process();

            var startInfo = new System.Diagnostics.ProcessStartInfo
            {
                WorkingDirectory = @"C:\kafka_2.11-0.10.1.0\bin\windows",
                WindowStyle = System.Diagnostics.ProcessWindowStyle.Normal,
                FileName = "cmd.exe",
                RedirectStandardInput = true,
                UseShellExecute = false
            };

            process.StartInfo = startInfo;
            process.Start();

            foreach(var q in queueBindings.ReceivingAddresses)
                process.StandardInput.WriteLine(@".\kafka-topics.bat --create --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --topic "+q);

            foreach (var q in queueBindings.SendingAddresses)
                process.StandardInput.WriteLine(@".\kafka-topics.bat --create --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --topic " + q);


            return Task.FromResult(0);

        }
    }
}
