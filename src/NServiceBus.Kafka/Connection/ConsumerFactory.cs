using RdKafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NServiceBus.Transports.Kafka.Connection
{
    class ConsumerFactory
    {
        Consumer consumer;
        string connectionString;

        public ConsumerFactory(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public Consumer GetConsumer()
        {
            var config = new RdKafka.Config() { GroupId = "example-csharp-consumer" };
            return new Consumer(config,connectionString);
        }
    }
}
