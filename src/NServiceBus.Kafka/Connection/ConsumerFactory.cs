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
        
        string connectionString;
        string endpointName;

        public ConsumerFactory(string connectionString, string endpointName)
        {
            this.connectionString = connectionString;
            this.endpointName = endpointName;
        }

        public EventConsumer GetConsumer()
        {
            var config = new RdKafka.Config() { GroupId = endpointName };
            return new EventConsumer(config,connectionString);
        }
    }
}
