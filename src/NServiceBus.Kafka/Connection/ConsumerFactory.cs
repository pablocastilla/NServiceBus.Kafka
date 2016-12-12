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

        EventConsumer consumer;
        static Object o = new Object();

        //TODO: singleton if only one consumer is used?consumer holder?
        public ConsumerFactory(string connectionString, string endpointName)
        {
            this.connectionString = connectionString;
            this.endpointName = endpointName;

            //TODO: sloopy singleton for testing
            if (consumer == null)
            {
                var config = new RdKafka.Config() { GroupId = endpointName };

                consumer = new EventConsumer(config, connectionString);
            }
        }

        public EventConsumer GetConsumer()
        {
                  
           return consumer;
        }


     
    }
}
