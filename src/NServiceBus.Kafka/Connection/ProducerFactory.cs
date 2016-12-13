using RdKafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NServiceBus.Transports.Kafka.Connection
{
    class ProducerFactory
    {        
        Producer instance;
        string connectionString;

        public ProducerFactory(string connectionString)
        {
            this.connectionString = connectionString;

            instance = new Producer(this.connectionString);
        }

        public Producer GetProducer()
        {
            
            return instance;
        }
    }
}
