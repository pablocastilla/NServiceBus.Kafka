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
        static Producer instance;
        string connectionString;
        object o = new object();
                
        public ProducerFactory(string connectionString)
        {
            this.connectionString = connectionString;

        
            if(instance==null)
            {
                lock (o)
                {
                    if(instance==null)
                        instance = new Producer(this.connectionString);
                }
            }

        }

        public Producer GetProducer()
        {
            return new Producer(this.connectionString);
            //return instance;

        }
    }
}
