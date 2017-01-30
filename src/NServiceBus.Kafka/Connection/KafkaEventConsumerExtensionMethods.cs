using NServiceBus.Logging;
using RdKafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NServiceBus.Transports.Kafka.Connection
{
    public static class KafkaEventConsumerExtensionMethods
    {
     
        static ILog Logger = LogManager.GetLogger(typeof(EventConsumer));
    
        public static void AddSubscriptionsBlocking(this EventConsumer consumer, List<string> topicsToAdd)
        {

            consumer.Subscribe(topicsToAdd);
            Logger.Debug("Subscriptions added:" + string.Join(", ", topicsToAdd));


           
        }

     
        class Subscription
        {
            public string Topic { get; set; }

            public bool Committed { get; set; }

        }
    }
}
