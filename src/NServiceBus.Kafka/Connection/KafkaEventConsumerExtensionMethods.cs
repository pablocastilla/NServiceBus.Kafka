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
        static Object o = new Object();
        static ILog Logger = LogManager.GetLogger(typeof(EventConsumer));
        static List<string> subscriptionList = new List<string>();

        public static void AddSubscriptionsBlocking(this EventConsumer consumer, List<string> topicsToAdd)
        {
            lock (o)
            {             
             
                var finalTopics = topicsToAdd.Where(t => !subscriptionList.Contains(t));

                if (finalTopics.Count() == 0)
                    return;

                foreach (var topic in finalTopics)
                {
                    if (!subscriptionList.Contains(topic))
                        subscriptionList.Add(topic);

                }

                Logger.Info("Subscriptions:" + string.Join(", ", subscriptionList));

                

            }
        }

        public static void CommitSubscriptionsBlocking(this EventConsumer consumer)
        {
            lock (o)
            {             

                Logger.Info("Subscriptions committed:" + string.Join(", ", subscriptionList));

                consumer.Subscribe(subscriptionList);

            }
        }
    }
}
