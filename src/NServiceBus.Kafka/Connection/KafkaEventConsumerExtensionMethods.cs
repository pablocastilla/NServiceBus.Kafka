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
        static Dictionary<string,List<Subscription>> subscriptionDictionary = new Dictionary<string, List<Subscription>>();

        public static void AddSubscriptionsBlocking(this EventConsumer consumer, List<string> topicsToAdd)
        {
            lock (o)
            {
                List<Subscription> subscriptionList;
                if (!subscriptionDictionary.ContainsKey(consumer.Name))
                {
                    subscriptionList = new List<Subscription>();
                    subscriptionDictionary.Add(consumer.Name, subscriptionList);
                }
                else
                    subscriptionList = subscriptionDictionary[consumer.Name];

                var finalTopics = topicsToAdd.Where(t => !subscriptionList.Select(s=> s.Topic).Contains(t));

                if (finalTopics.Count() == 0)
                    return;

                foreach (var topic in finalTopics)
                {                
                        
                     subscriptionList.Add(new Subscription() { Topic = topic });

                }

                Logger.Debug("Subscriptions added:" + string.Join(", ", subscriptionList));

                

            }
        }

        public static void CommitSubscriptionsBlocking(this EventConsumer consumer)
        {
            lock (o)
            {

                List<Subscription> subscriptionList;

                if (!subscriptionDictionary.ContainsKey(consumer.Name))
                {
                    subscriptionList = new List<Subscription>();
                    subscriptionDictionary.Add(consumer.Name, subscriptionList);
                }
                else
                    subscriptionList = subscriptionDictionary[consumer.Name];

                Logger.Debug("Subscriptions committed:" + string.Join(", ", subscriptionList));

                var subscriptionToCommit = subscriptionList.Select(s=>s.Topic).ToList();

                if (subscriptionToCommit.Count == 0)
                    return;

                consumer.Subscribe(subscriptionToCommit);

                subscriptionList.Where(s => subscriptionToCommit.Contains(s.Topic)).ToList().ForEach(s => s.Committed=true);

            }
        }

        class Subscription
        {
            public string Topic { get; set; }

            public bool Committed { get; set; }

        }
    }
}
