using NServiceBus.Transport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Extensibility;
using NServiceBus.Transports.Kafka.Connection;
using RdKafka;
using System.Collections.Concurrent;

namespace NServiceBus.Transports.Kafka.Administration
{
    class SubscriptionManager : IManageSubscriptions
    {
        private readonly ConsumerFactory consumerFactory;

        public SubscriptionManager(ConsumerFactory consumerFactory)
        {
            this.consumerFactory = consumerFactory;
        }

        public Task Subscribe(Type eventType, ContextBag context)
        {
           

            SetupTypeSubscriptions( eventType);

            return Task.FromResult(0); 
        }

        public Task Unsubscribe(Type eventType, ContextBag context)
        {
            var consumer = consumerFactory.GetConsumer();

           // consumer.Unsubscribe();

            return Task.FromResult(0);
        }

       

        void SetupTypeSubscriptions(Type type)
        {          
            var typeToProcess = type;
            CreateSubscription( ExchangeName(typeToProcess));
            var baseType = typeToProcess.BaseType;

            while (baseType != null)
            {
                if (type == typeof(Object))
                {
                    continue;
                }

                CreateSubscription( ExchangeName(baseType));
                
                typeToProcess = baseType;
                baseType = typeToProcess.BaseType;
            }

            foreach (var interfaceType in type.GetInterfaces())
            {
                var exchangeName = ExchangeName(interfaceType);

                CreateSubscription( exchangeName);
               
            }

           
        }

        void CreateSubscription(string exchangeName)
        {
            if (IsTypeTopologyKnownConfigured(exchangeName))
                return;

            var consumer = consumerFactory.GetConsumer();
            consumer.Subscribe(new List<string>() { exchangeName });
            /* consumer.OnPartitionsAssigned += Consumer_OnPartitionsAssigned;

             consumer.Start();*/
            //consumer.Start();

            MarkTypeConfigured(exchangeName,consumer);
        }

       /* private void Consumer_OnPartitionsAssigned(object sender, List<TopicPartitionOffset> e)
        {
            //TODO: circuit breaker ok

            ((EventConsumer)sender).Assign(e);
        }*/

        bool IsTypeTopologyKnownConfigured(string exchangeName) => typeTopologyConfiguredSet.ContainsKey(exchangeName);
        readonly ConcurrentDictionary<string, EventConsumer> typeTopologyConfiguredSet = new ConcurrentDictionary<string, EventConsumer>();
        static string ExchangeName(Type type) => type.Namespace + "." + type.Name;

        void MarkTypeConfigured(string exchangeName, EventConsumer consumer)
        {
            typeTopologyConfiguredSet[exchangeName] = consumer;
        }
    }
}
