using NServiceBus.Transport;
using NServiceBus.Transports.Kafka.Connection;
using RdKafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NServiceBus.Transport.Kafka.Receiving
{
    class MessagePump : IPushMessages, IDisposable
    {
        Func<MessageContext, Task> onMessage;
        Func<ErrorContext, Task<ErrorHandleResult>> onError;
        ConsumerFactory consumerFactory;
        EventConsumer consumer;
        
        string endpointName;

        public MessagePump(ConsumerFactory consumerFactory,  string endpointName)
        {
            this.consumerFactory = consumerFactory;            
            this.endpointName = endpointName;
        }

        public Task Init(Func<MessageContext, Task> onMessage, Func<ErrorContext, Task<ErrorHandleResult>> onError, CriticalError criticalError, PushSettings settings)
        {
            this.onMessage = onMessage;
            this.onError = onError;
            
            //TODO: circuit breaker?
            //circuitBreaker = new MessagePumpConnectionFailedCircuitBreaker($"'{settings.InputQueue} MessagePump'", timeToWaitBeforeTriggeringCircuitBreaker, criticalError);

                       
            return Task.FromResult(0);
        }

        public void Start(PushRuntimeSettings limitations)
        {
            var consumer = consumerFactory.GetConsumer();

            consumer.Subscribe(new List<string>() { endpointName});
            consumer.OnPartitionsAssigned += Consumer_OnPartitionsAssigned;
            consumer.OnMessage += Consumer_OnMessage;
        }

        private void Consumer_OnPartitionsAssigned(object sender, List<TopicPartitionOffset> e)
        {
            //TODO: circuit breaker ok
        }

        private void Consumer_OnMessage(object sender, Message e)
        {
            
        }

        public Task Stop()
        {
            
        }


        public void Dispose()
        {
            
        }
    }
}
