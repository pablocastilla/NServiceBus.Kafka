using NServiceBus.Logging;
using NServiceBus.Transport;
using NServiceBus.Transports.Kafka.Connection;
using RdKafka;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Transports.Kafka.Wrapper;
using NServiceBus;
using NServiceBus.Extensibility;
using NServiceBus.Settings;

namespace NServiceBus.Transport.Kafka.Receiving
{

    class MessagePump : IPushMessages, IDisposable
    {

        List<ConsumerHolder> consumerHolderList = new List<ConsumerHolder>();
             
        static TimeSpan StoppingAllTasksTimeout = TimeSpan.FromSeconds(30);

        static ILog Logger = LogManager.GetLogger(typeof(MessagePump));
       
        static readonly TransportTransaction transportTranaction = new TransportTransaction();
        private PushSettings settings;
        private SettingsHolder settingsHolder;
        private string connectionString;
        private string inputQueue;

        // Start
        int maxConcurrency;
        SemaphoreSlim semaphore;
        CancellationTokenSource messageProcessing;

        ConsumerHolder mainConsumer;
        ConsumerHolder eventsConsumer;
        Func<MessageContext, Task> onMessage;
        Func<ErrorContext, Task<ErrorHandleResult>> onError;

      

        public MessagePump( SettingsHolder settingsHolder,string connectionString)
        {           
           
            this.settingsHolder = settingsHolder;
            this.connectionString = connectionString;
        }

        public Task Init(Func<MessageContext, Task> onMessage, Func<ErrorContext, Task<ErrorHandleResult>> onError, CriticalError criticalError, PushSettings settings)
        {
            this.inputQueue = settings.InputQueue;

            var consumerHolder = new ConsumerHolder(connectionString, inputQueue, settings, settingsHolder, onMessage, onError,criticalError);
            consumerHolderList.Add(consumerHolder);
      

            if (inputQueue== settingsHolder.EndpointName())
            {
                mainConsumer = consumerHolder;
                this.onError = onError;
                this.onMessage = onMessage;
                eventsConsumer = new ConsumerHolder(connectionString, inputQueue, settings, settingsHolder, onMessage, onError, criticalError,true);
                consumerHolderList.Add(eventsConsumer);
            }

                      
            return Task.FromResult(0);
        }

        public ConsumerHolder GetEventsConsumerHolder()
        {
            return eventsConsumer;
        }

        public EventConsumer GetMainConsumer()
        {
            return mainConsumer.GetConsumer();
        }

        public void Start(PushRuntimeSettings limitations)
        {          
           
            messageProcessing = new CancellationTokenSource();

            maxConcurrency = limitations.MaxConcurrency;
            semaphore = new SemaphoreSlim(limitations.MaxConcurrency, limitations.MaxConcurrency);

            Parallel.ForEach(consumerHolderList, ch => ch.Init(messageProcessing));
            Parallel.ForEach(consumerHolderList, ch => ch.Start(limitations));

        }

           
        public async Task Stop()
        {
            
            Logger.Debug($"consumer.OnMessage -= Consumer_OnMessage");

          
            messageProcessing.Cancel();
            
            var timeoutTask = Task.Delay(StoppingAllTasksTimeout);
                   

            foreach (var ch in consumerHolderList)
                await ch.Stop();
            

        }


        public void Dispose()
        {
            consumerHolderList.ForEach(ch => ch.Dispose());
        }
    }
}
