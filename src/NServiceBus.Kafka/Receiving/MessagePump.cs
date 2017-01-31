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

        // Stop
        TaskCompletionSource<bool> connectionShutdownCompleted;

        public MessagePump( SettingsHolder settingsHolder,string connectionString)
        {           
           
            this.settingsHolder = settingsHolder;
            this.connectionString = connectionString;
        }

        public Task Init(Func<MessageContext, Task> onMessage, Func<ErrorContext, Task<ErrorHandleResult>> onError, CriticalError criticalError, PushSettings settings)
        {
            this.inputQueue = settings.InputQueue;

            var consumerHolder = new ConsumerHolder(connectionString, inputQueue, settings, settingsHolder, onMessage, onError);
            consumerHolderList.Add(consumerHolder);
            

            if (inputQueue== settingsHolder.EndpointName())
            {
                mainConsumer = consumerHolder;
                this.onError = onError;
                this.onMessage = onMessage;
                eventsConsumer = new ConsumerHolder(connectionString, inputQueue, settings, settingsHolder, onMessage, onError,true);
                consumerHolderList.Add(eventsConsumer);
            }

            //TODO: circuit breaker?
            //circuitBreaker = new MessagePumpConnectionFailedCircuitBreaker($"'{settings.InputQueue} MessagePump'", timeToWaitBeforeTriggeringCircuitBreaker, criticalError);
                    
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
            //runningReceiveTasks = new ConcurrentDictionary<Task, Task>();
            messageProcessing = new CancellationTokenSource();

            maxConcurrency = limitations.MaxConcurrency;
            semaphore = new SemaphoreSlim(limitations.MaxConcurrency, limitations.MaxConcurrency);

            Parallel.ForEach(consumerHolderList, ch => ch.Init(messageProcessing));
            Parallel.ForEach(consumerHolderList, ch => ch.Start());

        }

           
        public async Task Stop()
        {
            
            Logger.Debug($"consumer.OnMessage -= Consumer_OnMessage");

          
            messageProcessing.Cancel();
            // ReSharper disable once MethodSupportsCancellation
            var timeoutTask = Task.Delay(StoppingAllTasksTimeout);

            /*  var finishedTask = await Task.WhenAny(Task.WhenAll(allTasks), timeoutTask).ConfigureAwait(false);

               if (finishedTask.Equals(timeoutTask))
               {
                   Logger.Error("The message pump failed to stop with in the time allowed(30s)");
               }*/

            foreach (var ch in consumerHolderList)
                await ch.Stop();



        }


        public void Dispose()
        {
            consumerHolderList.ForEach(ch => ch.Dispose());
        }
    }
}
