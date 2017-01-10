Kafka transport for NServiceBus.

Example of configuring the consumer:

  endpointConfiguration.DisableFeature<TimeoutManager>();

            endpointConfiguration.UseTransport<KafkaTransport>().ConnectionString("127.0.0.1:9092");
           
            var recoverability = endpointConfiguration.Recoverability();
            recoverability.Delayed(
                delayed =>
                {
                    delayed.NumberOfRetries(0);
                });
                
Example of configuring the sender:

       static async Task<IEndpointInstance> GetInstance()
        {
            var endpointConfiguration = new EndpointConfiguration("EndpointName");
            endpointConfiguration.UseTransport<KafkaTransport>().ConnectionString("127.0.0.1:9092"); ;
            endpointConfiguration.SendOnly();
            return await Endpoint.Start(endpointConfiguration)
                .ConfigureAwait(false);
        }
