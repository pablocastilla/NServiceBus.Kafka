Kafka transport for NServiceBus. Part of:
https://pablocastilla.wordpress.com/2017/01/03/my-proposal-for-joining-net-and-the-hadoop-ecosystem/

## The nuget package  [![NuGet Status](http://img.shields.io/nuget/v/NServiceBus.Transports.Kafka.svg?style=flat)](https://www.nuget.org/packages/NServiceBus.Transports.Kafka/)

https://www.nuget.org/packages/NServiceBus.Transports.Kafka/

    PM> Install-Package NServiceBus.Transports.Kafka
    
## Documentation

https://docs.particular.net/nservicebus/kafka/

## Usage

Example of configuring the consumer:
            
            endpointConfiguration.UseTransport<KafkaTransport>().ConnectionString("127.0.0.1:9092");
 
                
                
Example of configuring the sender:

       static async Task<IEndpointInstance> GetInstance()
        {
            var endpointConfiguration = new EndpointConfiguration("EndpointName");
            endpointConfiguration.UseTransport<KafkaTransport>().ConnectionString("127.0.0.1:9092"); ;
            endpointConfiguration.SendOnly();
            return await Endpoint.Start(endpointConfiguration)
                .ConfigureAwait(false);
        }
