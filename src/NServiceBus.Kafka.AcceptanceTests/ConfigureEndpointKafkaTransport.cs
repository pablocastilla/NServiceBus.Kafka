using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NServiceBus;
using NServiceBus.AcceptanceTesting.Support;
using NServiceBus.AcceptanceTests.ScenarioDescriptors;
using NServiceBus.Transport.RabbitMQ.AcceptanceTests;
using NServiceBus.Transport.Kafka;

public class ConfigureScenariosForKafkaTransport : IConfigureSupportedScenariosForTestExecution
{
    public IEnumerable<Type> UnsupportedScenarioDescriptorTypes => new[] { typeof(AllDtcTransports), typeof(AllNativeMultiQueueTransactionTransports), typeof(AllTransportsWithMessageDrivenPubSub), typeof(AllTransportsWithoutNativeDeferralAndWithAtomicSendAndReceive) };
}

public class ConfigureEndpointKafkaTransport : IConfigureEndpointTestExecution
{
    DbConnectionStringBuilder connectionStringBuilder;


    static void ApplyDefault(DbConnectionStringBuilder builder, string key, string value)
    {
        if (!builder.ContainsKey(key))
        {
            builder.Add(key, value);
        }
    }

    public Task Configure(string endpointName, EndpointConfiguration configuration, RunSettings settings, PublisherMetadata publisherMetadata)
    {          
        configuration.UseTransport<KafkaTransport>().ConnectionString(Environment.GetEnvironmentVariable("KafkaTransport.ConnectionString") ?? "127.0.0.1:9092");

        return TaskEx.CompletedTask;
    }

    public Task Cleanup()
    {
        return TaskEx.CompletedTask;
    }
}