using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.AcceptanceTesting.Support;
using NServiceBus.AcceptanceTests.ScenarioDescriptors;
using NServiceBus.Transport.Kafka.AcceptanceTests;
using NServiceBus.Transport.Kafka;

public class ConfigureScenariosForKafkaTransport : IConfigureSupportedScenariosForTestExecution
{
    public IEnumerable<Type> UnsupportedScenarioDescriptorTypes => new[] {   typeof(AllTransportsWithMessageDrivenPubSub), typeof(AllTransportsWithoutNativeDeferral) };
}

public class ConfigureEndpointKafkaTransport : IConfigureEndpointTestExecution
{
    DbConnectionStringBuilder connectionStringBuilder;

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