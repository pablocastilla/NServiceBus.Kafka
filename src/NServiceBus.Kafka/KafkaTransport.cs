using NServiceBus.Transport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Settings;
using NServiceBus.Transport.Kafka;

namespace NServiceBus
{
    public class KafkaTransport : TransportDefinition
    {
        public override string ExampleConnectionStringForErrorMessage => "";


        public override TransportInfrastructure Initialize(SettingsHolder settings, string connectionString) => new KafkaTransportInfrastructure(settings, connectionString);
      
    }
}
