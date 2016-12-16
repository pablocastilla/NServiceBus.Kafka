using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NServiceBus.Transports.Kafka.Administration
{
    static class WellKnownConfigurationKeys
    {

        public const string MessageWrapperSerializationDefinition = "Transport.Kafka.MessageWrapperSerializationDefinition";
        public const string ConnectionString = "Transport.Kafka.ConnectionString";

    }
}
