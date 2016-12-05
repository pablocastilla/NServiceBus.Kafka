using NServiceBus.Transport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Settings;
using NServiceBus.Transport.Kafka;
using NServiceBus.Unicast.Messages;
using NServiceBus.Transports.Kafka.Wrapper;
using NServiceBus.Serialization;
using NServiceBus.MessageInterfaces;
using System.Reflection;
using NServiceBus.Transports.Kafka.Administration;

namespace NServiceBus.Transport.Kafka
{
    public class KafkaTransport : TransportDefinition
    {
        public override string ExampleConnectionStringForErrorMessage => "";
        internal static MessageWrapperSerializer serializer;
        


        public override TransportInfrastructure Initialize(SettingsHolder settings, string connectionString)
        {
            // configure JSON instead of XML as the default serializer:
            SetMainSerializer(settings, new JsonSerializer());

            // register the MessageWrapper as a system message to have it registered in mappings and serializers
            settings.GetOrCreate<Conventions>().AddSystemMessagesConventions(t => t == typeof(MessageWrapper));

            // TODO: register metadata of the wrapper for the sake of XML serializer
           // settings.Get<MessageMetadataRegistry>().GetMessageMetadata(typeof(MessageWrapper));

            return new KafkaTransportInfrastructure(settings, connectionString);
        }


        static void SetMainSerializer(SettingsHolder settings, SerializationDefinition definition)
        {
            settings.SetDefault("MainSerializer", Tuple.Create(definition, new SettingsHolder()));
        }

        internal static IMessageSerializer GetMainSerializer(IMessageMapper mapper, ReadOnlySettings settings)
        {
            var definitionAndSettings = settings.Get<Tuple<SerializationDefinition, SettingsHolder>>("MainSerializer");
            var definition = definitionAndSettings.Item1;
            var serializerSettings = definitionAndSettings.Item2;

            // serializerSettings.Merge(settings);
            var merge = typeof(SettingsHolder).GetMethod("Merge", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            merge.Invoke(serializerSettings, new object[]
            {
                settings
            });

            var serializerFactory = definition.Configure(serializerSettings);
            var serializer = serializerFactory(mapper);
            return serializer;
        }

        

    }


}
