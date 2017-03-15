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
using System.Globalization;
using System.Configuration;

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
            settings.Set("Recoverability.DisableLegacyRetriesSatellite", true);

            var appSettings = ConfigurationManager.AppSettings;

            settings.Set(WellKnownConfigurationKeys.KafkaDebugEnabled, Convert.ToBoolean(appSettings[WellKnownConfigurationKeys.KafkaDebugEnabled] ?? "false"));
            settings.Set(WellKnownConfigurationKeys.KafkaSessionTimeout, Convert.ToInt32(appSettings[WellKnownConfigurationKeys.KafkaSessionTimeout] ?? "30000"));
            settings.Set(WellKnownConfigurationKeys.KafkaHeartBeatInterval, Convert.ToInt32(appSettings[WellKnownConfigurationKeys.KafkaHeartBeatInterval] ?? "5000"));
            settings.Set(WellKnownConfigurationKeys.CreateQueues, Convert.ToBoolean(appSettings[WellKnownConfigurationKeys.CreateQueues] ?? "false"));
            settings.Set(WellKnownConfigurationKeys.KafkaPathToBin, appSettings[WellKnownConfigurationKeys.KafkaPathToBin] ?? @"C:\kafka_2.11-0.10.1.0\bin\windows");
            settings.Set(WellKnownConfigurationKeys.KafkaZooKeeperUrl, appSettings[WellKnownConfigurationKeys.KafkaZooKeeperUrl] ?? @"localhost:2181");
            settings.Set(WellKnownConfigurationKeys.NumberOfPartitions, appSettings[WellKnownConfigurationKeys.NumberOfPartitions] ?? @"1");
            settings.Set(WellKnownConfigurationKeys.ReplicationFactor, appSettings[WellKnownConfigurationKeys.ReplicationFactor] ?? @"1");


            // TODO: register metadata of the wrapper for the sake of XML serializer
            MessageMetadataRegistry registry;
            if (settings.TryGet(out registry) == false)
            {
                const BindingFlags flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.CreateInstance;

                registry = (MessageMetadataRegistry)Activator.CreateInstance(typeof(MessageMetadataRegistry), flags, null, new object[] { settings.GetOrCreate<Conventions>() }, CultureInfo.InvariantCulture);
                settings.Set<MessageMetadataRegistry>(registry);
            }

            settings.Get<MessageMetadataRegistry>().GetMessageMetadata(typeof(MessageWrapper));

            settings.SetDefault(WellKnownConfigurationKeys.ConnectionString, connectionString);    
                               

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
