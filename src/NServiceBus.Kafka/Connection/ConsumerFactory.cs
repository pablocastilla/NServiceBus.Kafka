
using NServiceBus.Logging;
using RdKafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Settings;
using NServiceBus.Transports.Kafka.Administration;

namespace NServiceBus.Transports.Kafka.Connection
{

    class ConsumerFactory
    {
        static ILog Logger = LogManager.GetLogger(typeof(ConsumerFactory));

        string connectionString;

        public ConsumerFactory(string connectionString, string endpointName, SettingsHolder settings) 
        {
            this.connectionString = connectionString;

            this.config = new RdKafka.Config { GroupId = endpointName, EnableAutoCommit = false };

            bool debugEnabled;
            if (settings.TryGet<bool>(WellKnownConfigurationKeys.KafkaDebugEnabled, out debugEnabled) && debugEnabled)
                config["debug"] = "all";

            var defaultConfig = new TopicConfig();
            defaultConfig["auto.offset.reset"] = "earliest";

            string sessionTimeout;
            if (settings.TryGet<string>(WellKnownConfigurationKeys.KafkaSessionTimeout, out sessionTimeout))
                config["session.timeout.ms"] = sessionTimeout;
            else
                config["session.timeout.ms"] = "30000";


            string heartBeatInterval;
            if (settings.TryGet<string>(WellKnownConfigurationKeys.KafkaHeartBeatInterval, out heartBeatInterval))
                config["heartbeat.interval.ms"] = heartBeatInterval;
            else
                config["heartbeat.interval.ms"] = "1000";

            config.DefaultTopicConfig = defaultConfig;
        }

        public EventConsumer CreateConsumer()
        {                  
           return new EventConsumer(config, connectionString);
        }

        private RdKafka.Config config;
    }
}
