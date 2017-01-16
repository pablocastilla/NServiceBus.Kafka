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
        public const string KafkaDebugEnabled = "Transport.Kafka.DebugEnabled";
        public const string KafkaSessionTimeout = "Transport.Kafka.SessionTimeout";
        public const string KafkaHeartBeatInterval = "Transport.Kafka.HeartBeatInterval";
        public const string KafkaPathToBin = "Transport.Kafka.PathToBin";
        public const string KafkaZooKeeperUrl = "Transport.Kafka.ZooKeeperUrl";
        public const string NumberOfPartitions = "Transport.Kafka.NumberOfPartitions";
        public const string ReplicationFactor = "Transport.Kafka.ReplicationFactor";
        public const string CreateQueues = "Transport.Kafka.CreateQueues";


    }
}
