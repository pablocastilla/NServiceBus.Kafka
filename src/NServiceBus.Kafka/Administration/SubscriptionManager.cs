using NServiceBus.Transport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Extensibility;

namespace NServiceBus.Transports.Kafka.Administration
{
    class SubscriptionManager : IManageSubscriptions
    {
        public Task Subscribe(Type eventType, ContextBag context)
        {
            throw new NotImplementedException();
        }

        public Task Unsubscribe(Type eventType, ContextBag context)
        {
            throw new NotImplementedException();
        }
    }
}
