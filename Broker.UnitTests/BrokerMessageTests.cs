using System;
using NUnit.Framework;
using Psns.Common.Clients.Broker;

namespace Broker.UnitTests
{
    [TestFixture]
    public class BrokerMessageTests : AssertionHelper
    {
        [Test]
        public void Constructor_MessageAsNull_MessageSetToEmptyString()
        {
            Expect(new BrokerMessage("Contract", "type", null, Guid.Empty, Guid.Empty).Message, EqualTo(string.Empty));
        }

        [Test]
        public void Constructor_MessageTypeAsNull_MessageTypeSetToEmptyString()
        {
            Expect(new BrokerMessage("Contract", null, "Message", Guid.Empty, Guid.Empty).MessageType, EqualTo(string.Empty));
        }

        [Test]
        public void Equals_BothEmpty_AreEqual()
        {
            Expect(BrokerMessage.Empty, EqualTo(BrokerMessage.Empty));
            Expect(BrokerMessage.Empty == new BrokerMessage(string.Empty, string.Empty, string.Empty, Guid.Empty, Guid.Empty), Is.True);
        }

        [Test]
        public void Equals_BothDifferent_AreNotEqual()
        {
            Expect(BrokerMessage.Empty != new BrokerMessage("contract", "type", "", Guid.Empty, Guid.Empty), Is.True);
        }
    }
}
