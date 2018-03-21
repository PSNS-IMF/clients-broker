using System;
using System.Diagnostics.Contracts;

namespace Psns.Common.Clients.Broker
{
    [Pure]
    public class BrokerMessage
    {
        public static readonly BrokerMessage Empty = new BrokerMessage(string.Empty, string.Empty, string.Empty, Guid.Empty, Guid.Empty);

        public readonly string Contract;
        public readonly string MessageType;
        public readonly string Message;
        public readonly Guid ConversationGroup;
        public readonly Guid Conversation;

        public BrokerMessage(string contract, string messageType, string message, Guid conversationGroup, Guid conversation)
        {
            Contract = contract ?? string.Empty;
            MessageType = messageType ?? string.Empty;
            Message = message ?? string.Empty;
            ConversationGroup = conversationGroup;
            Conversation = conversation;
        }

        public override string ToString() =>
            $@"Contract: {Contract} Type: {MessageType} Message: {Message} Conversation Group: {
                ConversationGroup.ToString()} Conversation: {Conversation.ToString()}";

        [Pure]
        public static bool operator ==(BrokerMessage a, BrokerMessage b) => a.Equals(b);

        [Pure]
        public static bool operator !=(BrokerMessage a, BrokerMessage b) => !a.Equals(b);

        public override bool Equals(object obj)
        {
            if(obj != null && obj is BrokerMessage)
            {
                var message = (BrokerMessage)obj;

                return message.Message == Message &&
                    message.MessageType == MessageType &&
                    message.ConversationGroup == ConversationGroup &&
                    message.Conversation == Conversation;
            }
            else
                return false;
        }

        public override int GetHashCode() =>
            Contract.GetHashCode() ^
            Message.GetHashCode() ^
            MessageType.GetHashCode() ^
            ConversationGroup.GetHashCode() ^
            Conversation.GetHashCode();
    }

    public static class BrokerMessageExtensions
    {
        public static BrokerMessage WithMessage(this BrokerMessage self, string message) =>
            new BrokerMessage(self.Contract, self.MessageType, message, self.ConversationGroup, self.Conversation);

        public static BrokerMessage WithType(this BrokerMessage self, string messageType) =>
            new BrokerMessage(self.Contract, messageType, self.Message, self.ConversationGroup, self.Conversation);

        public static BrokerMessage WithContract(this BrokerMessage self, string contract) =>
            new BrokerMessage(contract, self.MessageType, self.Message, self.ConversationGroup, self.Conversation);
    }
}