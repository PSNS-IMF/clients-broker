using System;
using System.Diagnostics.Contracts;

namespace Psns.Common.Clients.Broker
{
    [Pure]
    public class BrokerMessage
    {
        public static readonly BrokerMessage Empty = new BrokerMessage(string.Empty, string.Empty, Guid.Empty, Guid.Empty);

        public readonly string MessageType;
        public readonly string Message;
        public readonly Guid ConversationGroup;
        public readonly Guid Conversation;

        public BrokerMessage(string messageType, string message, Guid conversationGroup, Guid conversation)
        {
            MessageType = messageType ?? string.Empty;
            Message = message ?? string.Empty;
            ConversationGroup = conversationGroup;
            Conversation = conversation;
        }

        public BrokerMessage WithMessage(string message) =>
            new BrokerMessage(MessageType, message, ConversationGroup, Conversation);

        public BrokerMessage WithType(string messageType) =>
            new BrokerMessage(messageType, Message, ConversationGroup, Conversation);

        public override string ToString() =>
            $@"Type: {MessageType} Message: {Message} Conversation Group: {ConversationGroup.ToString()} Conversation: {Conversation.ToString()}";

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
            Message.GetHashCode() ^
            MessageType.GetHashCode() ^
            ConversationGroup.GetHashCode() ^
            Conversation.GetHashCode();
    }
}