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
        public readonly Guid ConversationHandle;

        public BrokerMessage(string messageType, string message, Guid conversationGroup, Guid conversationHandle)
        {
            MessageType = messageType ?? string.Empty;
            Message = message ?? string.Empty;
            ConversationGroup = conversationGroup;
            ConversationHandle = conversationHandle;
        }

        public BrokerMessage WithMessage(string message) =>
            new BrokerMessage(MessageType, message, ConversationGroup, ConversationHandle);

        public BrokerMessage WithType(string messageType) =>
            new BrokerMessage(messageType, Message, ConversationGroup, ConversationHandle);

        public override string ToString() =>
            $@"Type: {MessageType} Message: {Message} Conversation Group: {ConversationGroup.ToString()} Dialog: {ConversationHandle.ToString()}";

        public static bool operator ==(BrokerMessage a, BrokerMessage b) => a.Equals(b);

        public static bool operator !=(BrokerMessage a, BrokerMessage b) => !a.Equals(b);

        public override bool Equals(object obj)
        {
            if(obj != null && obj is BrokerMessage)
            {
                var message = (BrokerMessage)obj;

                return message.Message == Message &&
                    message.MessageType == MessageType &&
                    message.ConversationGroup == ConversationGroup &&
                    message.ConversationHandle == ConversationHandle;
            }
            else
                return false;
        }

        public override int GetHashCode() =>
            Message.GetHashCode() ^
            MessageType.GetHashCode() ^
            ConversationGroup.GetHashCode() ^
            ConversationHandle.GetHashCode();
    }
}