using Psns.Common.Functional;
using System;

namespace Psns.Common.Clients.Broker
{
    /// <summary>
    /// Called by <see cref="IBrokerClient"/> as messages are received from the Broker queue.
    /// </summary>
    public interface IBrokerObserver
    {
        /// <summary>
        /// Notifies the observer that the client has finished sending messages.
        /// </summary>
        void OnCompleted();

        /// <summary>
        /// Called when any error occurs during the client processing.
        /// </summary>
        /// <param name="error"></param>
        /// <param name="message">The message being processed when the error occurred</param>
        void OnError(Exception error, Maybe<BrokerMessage> message);

        /// <summary>
        /// Called when a new message is received.
        /// </summary>
        /// <param name="message"></param>
        void OnNext(BrokerMessage message);
    }
}
