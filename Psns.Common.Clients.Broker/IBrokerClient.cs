using System;
using System.Threading;
using System.Threading.Tasks;

using CSharpFunctionalExtensions;

namespace Psns.Common.Clients.Broker
{
    /// <summary>
    /// Provides communications with SQL Service Broker
    /// </summary>
    public interface IBrokerClient : IDisposable
    {
        /// <summary>
        /// Defines a method that opens the connection
        /// </summary>
        /// <returns></returns>
        Result Open();

        /// <summary>
        /// Defines a method that begins a transaction
        /// </summary>
        /// <returns></returns>
        Result BeginTransaction();

        /// <summary>
        /// Defines a method that sends a Service Broker message
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        Task<Result> SendAsync(BrokerMessage message);

        /// <summary>
        /// Defines a method that receives a Service Broker message
        /// </summary>
        /// <param name="queueName">The queue to listen on</param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task<Result<BrokerMessage>> ReceiveAsync(string queueName, CancellationToken token);

        /// <summary>
        /// Defines a method that ends a Service Broker conversation
        /// </summary>
        /// <param name="conversationHandle"></param>
        /// <returns></returns>
        Task<Result> EndConversationAsync(Guid conversationHandle);

        /// <summary>
        /// Defines a methods that ends a Service Broker conversation with an Error
        /// </summary>
        /// <param name="conversationHandle"></param>
        /// <param name="errorCode"></param>
        /// <param name="errorDescription"></param>
        /// <returns></returns>
        Task<Result> EndConversationWithErrorAsync(Guid conversationHandle, int errorCode, string errorDescription);

        /// <summary>
        /// Defines a method that commits an active transaction
        /// </summary>
        /// <returns></returns>
        Result Commit();

        /// <summary>
        /// Defines a method that rolls back an active transaction
        /// </summary>
        /// <returns></returns>
        Result Rollback();
    }
}