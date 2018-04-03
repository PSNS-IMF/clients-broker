using Psns.Common.Functional;
using Psns.Common.SystemExtensions;
using Psns.Common.SystemExtensions.Diagnostics;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using static Psns.Common.Clients.Broker.Constants;
using static Psns.Common.Clients.Broker.DB;
using static Psns.Common.Functional.Prelude;

namespace Psns.Common.Clients.Broker
{
    /// <summary>
    /// A function that retreives a BrokerMessage from a ServiceBroker queue.
    /// </summary>
    /// <param name="queueName"></param>
    /// <returns></returns>
    public delegate TryAsync<BrokerMessage> GetMessage(string queueName);

    /// <summary>
    /// A function that ends a Service Broker Conversation.
    /// </summary>
    /// <param name="conversationId"></param>
    /// <returns></returns>
    public delegate TryAsync<UnitValue> EndDialog(Guid conversationId);

    /// <summary>
    /// Creates a function that evaluates and processes BrokerMessages.
    /// </summary>
    /// <returns></returns>
    public delegate Func<IEnumerable<IObserver<BrokerMessage>>, BrokerMessage, TryAsync<UnitValue>> ProcessMessageAsync();

    public static partial class AppPrelude
    {
        /// <summary>
        /// Gets a message from a Service Broker Queue.
        /// </summary>
        /// <returns></returns>
        public static Func<Maybe<Log>, Func<IDbConnection>, OpenAsync, ExecuteNonQueryAsync, string, TryAsync<BrokerMessage>> GetMessageFactory() => 
            (log, connectionFactory, openAsync, exeAsync, queueName) =>
                CommandFactory<BrokerMessage>()(
                    log,
                    connectionFactory,
                    openAsync,
                    SetupReceive().Par(queueName.AssertValue()),
                    RunReceiveCommandFactory().Par(exeAsync));

        /// <summary>
        /// End a Service Broker dialog.
        /// </summary>
        /// <returns></returns>
        public static Func<Maybe<Log>, Func<IDbConnection>, OpenAsync, ExecuteNonQueryAsync, Guid, TryAsync<UnitValue>> EndDialogFactory() =>
            (log, connectionFactory, openAsync, exeAsync, conversationId) =>
                CommandFactory<UnitValue>()(
                    log,
                    connectionFactory,
                    openAsync,
                    SetupEndDialog().Par(conversationId.AssertValue()),
                    async cmd => { await exeAsync(cmd); return Unit; });

        /// <summary>
        /// When message type is Service Broker Error, calls Observer.OnError
        /// Else when message type is Service Broker End Dialog, calls EndDialog
        /// Else call Observer.OnNext for all other message types.
        /// </summary>
        /// <returns></returns>
        public static Func<
            Maybe<Log>, 
            CancellationToken, 
            EndDialog, 
            IEnumerable<IObserver<BrokerMessage>>, 
            BrokerMessage, 
            TryAsync<UnitValue>> ProcessMessageAsyncFactory() =>
            (logger, cancelToken, endDialog, observers, message) => () =>
                Match(
                    message == BrokerMessage.Empty,
                    NotEqual(true, _ =>
                        logger.Debug(
                            endDialog(message.Conversation).Regardless(
                                Match(
                                    logger.Debug(message.MessageType, $"Received {message}"),
                                    // error message
                                    AsEqual(ServiceBrokerErrorMessageType, __ => logger.Debug(observers, "Calling Observers OnError")
                                        .TryIterAsync(observer =>
                                            observer.SendError(new Exception(message.Message), logger), cancelToken)),
                                    // end dialog message
                                    AsEqual(ServiceBrokerEndDialogMessageType, __ => TryAsync(() => Task.FromResult(Unit))),
                                    // give the rest to the observers
                                    __ => logger.Debug(observers, "Calling Observers OnNext")
                                        .TryIterAsync(observer => 
                                            observer.SendNext(message, logger), cancelToken))).TryAsync(), 
                            "Ending Dialog")),
                    _ => Some(TryAsync(() => Task.FromResult(Unit)).TryAsync()));

        /// <summary>
        /// Adds error handling to IObserver.OnNext
        /// </summary>
        /// <param name="self"></param>
        /// <param name="next"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public static UnitValue SendNext(this IObserver<BrokerMessage> self, BrokerMessage next, Maybe<Log> logger) =>
            Try(() => self.OnNext(next)).Match(_ => _, e => self.SendError(e, logger));

        /// <summary>
        /// Adds error handling to IObserver.OnError
        /// </summary>
        /// <param name="self"></param>
        /// <param name="exception"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public static UnitValue SendError(this IObserver<BrokerMessage> self, Exception exception, Maybe<Log> logger) =>
            Try(() => self.OnError(exception)).Match(_ => _, 
                e => logger.Error<UnitValue>(new AggregateException(exception, e).GetExceptionChainMessagesWithSql()));
    }
}