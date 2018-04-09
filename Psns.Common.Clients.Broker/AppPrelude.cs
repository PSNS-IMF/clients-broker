using Psns.Common.Functional;
using Psns.Common.SystemExtensions;
using Psns.Common.SystemExtensions.Diagnostics;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
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
    public delegate TryAsync<UnitValue> ProcessMessageAsync(BrokerMessage message);

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
            TaskScheduler,
            CancellationToken,
            EndDialog, 
            IEnumerable<IObserver<BrokerMessage>>, 
            BrokerMessage, 
            TryAsync<UnitValue>> ProcessMessageAsyncFactory() =>
            (logger, scheduler, cancelToken, endDialog, observers, message) =>
                Match(
                    message == BrokerMessage.Empty,
                    NotEqual(true, _ =>
                        logger.Debug(
                            endDialog(message.Conversation).Bind(async u =>
                                {
                                    var result = UnitValue.Default.AsTask();    

                                    switch(message.MessageType)
                                    {
                                        case ServiceBrokerErrorMessageType:
                                            result = logger.Debug(observers, "Calling Observers OnError")
                                                .Iter(obs => obs.SendError(new Exception(message.Message), logger), cancelToken, scheduler);
                                            break;
                                        case ServiceBrokerEndDialogMessageType:
                                            result = logger.Debug(Unit, "Received EndDialog message").AsTask();
                                            break;
                                        default:
                                            result = logger.Debug(observers, "Calling Observers OnNext")
                                                .Iter(obs => obs.SendNext(message, logger), cancelToken, scheduler);
                                            break;
                                    }

                                    return await result;
                                }),
                            "Ending Dialog")),
                    _ => TryAsync(() => Unit.AsTask()));

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

        public static TryAsync<T> Log<T>(this TryAsync<T> self,
            Maybe<Log> mLog, 
            Func<T, string> description, 
            TraceEventType type = TraceEventType.Information) => async () =>
                await self.Bind(val => 
                    TryAsync(() => mLog.Match(
                        some: log => log.Log(val, description, type: type), 
                        none: () => val)
                    .AsTask()))
                .TryAsync();

        /// <summary>
        /// Perform an action on all <see cref="IEnumerable{T}"/> of <see cref="IObservable{T}"/> asynchronously.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="R"></typeparam>
        /// <param name="self"></param>
        /// <param name="func"></param>
        /// <param name="token"></param>
        /// <param name="scheduler"></param>
        /// <returns></returns>
        internal static async Task<UnitValue> Iter<T>(this IEnumerable<IObserver<T>> self,
            Action<IObserver<T>> func,
            CancellationToken token,
            TaskScheduler scheduler) =>
                (await Task.WhenAll(self.Aggregate(
                    new List<Task<UnitValue>>(),
                    (list, next) =>
                    {
                        list.Add(Task.Factory.StartNew(() => 
                            {
                                token.ThrowIfCancellationRequested();
                                func(next);
                                return Unit;
                            }, 
                            token, 
                            TaskCreationOptions.None, 
                            scheduler));

                        return list;
                    })))
                .FirstOrDefault();
    }
}