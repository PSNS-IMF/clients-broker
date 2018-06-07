﻿using Psns.Common.Functional;
using Psns.Common.SystemExtensions;
using Psns.Common.SystemExtensions.Diagnostics;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static Psns.Common.Clients.Broker.Constants;
using static Psns.Common.Clients.Broker.DB;
using static Psns.Common.Functional.Prelude;

namespace Psns.Common.Clients.Broker
{
    #region delegates
    /// <summary>
    /// A function that retreives a BrokerMessage from a ServiceBroker queue.
    /// </summary>
    /// <param name="queueName"></param>
    /// <returns></returns>
    public delegate TryAsync<BrokerMessage> GetMessageAsync(string queueName);

    /// <summary>
    /// A function that retreives a BrokerMessage from a ServiceBroker queue.
    /// </summary>
    /// <param name="queueName"></param>
    /// <returns></returns>
    public delegate Try<BrokerMessage> GetMessage(string queueName);

    /// <summary>
    /// A function that ends a Service Broker Conversation.
    /// </summary>
    /// <param name="conversationId"></param>
    /// <returns></returns>
    public delegate TryAsync<UnitValue> EndDialogAsync(Guid conversationId);

    /// <summary>
    /// A function that ends a Service Broker Conversation.
    /// </summary>
    /// <param name="conversationId"></param>
    /// <returns></returns>
    public delegate Try<UnitValue> EndDialog(Guid conversationId);

    /// <summary>
    /// Creates a function that evaluates and processes BrokerMessages.
    /// </summary>
    /// <returns></returns>
    public delegate TryAsync<UnitValue> ProcessMessageAsync(BrokerMessage message);

    /// <summary>
    /// Creates a function that evaluates and processes BrokerMessages.
    /// </summary>
    /// <returns></returns>
    public delegate Try<UnitValue> ProcessMessage(BrokerMessage message);
    #endregion

    public static partial class AppPrelude
    {
        /// <summary>
        /// Gets a message from a Service Broker Queue.
        /// </summary>
        /// <returns></returns>
        public static Func<Maybe<Log>, Func<IDbConnection>, OpenAsync, ExecuteNonQueryAsync, string, TryAsync<BrokerMessage>> GetMessageAsyncFactory() =>
            (log, connectionFactory, openAsync, exeAsync, queueName) =>
                CommandFactoryAsync<BrokerMessage>()(
                    log,
                    connectionFactory,
                    openAsync,
                    SetupReceive().Par(queueName.AssertValue()),
                    RunReceiveCommandFactory().Par(log, exeAsync));

        /// <summary>
        /// Gets a message from a Service Broker Queue.
        /// </summary>
        /// <returns></returns>
        public static Func<Maybe<Log>, Func<IDbConnection>, string, Try<BrokerMessage>> GetMessageFactory() =>
            (log, connectionFactory, queueName) =>
                CommandFactory<BrokerMessage>()(
                    log,
                    connectionFactory,
                    SetupReceive().Par(queueName.AssertValue()),
                    cmd => RunReceiveCommandFactory()
                        .Par(log, new ExecuteNonQueryAsync(c => c.ExecuteNonQuery().AsTask()))(cmd)
                        .Result);

        /// <summary>
        /// End a Service Broker dialog.
        /// </summary>
        /// <returns></returns>
        public static Func<Maybe<Log>, Func<IDbConnection>, OpenAsync, ExecuteNonQueryAsync, Guid, TryAsync<UnitValue>> EndDialogAsyncFactory() =>
            (log, connectionFactory, openAsync, exeAsync, conversationId) =>
                CommandFactoryAsync<UnitValue>()(
                    log,
                    connectionFactory,
                    openAsync,
                    SetupEndDialog().Par(log, conversationId.AssertValue()),
                    async cmd => { await exeAsync(cmd); return Unit; });

        /// <summary>
        /// End a Service Broker dialog.
        /// </summary>
        /// <returns></returns>
        public static Func<Maybe<Log>, Func<IDbConnection>, Guid, Try<UnitValue>> EndDialogFactory() =>
            (log, connectionFactory, conversationId) =>
                CommandFactory<UnitValue>()(
                    log,
                    connectionFactory,
                    SetupEndDialog().Par(log, conversationId.AssertValue()),
                    cmd => Unit.Tap(_ => cmd.ExecuteNonQuery()));

        /// <summary>
        /// Begin a Service Broker Conversation.
        /// </summary>
        /// <returns></returns>
        public static Func<
            Maybe<Log>, 
            Func<IDbConnection>, 
            string,
            string,
            string,
            Try<Guid>> BeginConversationFactory() =>
            (log, connectionFactory, fromService, toService, contract) =>
                CommandFactory<Guid>()(
                    log,
                    connectionFactory,
                    SetupBeginConversation().Par(log, fromService, toService, contract),
                    cmd => RunBeginCommandFactory()
                        .Par(new ExecuteNonQueryAsync(c => c.ExecuteNonQuery().AsTask()))(cmd)
                        .Result);

        /// <summary>
        /// Send a <see cref="BrokerMessage"/>.
        /// </summary>
        /// <returns></returns>
        public static Func<Maybe<Log>, Func<IDbConnection>, BrokerMessage, Try<UnitValue>> SendFactory() =>
            (log, connectionFactory, message) =>
                CommandFactory<UnitValue>()(
                    log,
                    connectionFactory,
                    SetupSend().Par(log, message.AssertValue()),
                    cmd => Unit.Tap(_ => cmd.ExecuteNonQuery()));

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
            EndDialogAsync,
            IEnumerable<IBrokerObserver>,
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

                                    switch (message.MessageType)
                                    {
                                        case ServiceBrokerErrorMessageType:
                                            result = logger.Debug(observers, "Calling Observers OnError")
                                                .IterAsync(obs => 
                                                    obs.SendError(new Exception(message.Message), message, logger), cancelToken, scheduler);
                                            break;
                                        case ServiceBrokerEndDialogMessageType:
                                            result = logger.Debug(Unit, "Received EndDialog message").AsTask();
                                            break;
                                        default:
                                            result = logger.Debug(observers, "Calling Observers OnNext")
                                                .IterAsync(obs => obs.SendNext(message, logger), cancelToken, scheduler);
                                            break;
                                    }

                                    return await result;
                                }),
                            "Ending Dialog")),
                    _ => TryAsync(() => Unit.AsTask()));

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
            IEnumerable<IBrokerObserver>,
            BrokerMessage,
            Try<UnitValue>> ProcessMessageFactory() =>
            (logger, scheduler, cancelToken, endDialog, observers, message) =>
                Match(
                    message == BrokerMessage.Empty,
                    NotEqual(true, _ =>
                        logger.Debug(
                            endDialog(message.Conversation).Bind(__ =>
                            {
                                var result = UnitValue.Default;

                                switch (message.MessageType)
                                {
                                    case ServiceBrokerErrorMessageType:
                                        result = logger.Debug(observers, "Calling Observers OnError")
                                            .Concurrently(obs => obs.SendError(new Exception(message.Message), message, logger), cancelToken, scheduler);
                                        break;
                                    case ServiceBrokerEndDialogMessageType:
                                        result = logger.Debug(Unit, "Received EndDialog message");
                                        break;
                                    default:
                                        result = logger.Debug(observers, "Calling Observers OnNext")
                                            .Concurrently(obs => obs.SendNext(message, logger), cancelToken, scheduler);
                                        break;
                                }

                                return result;
                            }),
                            "Ending Dialog")),
                    _ => Try(() => Unit));

        /// <summary>
        /// Adds error handling to IObserver.OnNext
        /// </summary>
        /// <param name="self"></param>
        /// <param name="next"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public static UnitValue SendNext(this IBrokerObserver self, BrokerMessage next, Maybe<Log> logger) =>
            Try(() => self.OnNext(next)).Match(_ => _, e => self.SendError(e, next, logger));

        /// <summary>
        /// Adds error handling to IObserver.OnError
        /// </summary>
        /// <param name="self"></param>
        /// <param name="exception"></param>
        /// <param name="message"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public static UnitValue SendError(this IBrokerObserver self, Exception exception, Maybe<BrokerMessage> message, Maybe<Log> logger) =>
            Try(() => self.OnError(exception, message))
                .Match(_ => _, 
                    e => logger.Error<UnitValue>(new AggregateException(exception, e).GetExceptionChainMessagesWithSql()));

        static UnitValue Concurrently<T>(this IEnumerable<T> self, Action<T> action, CancellationToken token, TaskScheduler scheduler) =>
            Unit.Tap(_ => Task.WaitAll(
                self.Aggregate(
                    Empty<Task>(),
                    (state, next) => 
                        Task.Factory.StartNew(
                            () => action(next),
                            token,
                            TaskCreationOptions.None,
                            scheduler)
                        .Cons(state))
                .ToArray()));
    }
}