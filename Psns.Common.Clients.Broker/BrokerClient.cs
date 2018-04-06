﻿using Psns.Common.Functional;
using Psns.Common.SystemExtensions.Diagnostics;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static Psns.Common.Clients.Broker.AppPrelude;
using static Psns.Common.Functional.Prelude;
using Subscriber = System.IObserver<Psns.Common.Clients.Broker.BrokerMessage>;

namespace Psns.Common.Clients.Broker
{
    /// <summary>
    /// A function that asynchronously opens a DB connection.
    /// </summary>
    /// <param name="connection">The connection to open</param>
    /// <returns>The opened connection</returns>
    public delegate Task<IDbConnection> OpenAsync(IDbConnection connection);

    /// <summary>
    /// A function that asynchronously executes a DB command.
    /// </summary>
    /// <param name="command">The target command</param>
    /// <returns>Affected rows</returns>
    public delegate Task<int> ExecuteNonQueryAsync(IDbCommand command);

    /// <summary>
    /// Defines an <see cref="IObservable{BrokerMessage}" /> derivative.
    /// </summary>
    public interface IBrokerClient : IObservable<BrokerMessage>
    {
        /// <summary>
        /// Defines a method that starts receiving messages from a Service Broker queue.
        /// </summary>
        /// <param name="queueName"></param>
        /// <returns><see cref="IRunningBrokerClient"/></returns>
        IRunningBrokerClient ReceiveMessages(string queueName);

        /// <summary>
        /// Defines a list of subscribers for incoming messages.
        /// </summary>
        IList<Subscriber> Subscribers { get; }
    }

    /// <summary>
    /// Provides SQL Service Broker operations.
    /// </summary>
    public class BrokerClient : IBrokerClient
    {
        #region private properties
        readonly Maybe<Log> _logger;

        // Observers that will receive messages from a SB queue.
        readonly ConcurrentBag<Subscriber> _observers;

        // DB initialization
        readonly Func<IDbConnection> _connectionFactory;
        readonly OpenAsync _openAsync;
        readonly ExecuteNonQueryAsync _executeNonQueryAsync;
        readonly GetMessage _getMessage;
        readonly Func<CancellationToken, ProcessMessageAsync> _composeProcessMessage;

        // worker thread handling
        readonly TaskScheduler _scheduler;
        CancellationTokenSource _tokenSource;
        Maybe<CancellationToken> _cancelToken;
        Maybe<Task> _receiver;
        #endregion

        #region constructors
        /// <summary>
        /// Creates a new client.
        /// </summary>
        /// <param name="connectionFactory">A function that creates a new DB connection</param>
        /// <param name="openAsync">A function that asynchronously opens the DB connection</param>
        /// <param name="executeNonQueryAsync">A function that asynchronously executes a DB command</param>
        /// <exception cref="System.ArgumentNullException"></exception>
        public BrokerClient(
            Func<IDbConnection> connectionFactory,
            OpenAsync openAsync,
            ExecuteNonQueryAsync executeNonQueryAsync)
                : this(connectionFactory, openAsync, executeNonQueryAsync, None, None) { }

        /// <summary>
        /// Creates a new client.
        /// </summary>
        /// <param name="connectionFactory">A function that creates a new DB connection</param>
        /// <param name="openAsync">A function that asynchronously opens the DB connection</param>
        /// <param name="executeNonQueryAsync">A function that asynchronously executes a DB command</param>
        /// <param name="logger">A function that writes logging messages</param>
        /// <exception cref="System.ArgumentNullException"></exception>
        public BrokerClient(
            Func<IDbConnection> connectionFactory,
            OpenAsync openAsync,
            ExecuteNonQueryAsync executeNonQueryAsync,
            Log logger)
                : this(connectionFactory, openAsync, executeNonQueryAsync, logger, None) { }

        /// <summary>
        /// Creates a new client.
        /// </summary>
        /// <param name="connectionFactory">A function that creates a new DB connection</param>
        /// <param name="openAsync">A function that asynchronously opens the DB connection</param>
        /// <param name="executeNonQueryAsync">A function that asynchronously executes a DB command</param>
        /// <param name="scheduler"></param>
        /// <exception cref="System.ArgumentNullException"></exception>
        public BrokerClient(
            Func<IDbConnection> connectionFactory,
            OpenAsync openAsync,
            ExecuteNonQueryAsync executeNonQueryAsync,
            TaskScheduler scheduler)
                : this(connectionFactory, openAsync, executeNonQueryAsync, None, scheduler) { }

        /// <summary>
        /// Creates a new client.
        /// </summary>
        /// <param name="connectionFactory">A function that creates a new DB connection</param>
        /// <param name="openAsync">A function that asynchronously opens the DB connection</param>
        /// <param name="executeNonQueryAsync">A function that asynchronously executes a DB command</param>
        /// <param name="logger">A function that writes logging messages</param>
        /// <param name="scheduler"></param>
        /// <exception cref="System.ArgumentNullException"></exception>
        public BrokerClient(
            Func<IDbConnection> connectionFactory, 
            OpenAsync openAsync,
            ExecuteNonQueryAsync executeNonQueryAsync,
            Maybe<Log> logger,
            Maybe<TaskScheduler> scheduler)
        {
            _observers = new ConcurrentBag<Subscriber>();
            _connectionFactory = connectionFactory.AssertValue();
            _openAsync = openAsync.AssertValue();
            _executeNonQueryAsync = executeNonQueryAsync.AssertValue();
            _logger = logger;
            _scheduler = scheduler | TaskScheduler.Current;

            _getMessage = new GetMessage(GetMessageFactory().Par(
                _logger,
                _connectionFactory,
                _openAsync, 
                _executeNonQueryAsync));

            var endDialog = new EndDialog(EndDialogFactory().Par(
                _logger,
                _connectionFactory,
                _openAsync,
                _executeNonQueryAsync));

            _composeProcessMessage = fun((CancellationToken token) => 
                new ProcessMessageAsync(() => 
                    ProcessMessageAsyncFactory().Par(
                        _logger,
                        _scheduler,
                        token,
                        endDialog)));
        }
        #endregion

        /// <summary>
        /// Adds a new observer to receive future queue messages
        /// </summary>
        /// <remarks>Be aware that <see cref="IObserver{T}.OnNext(T)"/>, <see cref="IObserver{T}.OnError(Exception)"/>,
        /// and <see cref="IObserver{T}.OnCompleted()"/> are called asynchronously; 
        /// so appropriate state sharing precautions should be taken to avoid race conditions.</remarks>
        /// <param name="observer">New addition</param>
        /// <exception cref="System.ArgumentNullException"></exception>
        /// <returns>A subscription that can be unsubscribed from by calling Dispose</returns>
        public IDisposable Subscribe(Subscriber observer)
        {
            if (!_observers.Contains(observer.AssertValue()))
            {
                _observers.Add(observer);

                _logger.Debug($"Added {observer}");
            }

            return new Subscription(_observers, observer, _logger);
        }

        /// <summary>
        /// Begin receiving messages.
        /// </summary>
        /// <param name="queueName">The queue to listen for messages on</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentNullException"></exception>
        public IRunningBrokerClient ReceiveMessages(string queueName)
        {
            _tokenSource = new CancellationTokenSource();
            var cancelToken = _tokenSource.Token;
            _cancelToken = cancelToken;

            _receiver = Task.Run(() =>
            {
                _logger.Debug("Receiver started");

                while (!_tokenSource.IsCancellationRequested)
                {
                    _getMessage(queueName).Match(
                        success: message => 
                            QueueForProcessing(message, cancelToken),
                        fail: exception => 
                            _observers.Iter(obs => 
                                obs.SendError(exception, _logger),
                                cancelToken,
                                _scheduler));
                }

                _logger.Debug("Receiver cancelling");

                _cancelToken.IfSome(token => token.ThrowIfCancellationRequested());
            },
            cancelToken);

            _logger.Debug("Starting Receiver");

            return new RunningBrokerClient(_logger, _observers, _receiver, _tokenSource);
        }

        Task<UnitValue> QueueForProcessing(BrokerMessage message, CancellationToken token) =>
            Task.Factory.StartNew(() => // await
            _composeProcessMessage(token)().Par(_observers)(message) // await
                .Match( // await
                    success: _ => 
                        Unit,
                    fail: exception => 
                        _observers.Iter(obs =>
                            obs.SendError(exception, _logger),
                            token,
                            _scheduler).Result).Result,
                token,
                TaskCreationOptions.AttachedToParent,
                _scheduler);

        /// <summary>
        /// A list of the current Subscribers
        /// </summary>
        public IList<Subscriber> Subscribers =>
            _observers.ToList();

        public override string ToString() =>
            $@"Observers: {_observers.Count}, WorkerStatus: {
                _receiver.Match(task => task.Status.ToString(), () => None.ToString())}, CanBeCanceled: {
        _cancelToken.Match(token => token.CanBeCanceled.ToString(), () => None.ToString())}";
    }

    /// <summary>
    /// Returned to the caller on Subscription.
    /// 
    /// When disposed of, removes the subscriber from the BrokerClient's list of subscribers.
    /// </summary>
    class Subscription : IDisposable
    {
        readonly IProducerConsumerCollection<Subscriber> _observers;
        readonly Subscriber _observer;
        readonly Maybe<Log> _logger;

        internal Subscription(IProducerConsumerCollection<Subscriber> observers, Subscriber observer, Maybe<Log> logger)
        {
            _observers = observers.AssertValue();
            _observer = observer.AssertValue();
            _logger = logger;
        }

        public override string ToString() =>
            $"Disposed: {disposedValue}";

        #region IDisposable Support
        bool disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing && _observer != null && _observers != null && _observers.Contains(_observer))
                {
                    Subscriber removing;
                    if(_observers.TryTake(out removing))
                    {
                        _logger.Debug($"Removed Observer: {ToString()}");
                    }
                    else
                    {
                        _logger.Error($"Failed to remove Observer: {ToString()}");
                    }
                }

                disposedValue = true;
            }
        }

        public void Dispose() => Dispose(true);
        #endregion
    }
}