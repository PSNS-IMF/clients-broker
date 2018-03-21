using Psns.Common.Functional;
using Psns.Common.SystemExtensions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static Psns.Common.Clients.Broker.AppPrelude;
using static Psns.Common.Functional.Prelude;
using Subscriber = System.IObserver<Psns.Common.Clients.Broker.BrokerMessage>;

namespace Psns.Common.Clients.Broker
{
    /// <summary>
    /// A function that writes a log message.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="eventType">The type of event to log</param>
    public delegate void Log(string message, TraceEventType eventType);
    
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
    /// Provides SQL Service Broker operations.
    /// </summary>
    public class BrokerClient : IObservable<BrokerMessage>
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
        readonly ProcessMessageAsync _processMessage;

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

            _processMessage = new ProcessMessageAsync(() => 
                ProcessMessageAsyncFactory().Par(
                    _logger, 
                    _cancelToken,
                    endDialog));
        }
        #endregion

        /// <summary>
        /// Adds a new observer to receive future queue messages.
        /// </summary>
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
        public RunningBrokerClient ReceiveMessages(string queueName)
        {
            _tokenSource = new CancellationTokenSource();
            _cancelToken = _tokenSource.Token;

            _receiver = Task.Run(async () =>
            {
                _logger.Debug("Receiver started");

                while (!_tokenSource.IsCancellationRequested)
                {
                    // have to await twice because 2 tasks are created in this call chain
                    await await _getMessage(queueName)
                        // inside await
                        .Bind(_processMessage().Par(_observers))
                        .Match(
                            success: async _ => await Task.FromResult(Unit),
                            fail: async exception =>
                                // outside await
                                await _observers.IterAsync(observer =>
                                    observer.SendError(
                                        _logger.Debug(
                                            exception, 
                                            $"Calling Observers OnError for: {exception.GetExceptionChainMessagesWithSql()}"),
                                        _logger), 
                                _cancelToken));
                }

                _logger.Debug("Receiver cancelling");

                _cancelToken.IfSome(token => token.ThrowIfCancellationRequested());
            },
            _cancelToken);

            _logger.Debug("Starting Receiver");

            return new RunningBrokerClient(_logger, _observers, _receiver, _tokenSource);
        }

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