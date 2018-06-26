using Psns.Common.Functional;
using Psns.Common.SystemExtensions;
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
    public interface IBrokerClient
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
        IList<IBrokerObserver> Subscribers { get; }

        /// <summary>
        /// Subscribe a new observer to receive Broker messages.
        /// </summary>
        /// <param name="observer"></param>
        /// <returns>An <see cref="IDisposable"/> that can be disposed of to unsubscribe</returns>
        IDisposable Subscribe(IBrokerObserver observer);

        /// <summary>
        /// Begin a Service Broker <c>Conversation</c>.
        /// </summary>
        /// <param name="from">Name of the originating <c>Service</c></param>
        /// <param name="to">Name of the destination <c>Service</c></param>
        /// <param name="contract">The name of the <c>Contract</c> to be used</param>
        /// <returns>A <c>Conversation</c> identifier to be used to send a <see cref="BrokerMessage"/></returns>
        Either<Exception, Guid> BeginConversation(string from, string to, string contract);

        /// <summary>
        /// Sends a <see cref="BrokerMessage"/>.
        /// </summary>
        /// <param name="message">The <see cref="BrokerMessage"/> to send</param>
        /// <returns></returns>
        Either<Exception, UnitValue> Send(BrokerMessage message);
    }

    /// <summary>
    /// Provides SQL Service Broker operations.
    /// </summary>
    public class BrokerClient : IBrokerClient
    {
        #region private properties
        delegate ProcessMessageAsync ComposeProcessMessageAsync(CancellationToken token, IEnumerable<IBrokerObserver> observers);
        delegate ProcessMessage ComposeProcessMessage(CancellationToken token, IEnumerable<IBrokerObserver> observers);

        readonly Maybe<Log> _logger;

        // Observers that will receive messages from a SB queue.
        readonly ConcurrentBag<IBrokerObserver> _observers;

        // DB initialization
        readonly Func<IDbConnection> _connectionFactory;
        readonly Maybe<OpenAsync> _openAsync;
        readonly Maybe<ExecuteNonQueryAsync> _executeNonQueryAsync;
        readonly Maybe<GetMessageAsync> _getMessageAsync;
        readonly Maybe<GetMessage> _getMessage;
        readonly Maybe<ComposeProcessMessageAsync> _composeProcessMessageAsync;
        readonly Maybe<ComposeProcessMessage> _composeProcessMessage;
        bool IsAsync => _openAsync.IsSome && _executeNonQueryAsync.IsSome;

        // worker thread handling
        readonly TaskScheduler _scheduler;
        CancellationTokenSource _tokenSource;
        Maybe<CancellationToken> _cancelToken;
        Maybe<Task> _receiver;
        ConcurrentQueue<Task> _workers;
        #endregion

        #region constructors

        /// <summary>
        /// Creates a new client.
        /// </summary>
        /// <param name="connectionFactory">A function that creates a new DB connection</param>
        /// <exception cref="System.ArgumentNullException"></exception>
        public BrokerClient(
            Func<IDbConnection> connectionFactory)
                : this(connectionFactory, None, None, None, None) { }

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
        /// <param name="logger">A function that writes logging messages</param>
        /// <exception cref="System.ArgumentNullException"></exception>
        public BrokerClient(
            Func<IDbConnection> connectionFactory,
            Log logger)
                : this(connectionFactory, None, None, logger, None) { }

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
        /// <param name="scheduler"></param>
        /// <exception cref="System.ArgumentNullException"></exception>
        public BrokerClient(
            Func<IDbConnection> connectionFactory,
            TaskScheduler scheduler)
                : this(connectionFactory, None, None, None, scheduler) { }

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
        /// <param name="logger"></param>
        /// <param name="scheduler"></param>
        /// <exception cref="System.ArgumentNullException"></exception>
        public BrokerClient(
            Func<IDbConnection> connectionFactory,
            Log logger,
            TaskScheduler scheduler)
                : this(connectionFactory, None, None, logger, scheduler) { }

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
            Maybe<OpenAsync> openAsync,
            Maybe<ExecuteNonQueryAsync> executeNonQueryAsync,
            Maybe<Log> logger,
            Maybe<TaskScheduler> scheduler)
        {
            _observers = new ConcurrentBag<IBrokerObserver>();
            _connectionFactory = connectionFactory.AssertValue();
            _openAsync = openAsync;
            _executeNonQueryAsync = executeNonQueryAsync;
            _logger = logger;
            _scheduler = scheduler | TaskScheduler.Current;
            _workers = new ConcurrentQueue<Task>();

            _getMessageAsync = _openAsync.Match(open =>
                _executeNonQueryAsync.Match(exec =>
                    new GetMessageAsync(GetMessageAsyncFactory().Par(
                        _logger,
                        _connectionFactory,
                        open, 
                        exec)),
                    () => Maybe<GetMessageAsync>.None),
                () => Maybe<GetMessageAsync>.None);

            _getMessage = IsAsync
                ? Maybe<GetMessage>.None
                : new GetMessage(GetMessageFactory()
                    .Par(_logger, _connectionFactory));

            var endDialogAsync = _openAsync.Match(open =>
                _executeNonQueryAsync.Match(exec =>
                    new EndDialogAsync(EndDialogAsyncFactory().Par(
                        _logger,
                        _connectionFactory,
                        open,
                        exec)),
                    () => Maybe<EndDialogAsync>.None),
                () => Maybe<EndDialogAsync>.None);

            _composeProcessMessageAsync = endDialogAsync.Match(end => 
                new ComposeProcessMessageAsync(fun((CancellationToken token, IEnumerable<IBrokerObserver> observers) => 
                    new ProcessMessageAsync(ProcessMessageAsyncFactory().Par(
                        _logger,
                        _scheduler,
                        token,
                        end,
                        observers)))),
                () => Maybe<ComposeProcessMessageAsync>.None);

            var endDialog = IsAsync
                ? Maybe<EndDialog>.None
                : new EndDialog(EndDialogFactory().Par(_logger, _connectionFactory));

            _composeProcessMessage = endDialog.Match(end =>
                new ComposeProcessMessage(fun((CancellationToken token, IEnumerable<IBrokerObserver> observers) =>
                    new ProcessMessage(ProcessMessageFactory().Par(
                        _logger,
                        _scheduler,
                        token,
                        end,
                        observers)))),
                () => Maybe<ComposeProcessMessage>.None);
        }
        
        #endregion

        /// <summary>
        /// Adds a new observer to receive future queue messages
        /// </summary>
        /// <remarks>Be aware that <see cref="IObserver{T}.OnNext(T)"/>, <see cref="IObserver{T}.OnError(Exception)"/>,
        /// and <see cref="IObserver{T}.OnCompleted()"/> are called asynchronously; 
        /// so appropriate state sharing precautions should be taken to avoid race conditions.</remarks>
        /// <param name="observer">New addition that needs to implement <see cref="IBrokerObserver"/></param>
        /// <exception cref="System.ArgumentNullException"></exception>
        /// <exception cref="System.InvalidOperationException">When <paramref name="observer"/> is not an <see cref="IBrokerObserver"/></exception>
        /// <returns>A subscription that can be unsubscribed from by calling Dispose</returns>
        public IDisposable Subscribe(IBrokerObserver observer)
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
                    _getMessageAsync.Match(
                        some: get => get(queueName).Match(
                            success: msg => QueueForProcessing(msg, cancelToken, _scheduler),
                            fail: exception => ProcessException(exception, None, cancelToken, _scheduler)).Result,
                        none: () => _getMessage.Match(
                            some: get => get(queueName).Match(
                                right: msg => QueueForProcessing(msg, cancelToken, _scheduler),
                                left: exception => ProcessException(exception, None, cancelToken, _scheduler)),
                            none: () => Unit));
                }

                _logger.Debug("Receiver cancelling");

                _cancelToken.IfSome(token => token.ThrowIfCancellationRequested());
            },
            cancelToken);

            _logger.Debug("Starting Receiver");

            _receiver.IfSome(t => _workers.Enqueue(t));

            return new RunningBrokerClient(_logger, _observers, _workers, _tokenSource);
        }

        /// <summary>
        /// Begin a <c>Conversation</c>.
        /// </summary>
        /// <param name="from"></param>
        /// <param name="to"></param>
        /// <param name="contract"></param>
        /// <returns></returns>
        public Either<Exception, Guid> BeginConversation(string from, string to, string contract) =>
            BeginConversationFactory()(_logger, _connectionFactory, from, to, contract);

        /// <summary>
        /// Send a <see cref="BrokerMessage"/>.
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public Either<Exception, UnitValue> Send(BrokerMessage message) =>
            SendFactory()(_logger, _connectionFactory, message);

        /// <summary>
        /// Execute tryGetMessage and wait for result
        ///     On success, process message
        ///     On fail, notify observers of failure
        /// 
        /// </summary>
        /// <param name="message"></param>
        /// <param name="token"></param>
        /// <param name="scheduler"></param>
        /// <returns></returns>
        UnitValue QueueForProcessing(BrokerMessage message, CancellationToken token, TaskScheduler scheduler) =>
            Unit.Tap(_ => _workers.Enqueue(
                Task.Factory.StartNew(() =>
                    IsAsync
                    ? _composeProcessMessageAsync.Match(
                        some: compose => 
                                Map(compose(token, _observers), processMessage =>
                                  processMessage(message)
                                      .Match(
                                          success: __ => Unit,
                                          fail: exception => ProcessException(exception, message, token, _scheduler)).Result),
                        none: () => Unit)
                    : _composeProcessMessage.Match(
                        some: compose =>
                                Map(compose(token, _observers), processMessage =>
                                    processMessage(message).Match(
                                        right: __ => Unit,
                                        left: exception => ProcessException(exception, message, token, _scheduler))),
                        none: () => Unit),
                        token, TaskCreationOptions.None, scheduler)
                .ContinueWith(TaskContinuation, token)));

        UnitValue ProcessException(Exception exception, Maybe<BrokerMessage> message, CancellationToken token, TaskScheduler scheduler) =>
            Unit.Tap(_ => _workers.Enqueue(
            Task.Factory.StartNew(() =>
                token.IsCancellationRequested
                    ? Unit.Tap(__ => _logger.Error(exception.GetExceptionChainMessagesWithSql()))
                    : _observers.Iter(obs =>
                        obs.SendError(exception, message, _logger)),
                token,
                TaskCreationOptions.None,
                scheduler)
            .ContinueWith(TaskContinuation, token)));

        void TaskContinuation(Task task)
        {
            _logger.Debug($"Task finished with Status: {task.Status}");

            Possible(task.Exception).IfSome(ex => _logger.Error(ex));

            Task removed;
            if (_workers.TryDequeue(out removed))
                _logger.Debug($"Successfully released 1 completed worker of {_workers.Count + 1}");
            else
                _logger.Error($"Unable to release completed worker");
        }

        /// <summary>
        /// A list of the current Subscribers
        /// </summary>
        public IList<IBrokerObserver> Subscribers =>
            _observers.ToList();

        /// <summary>
        /// A <see cref="string"/> representation of the <see cref="BrokerClient"/>.
        /// </summary>
        /// <returns></returns>
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
        readonly IProducerConsumerCollection<IBrokerObserver> _observers;
        readonly IBrokerObserver _observer;
        readonly Maybe<Log> _logger;

        internal Subscription(IProducerConsumerCollection<IBrokerObserver> observers, IBrokerObserver observer, Maybe<Log> logger)
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
                    IBrokerObserver removing;
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