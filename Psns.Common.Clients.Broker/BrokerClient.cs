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
        delegate ProcessMessageAsync ComposeProcessMessageAsync(CancellationToken token, IEnumerable<Subscriber> observers);
        delegate ProcessMessage ComposeProcessMessage(CancellationToken token, IEnumerable<Subscriber> observers);

        readonly Maybe<Log> _logger;

        // Observers that will receive messages from a SB queue.
        readonly ConcurrentBag<Subscriber> _observers;

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
        /// <param name="openAsync">A function that asynchronously opens the DB connection</param>
        /// <param name="executeNonQueryAsync">A function that asynchronously executes a DB command</param>
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
            _observers = new ConcurrentBag<Subscriber>();
            _connectionFactory = connectionFactory.AssertValue();
            _openAsync = openAsync;
            _executeNonQueryAsync = executeNonQueryAsync;
            _logger = logger;
            _scheduler = scheduler | TaskScheduler.Current;

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
                new ComposeProcessMessageAsync(fun((CancellationToken token, IEnumerable<Subscriber> observers) => 
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
                new ComposeProcessMessage(fun((CancellationToken token, IEnumerable<Subscriber> observers) =>
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
                    var result = _getMessageAsync.Match(
                        some: get => get(queueName).Match(
                            success: msg => QueueForProcessing(_logger.Debug(msg, "getMessageAsync success"), cancelToken), 
                            fail: exception => _logger.Debug(ProcessException(exception, cancelToken), $"getMessageAsync fail with: {exception.GetExceptionChainMessagesWithSql()}")).Result,
                        none: () => _getMessage.Match(
                            some: get => get(queueName).Match(
                                success: msg => QueueForProcessing(_logger.Debug(msg, "getMessage success"), cancelToken),
                                fail: exception => _logger.Debug(ProcessException(exception, cancelToken), $"getMessageAsync fail with: {exception.GetExceptionChainMessagesWithSql()}")),
                            none: () => Unit.AsTask()))
                        .ContinueWith(task => Unit.Tap(_ => _logger.Debug("Message processing worker completed")));
                }

                _logger.Debug("Receiver cancelling");

                _cancelToken.IfSome(token => token.ThrowIfCancellationRequested());
            },
            cancelToken);

            _logger.Debug("Starting Receiver");

            return new RunningBrokerClient(_logger, _observers, _receiver, _tokenSource);
        }

        /// <summary>
        /// Execute tryGetMessage and wait for result
        ///     On success, process message
        ///     On fail, notify observers of failure
        ///
        /// On both success and fail, a child Task (AttachedToParent) is created
        /// so that more messages can be processed concurrently. Attaching to parent
        /// ensures that when parent is stopped, all children are also waited on
        /// and stopped.
        /// 
        /// </summary>
        /// <param name="tryGetMessage"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task<UnitValue> QueueForProcessing(BrokerMessage message, CancellationToken token) =>
            IsAsync
            ? _composeProcessMessageAsync.Match(compose => Map(compose(token, _observers), processMessage =>
                  Task.Factory.StartNew(() =>
                      processMessage(message)
                          .Match(
                              success: _ =>
                                  _logger.Debug(Unit, "processMessageAsync success"),
                              fail: exception =>
                                  _logger.Debug(
                                      ProcessException(exception, token).Result,
                                      $"processMessageAsync fail: {exception.GetExceptionChainMessagesWithSql()}")).Result,
                              token,
                              TaskCreationOptions.AttachedToParent,
                              _scheduler)),
                () => Unit.AsTask())
            : _composeProcessMessage.Match(compose => Map(compose(token, _observers), processMessage =>
                Task.Factory.StartNew(() =>
                    processMessage(message).Match(
                        success: _ => _logger.Debug(Unit, "processMessage success"),
                        fail: exception => _logger.Debug(
                            ProcessException(exception, token).Result, 
                            $"processMessage fail with: {exception.GetExceptionChainMessagesWithSql()}")),
                    token,
                    TaskCreationOptions.AttachedToParent,
                    _scheduler)),
                () => Unit.AsTask());

        Task<UnitValue> ProcessException(Exception exception, CancellationToken token) =>
            token.IsCancellationRequested
                ? Unit.Tap(_ => _logger.Error(exception.GetExceptionChainMessagesWithSql())).AsTask()
                : _observers.Iter(obs =>
                    obs.SendError(exception, _logger),
                    token,
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