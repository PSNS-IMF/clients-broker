using Psns.Common.Functional;
using Psns.Common.SystemExtensions.Diagnostics;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static Psns.Common.Functional.Prelude;

namespace Psns.Common.Clients.Broker
{
    /// <summary>
    /// Defines a BrokerClient that has started receiving messages.
    /// </summary>
    public interface IRunningBrokerClient
    {
        /// <summary>
        /// Defines a method that stops receiving all messages.
        /// </summary>
        /// <returns></returns>
        StopReceivingResult StopReceiving();
    }

    /// <summary>
    /// Represents the state of a BrokerClient after it has begun receiving messages.
    /// </summary>
    public class RunningBrokerClient : IRunningBrokerClient
    {
        readonly Maybe<Log> _logger;
        readonly Maybe<IProducerConsumerCollection<IObserver<BrokerMessage>>> _observers;
        readonly Maybe<Task> _worker;
        readonly Maybe<CancellationTokenSource> _tokenSource;

        internal RunningBrokerClient(
            Maybe<Log> logger,
            Maybe<IProducerConsumerCollection<IObserver<BrokerMessage>>> observers,
            Maybe<Task> worker,
            CancellationTokenSource tokenSource)
        {
            _logger = logger;
            _observers = observers;
            _worker = worker;
            _tokenSource = tokenSource;
        }

        /// <summary>
        /// Calls OnCompleted on all Subscribers, unsubscribes them,
        ///     and stops receiving messages on the queue.
        /// </summary>
        /// <exception cref="System.InvalidOperationException"></exception>
        /// <returns>A result containing any Exceptions from removing Subscribers</returns>
        public StopReceivingResult StopReceiving()
        {
            if (_observers.IsNone || _worker.IsNone || _tokenSource.IsNone)
            {
                throw new InvalidOperationException($"{nameof(BrokerClient)} has not started receiving");
            }

            var tokenSource = _tokenSource | CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
            var worker = _worker | Task.Delay(0);
            var observers = _observers.Match(s => s, () => new ConcurrentBag<IObserver<BrokerMessage>>());

            if (worker.IsCompleted)
            {
                throw new InvalidOperationException($"{nameof(BrokerClient)} has already stopped receiving");
            }

            return new StopReceivingResult(Try(() => tokenSource.Cancel()))
                .Append(Try(() => { worker.Wait(); return Unit; }))
                .Append(Try(() => { worker.Dispose(); _logger.Debug("Receiver stopped"); }))
                .Append(Try(() => tokenSource.Dispose()))
                .Append(
                    _logger.Debug(observers, "Calling Observers OnCompleted").Aggregate(
                        new StopReceivingResult(),
                        (prev, next) =>
                            prev.Append(new StopReceivingResult(Try(() => next.OnCompleted())))))
                .Append(
                    new StopReceivingResult(Try(() =>
                    {
                        IObserver<BrokerMessage> removing;
                        while (observers.TryTake(out removing)) { };
                        _logger.Debug("All Observers Removed");
                    })));
        }
    }
}