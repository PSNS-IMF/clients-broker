using Psns.Common.Functional;
using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using static Psns.Common.Functional.Prelude;

namespace Psns.Common.Clients.Broker
{
    /// <summary>
    /// Captures any exceptions from the call chain in context.
    /// </summary>
    public struct StopReceivingResult
    {
        readonly Maybe<ImmutableList<Exception>> _exceptions;

        /// <summary>
        /// If any Exceptions occurred.
        /// </summary>
        public bool Failed =>
            _exceptions.Match(
                exs => exs.Count > 0 && exs.Any(e => !IsCancellation(e)),
                () => false);

        /// <summary>
        /// Contains all Exceptions captured.
        /// </summary>
        public AggregateException Exceptions =>
            _exceptions.Match(
                exs => new AggregateException(exs.Where(e => !IsCancellation(e))),
                () => new AggregateException());

        public StopReceivingResult(Either<Exception, UnitValue> attempt)
        {
            _exceptions = attempt.Match(
                _ => ImmutableList.Create<Exception>(),
                exception => ImmutableList.Create(exception));
        }

        public StopReceivingResult(ImmutableList<Exception> exceptions)
        {
            _exceptions = exceptions;
        }

        public StopReceivingResult Append(StopReceivingResult result) =>
            _exceptions.Match(exceptions =>
                new StopReceivingResult(
                    result._exceptions
                        .Match(
                            some: rExceptions => exceptions.AddRange(rExceptions),
                            none: () => exceptions)),
                () => new StopReceivingResult());

        static bool IsCancellation(Exception exception) =>
            Map(
                Cons(typeof(TaskCanceledException), typeof(OperationCanceledException)),
                types => types.Contains(exception.GetType()) || HasCancellation(exception));

        static bool HasCancellation(Exception exception) =>
            exception is AggregateException
                && IsCancellation((exception as AggregateException).InnerException);
    }

    public static partial class AppPrelude
    {
        public static StopReceivingResult Append(this StopReceivingResult self, Either<Exception, UnitValue> next) =>
            self.Append(new StopReceivingResult(next));
    }
}
