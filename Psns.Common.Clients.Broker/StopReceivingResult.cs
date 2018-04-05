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
        readonly ImmutableList<Exception> _exceptions;

        /// <summary>
        /// If any Exceptions occurred.
        /// </summary>
        public bool Failed => _exceptions?.Count > 0 
            && _exceptions.Any(e => e.GetType() != typeof(TaskCanceledException));

        /// <summary>
        /// Contains all Exceptions captured.
        /// </summary>
        public AggregateException Exceptions =>
            new AggregateException(_exceptions.Where(e => e.GetType() != typeof(TaskCanceledException)));

        public StopReceivingResult(Try<UnitValue> attempt)
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
            Map((_exceptions ?? ImmutableList.Create<Exception>()), exceptions =>
                new StopReceivingResult(Possible(result._exceptions)
                    .Match(
                        some: rExceptions => exceptions.AddRange(rExceptions),
                        none: () => exceptions)));
    }

    public static partial class AppPrelude
    {
        public static async Task<StopReceivingResult> Append(this StopReceivingResult self, TryAsync<UnitValue> next) =>
            self.Append(await next.FromAsync());

        public static async Task<StopReceivingResult> Append(this Task<StopReceivingResult> self, Try<UnitValue> next) =>
            (await self).Append(new StopReceivingResult(next));

        public static async Task<StopReceivingResult> Append(this Task<StopReceivingResult> self, StopReceivingResult next) =>
            (await self).Append(next);

        public static async Task<StopReceivingResult> FromAsync(this TryAsync<UnitValue> attempt) =>
            await attempt.Match(
                res => new StopReceivingResult(() => res),
                e => new StopReceivingResult(ImmutableList.Create(e)));
    }
}
