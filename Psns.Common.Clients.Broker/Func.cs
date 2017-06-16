using System;
using System.Diagnostics.Contracts;
using System.Threading.Tasks;
using LanguageExt;
using static LanguageExt.Prelude;

namespace Psns.Common.Clients.Broker
{
    public static partial class AppPrelude
    {
        [Pure]
        public async static Task<R2> matchAsync<T, L, R2>(Either<L, T> self, Func<T, Task<R2>> right, Func<L, R2> left) =>
            await self.MatchAsync(right, left);

        [Pure]
        public async static Task<R2> matchAsync<T, L, R2>(Task<Either<L, T>> self, Func<T, R2> right, Func<L, R2> left)
        {
            var either = await self;

            return match(
                either,
                Right: val => right(val),
                Left: err => left(err));
        }

        /// <summary>
        /// Runs a function within a try/catch block
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fun"></param>
        /// <returns>A string representation of the exception on failure</returns>
        [Pure]
        public static Either<Exception, T> safe<T>(Func<T> fun)
        {
            try
            {
                return fun();
            }
            catch(Exception e)
            {
                return e;
            }
        }

        [Pure]
        public async static Task<Either<Exception, T>> safeAsync<T>(Func<Task<T>> f)
        {
            try
            {
                return await f();
            }
            catch(Exception e)
            {
                return await Task.FromResult(e);
            }
        }

        [Pure]
        public static Func<T, Option<R>> always<T, R>(T value, Action<T> map) =>
            (T input) => { map(input); return None; };

        [Pure]
        public static Either<L, R> Join<L, R>(this Either<L, R> self, Either<L, R> other) =>
            self.Join(other, o => o, i => i, (o, i) => i);

        [Pure]
        public static Either<L, R> Join<L, R, U>(this Either<L, R> self, Either<U, R> other, Func<U, L> project) =>
            self.Join(
                match(
                    other,
                    Right: r => Right<L, R>(r),
                    Left: u => project(u)));

        [Pure]
        public static Unit dispose(IDisposable d)
        {
            d.Dispose();
            return Unit.Default;
        }
    }
}