using System;
using CSharpFunctionalExtensions;
using Psns.Common.SystemExtensions;

namespace Psns.Common.Clients.Broker
{
    public static class ResultExtensions
    {
        public static Result<T> Merge<T>(this Result @this, Result<T> result)
        {
            return result.IsFailure
                ? result
                : @this.IsFailure
                    ? Result.Fail<T>(@this.Error)
                    : Result.Ok<T>(result.Value);
        }

        public static Result<R> Convert<T, R>(this Result<T> @this, R value)
        {
            return @this.IsFailure
                ? Result.Fail<R>(@this.Error)
                : Result.Ok(value);
        }
    }

    internal static class Functional
    {
        public static Result Try(Action action)
        {
            try
            {
                action();

                return Result.Ok();
            }
            catch(Exception e)
            {
                return Result.Fail(e.GetExceptionChainMessages());
            }
        }

        public static Result<T> Try<T>(Func<T> func)
        {
            try
            {
                return Result.Ok(func());
            }
            catch(Exception e)
            {
                return Result.Fail<T>(e.GetExceptionChainMessages());
            }
        }
    }
}