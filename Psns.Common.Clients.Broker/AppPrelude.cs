using System;
using System.Data;
using System.Threading.Tasks;
using System.Diagnostics.Contracts;
using LanguageExt;

namespace Psns.Common.Clients.Broker
{
    /// <summary>
    /// Convenience functions
    /// </summary>
    public static class AppPrelude
    {
        [Pure]
        public static BrokerMessage brokerMessage(string messageType, string message, Guid conversationGroup, Guid conversation) =>
            new BrokerMessage(messageType, message, conversationGroup, conversation);

        [Pure]
        public static Either<Exception, OpenConnection> openConnection(IDbConnection connection) =>
            safe(() =>
            {
                connection.Open();
                return new OpenConnection(connection);
            });

        [Pure]
        public static Either<Exception, Transaction> beginTransaction(Either<Exception, OpenConnection> connection) =>
            from conn in connection
            from trans in conn.BeginTransaction()
            select new Transaction(trans);

        [Pure]
        public static Either<Exception, Command> createCommand(Either<Exception, Transaction> transaction) =>
            from trans in transaction
            select new Command(trans.CreateCommand);

        [Pure]
        public async static Task<R2> matchAsync<T, L, R2>(Either<L, T> self, Func<T, Task<R2>> right, Func<L, R2> left) =>
            await self.MatchAsync(right, left);

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
        public static Unit dispose(IDisposable d)
        {
            d.Dispose();
            return Unit.Default;
        }
    }
}