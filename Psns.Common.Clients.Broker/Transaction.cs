using System;
using System.Data;
using LanguageExt;
using static Psns.Common.Clients.Broker.AppPrelude;

namespace Psns.Common.Clients.Broker
{
    /// <summary>
    /// A Transaction in progress
    /// </summary>
    public class Transaction
    {
        readonly IDbTransaction _transaction;

        internal Transaction(IDbTransaction transaction)
        {
            _transaction = transaction;
        }

        internal Either<Exception, IDbCommand> CreateCommand() =>
            safe(() =>
            {
                var command = _transaction.Connection.CreateCommand();
                command.Transaction = _transaction;
                return command;
            });

        public Either<Exception, Unit> Commit() =>
            safe(() =>
            {
                _transaction.Commit();
                return Unit.Default;
            });
    }
}