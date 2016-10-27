using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using LanguageExt;

namespace Psns.Common.Clients.Broker
{
    public delegate Task<Either<string, BrokerMessage>> ReceiveFunc(string queueName, CancellationToken token);
    public delegate Task<Either<string, Unit>> SendFunc(BrokerMessage message);
    public delegate Task<Either<string, Unit>> EndDialogFunc(Guid conversation);
    public delegate Either<string, Unit> CommitFunc();

    public static class CurriedDelegates
    {
        public static Func<IDbConnection, Either<string, IDbConnection>> open = BrokerClient.Open;

        public static Func<Either<string, IDbConnection>, Either<string, IDbTransaction>> beginTransaction = BrokerClient.BeginTransaction;

        public static Func<Either<string, IDbTransaction>, Either<string, IDbCommand>> createCommand = BrokerClient.CreateCommand;

        public static Func<Either<string, IDbCommand>, Func<IDbCommand, Task<int>>, Func<BrokerMessage, Task<Either<string, Unit>>>> sendAsync =
            (cmd, query) =>
                msg => BrokerClient.SendAsync(cmd, query, msg);

        public static Func<Either<string, IDbCommand>, Func<IDbCommand, CancellationToken, Task<int>>, Func<string, CancellationToken, Task<Either<string, BrokerMessage>>>> receiveAsync =
            (cmd, query) =>
            (queue, token) =>
                BrokerClient.ReceiveAsync(cmd, query, queue, token);

        public static Func<Either<string, IDbCommand>, Func<IDbCommand, Task<int>>, Func<Guid, Task<Either<string, Unit>>>> endAsync =
            (cmd, query) =>
            con => BrokerClient.EndDialogAsync(cmd, query, con);

        public static Func<Either<string, IDbTransaction>, Func<Either<string, Unit>>> commit =
            trans => () => BrokerClient.Commit(trans);
    }
}