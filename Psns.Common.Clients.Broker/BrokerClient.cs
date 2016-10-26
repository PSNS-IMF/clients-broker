using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using LanguageExt;
using static Psns.Common.Clients.Broker.AppPrelude;
using static LanguageExt.Prelude;
using static LanguageExt.List;

namespace Psns.Common.Clients.Broker
{
    public static class BrokerClient
    {
        public static Either<string, IDbConnection> Open(Func<IDbConnection> connectionFactory) =>
            Safe(() =>
            {
                var conn = connectionFactory();
                conn.Open();
                return conn;
            });

        public static Either<string, IDbTransaction> BeginTransaction(Either<string, IDbConnection> connection) =>
            from conn in connection
            from tran in Safe(conn.BeginTransaction)
            select tran;

        public static Either<string, IDbCommand> CreateCommand(Either<string, IDbConnection> connection,
            Either<string, IDbTransaction> transaction) =>
            from conn in connection
            from tran in transaction
            from command in Safe(() =>
            {
                var command = conn.CreateCommand();
                command.Transaction = tran;
                return command;
            })
            select command;

        public static async Task<Either<string, Unit>> SendAsync(
            Func<Task<int>> executeQuery,
            Either<string, IDbCommand> command,
            BrokerMessage message) =>
            await command.MatchAsync(
                async cmd =>
                {
                    var parameters = new[]
                    {
                        new SqlParameter("@message", SqlDbType.NVarChar, message.Message.Length),
                        new SqlParameter("@conversation", SqlDbType.UniqueIdentifier)
                    };

                    iter(parameters, parameter => cmd.Parameters.Add(parameter));

                    cmd.CommandText = "SEND ON CONVERSATION @conversation MESSAGE TYPE [" + message.MessageType + "] (@message)";

                    await executeQuery();

                    return Right<string, Unit>(Unit.Default);
                },
                error => error);

        public static async Task<Either<string, BrokerMessage>> ReceiveAsync(
            Func<CancellationToken, Task<int>> executeQuery,
            Either<string, IDbCommand> command,
            string queueName,
            CancellationToken token) =>
            await command.MatchAsync(
                async cmd =>
                {
                    var parameters = new[]
                    {
                        new SqlParameter("@messageType", SqlDbType.NVarChar, 256),
                        new SqlParameter("@message", SqlDbType.NVarChar, 4000),
                        new SqlParameter("@conversationGroup", SqlDbType.UniqueIdentifier),
                        new SqlParameter("@conversation", SqlDbType.UniqueIdentifier)
                    };

                    iter(parameters, parameter =>
                    {
                        parameter.Direction = ParameterDirection.Output;
                        cmd.Parameters.Add(parameter);
                    });

                    cmd.CommandText = "WAITFOR (RECEIVE TOP(1) " +
                        "@messageType = message_type_name, " +
                        "@message = message_body, " +
                        "@conversationGroup = conversation_group_id, " +
                        "@conversation = conversation_handle " +
                        $"FROM [{queueName}]), TIMEOUT 5000;";

                    await executeQuery(token);

                    if(!(parameters[0].Value is DBNull))
                    {
                        return Right<string, BrokerMessage>(
                            new BrokerMessage(
                                (string)parameters[0].Value,
                                (string)parameters[1].Value,
                                (Guid)parameters[2].Value,
                                (Guid)parameters[3].Value));
                    }
                    else
                        return BrokerMessage.Empty;
                },
                error => error);

        public static async Task<Either<string, Unit>> EndDialogAsync(
            Func<Task<int>> executeQuery,
            Either<string, IDbCommand> command,
            Guid conversation) =>
            await command.MatchAsync(
                async cmd =>
                {
                    var parameters = new[]
                    {
                        new SqlParameter("@conversation", SqlDbType.UniqueIdentifier)
                    };

                    iter(parameters, parameter => cmd.Parameters.Add(parameter));

                    cmd.CommandText = "END CONVERSATION @conversation";

                    await executeQuery();
                    return Right<string, Unit>(Unit.Default);
                },
                error => error);

        public static Either<string, Unit> Commit(Either<string, IDbTransaction> transaction) =>
            from trans in transaction
            from unit in Safe(() => { trans.Commit(); return Unit.Default; })
            select unit;
    }
}