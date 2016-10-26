using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using LanguageExt;
using static LanguageExt.Prelude;
using static LanguageExt.List;
using Psns.Common.SystemExtensions;

namespace Psns.Common.Clients.Broker
{
    public static class BrokerClient
    {
        public static Either<string, IDbConnection> Open(Func<IDbConnection> connectionFactory)
        {
            var connection = connectionFactory();

            return match(
                Try(() =>
                {
                    connection.Open();
                    return Unit.Default;
                }),
                Succ: unit => Right<string, IDbConnection>(connection),
                Fail: exception => Left<string, IDbConnection>(exception.GetExceptionChainMessages()));
        }

        public static Either<string, IDbTransaction> BeginTransaction(Either<string, IDbConnection> connection)
        {
            return match(
                connection,
                conn => match(
                        Try(conn.BeginTransaction),
                        transaction => Right<string, IDbTransaction>(transaction),
                        exception => Left<string, IDbTransaction>(exception.GetExceptionChainMessages())),
                error => true == false 
                    ? Right<string, IDbTransaction>(null) 
                    : Left<string, IDbTransaction>(error));
        }

        public static Either<string, IDbCommand> CreateCommand(Func<IDbConnection> connectionFactory, 
            Either<string, IDbTransaction> transaction)
        {
            var command = connectionFactory().CreateCommand();

            return match(transaction,
                Right: trans =>
                {
                    command.Transaction = trans;
                    return Right<string, IDbCommand>(command);
                },
                Left: error => Left<string, IDbCommand>(error));
        }

        public static async Task<Either<string, Unit>> SendAsync(
            Func<Task<int>> executeQuery,
            BrokerMessage message,
            Either<string, IDbCommand> command)
        {
            return await match(
                command,
                Right: async cmd =>
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
                Left: error => Task.FromResult(Left<string, Unit>(error)));
        }

        public static async Task<Either<string, BrokerMessage>> ReceiveAsync(
            Func<CancellationToken, Task<int>> executeQuery,
            string queueName, 
            CancellationToken token, 
            Either<string, IDbCommand> command)
        {
            return await match(
                command,
                Right: async cmd =>
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
                        return Right<string, BrokerMessage>(new BrokerMessage(
                            (string)parameters[0].Value,
                            (string)parameters[1].Value,
                            (Guid)parameters[2].Value,
                            (Guid)parameters[3].Value));
                    }
                    else
                        return Right<string, BrokerMessage>(BrokerMessage.Empty);
                },
                Left: error => Task.FromResult(Left<string, BrokerMessage>(error)));
        }

        public static async Task<Either<string, Unit>> EndDialogAsync(
            Func<Task<int>> executeQuery,
            Guid conversation,
            Either<string, IDbCommand> command)
        {
            return await match(
                command,
                Right: async cmd =>
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
                Left: error => Task.FromResult(Left<string, Unit>(error)));
        }

        public static Either<string, Unit> Commit(Either<string, IDbTransaction> transaction)
        {
            return match(
                transaction,
                Right: trans =>
                {
                    return match(
                        Try(() => { trans.Commit(); return Unit.Default; }),
                        Succ: unit => Right<string, Unit>(unit),
                        Fail: exception => Left<string, Unit>(exception.GetExceptionChainMessages()));
                },
                Left: error => true == false
                    ? Right<string, Unit>(Unit.Default)
                    : Left<string, Unit>(error));
        }
    }
}