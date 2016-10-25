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
        public static Either<string, IDbTransaction> BeginTransaction(Func<IDbConnection> connectionFactory)
        {
            return match(
                Try(connectionFactory().BeginTransaction),
                transaction => Right<string, IDbTransaction>(transaction),
                exception => Left<string, IDbTransaction>(exception.GetExceptionChainMessages()));
        }

        public static Either<string, IDbCommand> CreateCommand(Func<IDbConnection> connectionFactory, Func<Either<string, IDbTransaction>> transactionFactory)
        {
            var command = connectionFactory().CreateCommand();

            return match(transactionFactory(),
                transaction =>
                {
                    command.Transaction = transaction;
                    return Right<string, IDbCommand>(command);
                },
                error => Left<string, IDbCommand>(error));
        }

        public static async Task<Either<string, BrokerMessage>> ReceiveAsync(string queueName, 
            CancellationToken token, 
            Func<Either<string, IDbCommand>> commandFactory, 
            Func<CancellationToken, Task<int>> executeQuery)
        {
            await match(
                commandFactory(),
                async command =>
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
                        command.Parameters.Add(parameter);
                    });

                    command.CommandText = "WAITFOR (RECEIVE TOP(1) " +
                        "@messageType = message_type_name, " +
                        "@message = message_body, " +
                        "@conversationGroup = conversation_group_id, " +
                        "@conversation = conversation_handle " +
                        $"FROM [{queueName}]), TIMEOUT 5000;";

                    await executeQuery(token);

                    if(!(parameters[0].Value is DBNull))
                    {
                        return new BrokerMessage(
                            (string)parameters[0].Value,
                            (string)parameters[1].Value,
                            (Guid)parameters[2].Value,
                            (Guid)parameters[3].Value);
                    }
                    else
                        return BrokerMessage.Empty;
                },
                error => Task.FromResult(Left<string, BrokerMessage>(error)));
        }
    }
}