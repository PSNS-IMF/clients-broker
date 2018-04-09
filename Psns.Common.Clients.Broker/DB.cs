using Psns.Common.Functional;
using Psns.Common.SystemExtensions;
using Psns.Common.SystemExtensions.Diagnostics;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using static Psns.Common.Functional.Prelude;
using static Psns.Common.SystemExtensions.Database.Prelude;

namespace Psns.Common.Clients.Broker
{
    /// <summary>
    /// Contains lower-level Db functions
    /// </summary>
    public static class DB
    {
        /// <summary>
        /// Composes a function of:
        /// 1) a function that sets up a command with it's text and parameters
        /// 2) a function that takes the setup command and returns a result of T
        /// </summary>
        /// <returns></returns>
        public static Func<
            Maybe<Log>,
            Func<IDbConnection>,
            OpenAsync,
            Func<IDbCommand, IDbCommand>,
            Func<IDbCommand, Task<T>>,
            TryAsync<T>> CommandFactory<T>() =>
            (log, connectionFactory, openAsync, setupCommand, withCommand) =>
            {
                var commitTransaction = fun((Func<IDbTransaction, TryAsync<T>> func, IDbTransaction transaction) => 
                    func(transaction).Regardless(TryAsync(async () => (await transaction.AsTask()).Commit())));
                var createCommand = CreateCommandAsync<T>().Par(withCommand.Compose(setupCommand));
                var runWithCommit = commitTransaction.Par(createCommand);

                var beginTransaction = BeginTransactionAsync<T>().Par(runWithCommit);
                var connect = ConnectAsync<T>()
                    .Par(beginTransaction, async cmd => await openAsync(cmd))
                    .Compose(() => connectionFactory);

                return connect();
            };

        /// <summary>
        /// Creates a function that executes the DB command to get a message and parse as a BrokerMessage.
        /// </summary>
        /// <returns>BrokerMessage</returns>
        public static Func<
            ExecuteNonQueryAsync,
            IDbCommand,
            Task<BrokerMessage>> RunReceiveCommandFactory() => async (executeNonQueryAsync, command) =>
                {
                    var message = BrokerMessage.Empty;
                    var result = await executeNonQueryAsync(command);
                    var parameters = command.Parameters;

                    if (!(parameters[0].AsSqlParameter().Value is DBNull))
                    {
                        message = new BrokerMessage(
                            parameters[0].AsSqlParameter().Value.ToString(),
                            parameters[1].AsSqlParameter().Value.ToString(),
                            parameters[2].AsSqlParameter().Value.ToString(),
                            (Guid)parameters[3].AsSqlParameter().Value,
                            (Guid)parameters[4].AsSqlParameter().Value);
                    }

                    return message;
                };

        /// <summary>
        /// Creates a function that sets up a DB command with parameters 
        ///     and text to receive a BrokerMessage from a queue.
        /// </summary>
        /// <returns></returns>
        public static Func<string, IDbCommand, IDbCommand> SetupReceive() => (queueName, command) =>
        {
            new[]
            {
                new SqlParameter("@contract", SqlDbType.NVarChar, 128),
                new SqlParameter("@messageType", SqlDbType.NVarChar, 256),
                new SqlParameter("@message", SqlDbType.NVarChar, -1), // -1 is nvarchar(max)
                new SqlParameter("@conversationGroup", SqlDbType.UniqueIdentifier),
                new SqlParameter("@conversation", SqlDbType.UniqueIdentifier)
            }
            .Iter(param =>
            {
                param.Direction = ParameterDirection.Output;
                command.Parameters.Add(param);
            });

            command.CommandText = "WAITFOR (RECEIVE TOP(1) " +
                "@contract = service_contract_name, " +
                "@messageType = message_type_name, " +
                "@message = message_body, " +
                "@conversationGroup = conversation_group_id, " +
                "@conversation = conversation_handle " +
                $"FROM [{ queueName }]), TIMEOUT 5000;";

            return command;
        };

        /// <summary>
        /// Creates a function that sets up a DB command with parameters 
        ///     to end a given Service Broker Conversation.
        /// </summary>
        /// <returns></returns>
        public static Func<Guid, IDbCommand, IDbCommand> SetupEndDialog() => (conversationId, command) =>
        {
            command.Parameters.Add(new SqlParameter("@conversation", conversationId));

            command.CommandText = "END CONVERSATION @conversation";

            return command;
        };
    }

    internal static class Extensions
    {
        public static SqlParameter AsSqlParameter(this object obj) =>
            (obj as SqlParameter) ?? new SqlParameter(string.Empty, DBNull.Value);

        public static string ToLogString(this IDbCommand self, Maybe<string> callerName) =>
            $@"{callerName} -> Param Count: {self?.Parameters?.Count.ToString() ?? "Null"} Connection State: {
                self?.Connection?.State.ToString() ?? "Null"} Transaction Isolation Leve: {
                self?.Transaction?.IsolationLevel.ToString() ?? "Null"}";
    }
}
