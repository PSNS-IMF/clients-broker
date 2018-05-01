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
            TryAsync<T>> CommandFactoryAsync<T>() =>
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
        /// Composes a function of:
        /// 1) a function that sets up a command with it's text and parameters
        /// 2) a function that takes the setup command and returns a result of T
        /// </summary>
        /// <returns></returns>
        public static Func<
            Maybe<Log>,
            Func<IDbConnection>,
            Func<IDbCommand, IDbCommand>,
            Func<IDbCommand, T>,
            Try<T>> CommandFactory<T>() =>
            (log, connectionFactory, setupCommand, withCommand) =>
            {
                var commitTransaction = fun((Func<IDbTransaction, Try<T>> func, IDbTransaction transaction) =>
                    new Try<T>(fun(() => func(transaction).Regardless(Try(() => transaction.Commit())).Try())));
                var createCommand = CreateCommand<T>().Par(withCommand.Compose(setupCommand));
                var runWithCommit = commitTransaction.Par(createCommand);

                var beginTransaction = BeginTransaction<T>().Par(runWithCommit);
                var openConnection = fun((Func<IDbConnection, Try<T>> func, IDbConnection conn) => 
                    Try(() => conn.Tap(_ => conn.Open())).Bind(func));

                var connect = Connect<T>()
                    .Par(openConnection.Par(beginTransaction))
                    .Compose(() => fun(() => connectionFactory()));

                return connect();
            };

        /// <summary>
        /// Creates a function that executes the DB command to get a message and parse as a BrokerMessage.
        /// </summary>
        /// <returns>BrokerMessage</returns>
        public static Func<
            Maybe<Log>,
            ExecuteNonQueryAsync,
            IDbCommand,
            Task<BrokerMessage>> RunReceiveCommandFactory() => async (log, executeNonQueryAsync, command) =>
                {
                    var message = BrokerMessage.Empty;
                    var result = await executeNonQueryAsync(command);
                    var parameters = command.Parameters;

                    if (!(parameters[0].AsSqlParameter().Value is DBNull))
                    {
                        log.Debug(command.ToLogString(nameof(RunReceiveCommandFactory)));

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
            Cons(
                new SqlParameter("@contract", SqlDbType.NVarChar, 128),
                new SqlParameter("@messageType", SqlDbType.NVarChar, 256),
                new SqlParameter("@message", SqlDbType.NVarChar, -1), // -1 is nvarchar(max)
                new SqlParameter("@conversationGroup", SqlDbType.UniqueIdentifier),
                new SqlParameter("@conversation", SqlDbType.UniqueIdentifier))
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
        public static Func<Maybe<Log>, Guid, IDbCommand, IDbCommand> SetupEndDialog() => (log, conversationId, command) =>
        {
            command.Parameters.Add(new SqlParameter("@conversation", conversationId));

            command.CommandText = "END CONVERSATION @conversation";

            return log.Debug(command, command.ToLogString(nameof(SetupEndDialog)));
        };
    }

    internal static class Extensions
    {
        public static SqlParameter AsSqlParameter(this object obj) =>
            (obj as SqlParameter) ?? new SqlParameter(string.Empty, DBNull.Value);
    }
}
