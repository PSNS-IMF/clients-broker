using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using LanguageExt;
using static LanguageExt.List;
using static LanguageExt.Prelude;

namespace Psns.Common.Clients.Broker
{
    public static partial class AppPrelude
    {
        public static readonly Func<IDbConnection, Either<Exception, OpenConnection>> openConnection = connection =>
            safe(() =>
            {
                connection.Open();
                return new OpenConnection(connection);
            });

        public static readonly Func<Either<Exception, OpenConnection>, Either<Exception, Transaction>> beginTransaction = connection =>
            from conn in connection
            from trans in conn.BeginTransaction()
            select new Transaction(trans);

        public static readonly Func<Either<Exception, Transaction>, Either<Exception, Func<Either<Exception, IDbCommand>>>> createCommandFactory =
            transaction =>
                    from trans in transaction
                    select trans.CreateCommandFactory();

        public static readonly Func<
           Action<string>,
           Either<Exception, Func<Either<Exception, IDbCommand>>>,
           Func<IDbCommand, Task<int>>,
           string,
           string,
           string,
           Task<Either<Exception, Guid>>> beginConversationAsync = 
            (log, commandFactory, query, fromService, toService, contract) =>
            {
                var conversationParameter = new SqlParameter("@conversation", SqlDbType.UniqueIdentifier);
                conversationParameter.Direction = ParameterDirection.Output;

                var fromServiceParameter = new SqlParameter("@fromService", SqlDbType.NVarChar, fromService.Length);
                fromServiceParameter.Value = fromService;

                var toServiceParameter = new SqlParameter("@toService", SqlDbType.NVarChar, toService.Length);
                toServiceParameter.Value = toService;

                var contractParameter = new SqlParameter("@contract", SqlDbType.NVarChar, contract.Length);
                contractParameter.Value = contract;

                var parameters = new[] { conversationParameter, fromServiceParameter, toServiceParameter, contractParameter };

                return matchAsync(
                    from factory in commandFactory
                    from command in factory()
                    select command,
                    right: command =>
                        use(
                            command,
                            async cmd =>
                            {
                                iter(parameters, param => cmd.Parameters.Add(param));

                                cmd.CommandText = "BEGIN DIALOG CONVERSATION @conversation " +
                                    "FROM SERVICE @fromService " +
                                    "TO SERVICE @toService " +
                                    "ON CONTRACT @contract " +
                                    "WITH ENCRYPTION = OFF;";

                                logCommand("beginConversationAsync", cmd, log);

                                return await safeAsync(async () =>
                                {
                                    await query(cmd);

                                    return conversationParameter.Value is DBNull
                                        ? Guid.Empty
                                        : (Guid)conversationParameter.Value;
                                });
                            }),
                    left: error => error);
            };

        public static readonly Func<
            Action<string>,
            Either<Exception, Func<Either<Exception, IDbCommand>>>,
            Func<IDbCommand, Task<int>>,
            string,
            Task<Either<Exception, BrokerMessage>>> receiveAsync = (log, commandFactory, query, queueName) =>
            {
                var parameters = new[]
                {
                    new SqlParameter("@contract", SqlDbType.NVarChar, 128),
                    new SqlParameter("@messageType", SqlDbType.NVarChar, 256),
                    new SqlParameter("@message", SqlDbType.NVarChar, -1),
                    new SqlParameter("@conversationGroup", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@conversation", SqlDbType.UniqueIdentifier)
                };

                return matchAsync(
                    from factory in commandFactory
                    from command in factory()
                    select command,
                    right: command =>
                        use(
                            command,
                            async cmd =>
                            {
                                iter(parameters, parameter =>
                                {
                                    parameter.Direction = ParameterDirection.Output;
                                    cmd.Parameters.Add(parameter);
                                });

                                cmd.CommandText = "WAITFOR (RECEIVE TOP(1) " +
                                    "@contract = service_contract_name, " +
                                    "@messageType = message_type_name, " +
                                    "@message = message_body, " +
                                    "@conversationGroup = conversation_group_id, " +
                                    "@conversation = conversation_handle " +
                                    $"FROM [{ queueName }]), TIMEOUT 5000;";

                                logCommand("receiveAsync", cmd, log);

                                return await safeAsync(async () =>
                                {
                                    await query(cmd);

                                    if(!(parameters[0].Value is DBNull))
                                    {
                                        return new BrokerMessage(
                                            parameters[0].Value.ToString(),
                                            parameters[1].Value.ToString(),
                                            parameters[2].Value.ToString(),
                                            (Guid)parameters[3].Value,
                                            (Guid)parameters[4].Value);
                                    }
                                    else
                                        return BrokerMessage.Empty;
                                });
                            }),
                    left: error => error);
            };

        public static readonly Func<
            Action<string>,
            Either<Exception, Func<Either<Exception, IDbCommand>>>,
            Func<IDbCommand, Task<int>>,
            BrokerMessage,
            Task<Either<Exception, Unit>>> sendAsync = (log, commandFactory, query, message) =>
            {
                var messageParameter = new SqlParameter("@message", SqlDbType.NVarChar, message.Message.Length);
                messageParameter.Value = message.Message;
                var messageTypeParameter = new SqlParameter("@messageType", SqlDbType.NVarChar, message.MessageType.Length);
                messageTypeParameter.Value = message.MessageType;

                var parameters = new[]
                {
                    messageParameter,
                    messageTypeParameter,
                    new SqlParameter("@conversation", message.Conversation)
                };

                return matchAsync(
                    from factory in commandFactory
                    from command in factory()
                    select command,
                    right: command =>
                        use(
                            command,
                            async cmd =>
                            {
                                iter(parameters, parameter => cmd.Parameters.Add(parameter));

                                cmd.CommandText = "SEND ON CONVERSATION @conversation MESSAGE TYPE @messageType (@message)";

                                logCommand("sendAsync", cmd, log);
                                log($"sendAsync -> message: {message.ToString()}");

                                return await safeAsync(async () => { await query(cmd); return Unit.Default; });
                            }),
                    left: error => error);
            };

        public static readonly Func<
            Action<string>,
            Either<Exception, Func<Either<Exception, IDbCommand>>>,
            Func<IDbCommand, Task<int>>,
            Guid,
            Task<Either<Exception, Unit>>> endConversationAsync = (log, commandFactory, query, conversation) =>
            {
                var parameters = new[]
                {
                    new SqlParameter("@conversation", conversation)
                };

                return matchAsync(
                    from factory in commandFactory
                    from command in factory()
                    select command,
                    right: command =>
                        use(
                            command,
                            async cmd =>
                            {
                                iter(parameters, parameter => cmd.Parameters.Add(parameter));

                                cmd.CommandText = "END CONVERSATION @conversation";

                                logCommand("endConversationAsync", cmd, log);

                                return await safeAsync(async () => { await query(cmd); return Unit.Default; });
                            }),
                    left: error => error);
            };

        static Unit logCommand(Some<string> callerName, IDbCommand command, Action<string> log)
        {
            log($@"{callerName} -> Param Count: {command?.Parameters?.Count.ToString() ?? "Null"} Connection State: {
                command?.Connection?.State.ToString() ?? "Null"} Transaction Isolation Leve: {
                command?.Transaction?.IsolationLevel.ToString() ?? "Null"}");
            return Unit.Default;
        }
    }
}