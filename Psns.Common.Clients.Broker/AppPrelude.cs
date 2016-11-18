using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.Contracts;
using System.Threading.Tasks;
using LanguageExt;
using static LanguageExt.List;
using static LanguageExt.Prelude;

namespace Psns.Common.Clients.Broker
{
    public static class AppPrelude
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
                                            (string)parameters[0].Value,
                                            (string)parameters[1].Value,
                                            (Guid)parameters[2].Value,
                                            (Guid)parameters[3].Value);
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

        [Pure]
        public async static Task<R2> matchAsync<T, L, R2>(Either<L, T> self, Func<T, Task<R2>> right, Func<L, R2> left) =>
            await self.MatchAsync(right, left);

        [Pure]
        public async static Task<R2> matchAsync<T, L, R2>(Task<Either<L, T>> self, Func<T, R2> right, Func<L, R2> left)
        {
            var either = await self;

            return match(
                either,
                Right: val => right(val),
                Left: err => left(err));
        }

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
        public static Func<T, Option<R>> always<T, R>(T value, Action<T> map) =>
            (T input) => { map(input); return None; };

        [Pure]
        public static Either<L, R> Join<L, R>(this Either<L, R> self, Either<L, R> other) =>
            self.Join(other, o => o, i => i, (o, i) => i);

        [Pure]
        public static Either<L, R> Join<L, R, U>(this Either<L, R> self, Either<U, R> other, Func<U, L> project) =>
            self.Join(
                match(
                    other,
                    Right: r => Right<L, R>(r),
                    Left: u => project(u)));

        [Pure]
        public static Unit dispose(IDisposable d)
        {
            d.Dispose();
            return Unit.Default;
        }

        static Unit logCommand(Some<string> callerName, IDbCommand command, Action<string> log)
        {
            log($"{callerName} -> Param Count: {command.Parameters.Count} Connection State: {command.Connection.State.ToString()} Transaction Isolation Leve: {command.Transaction.IsolationLevel.ToString()}");
            return Unit.Default;
        }
    }
}