﻿using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.Contracts;
using System.Threading;
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

        public static readonly Func<Either<Exception, Transaction>, Either<Exception, Func<IDbCommand>>> createCommandFactory =
            transaction =>
                    from trans in transaction
                    from factory in trans.CreateCommandFactory()
                    select factory;

        public static readonly Func<
            Func<Either<Exception, Func<IDbCommand>>>,
            Func<IDbCommand, CancellationToken, Task<int>>,
            string,
            CancellationToken,
            Task<Either<Exception, BrokerMessage>>> receiveAsync = (commandFactory, query, queueName, cancelToken) =>
            {
                var parameters = new[]
                {
                    new SqlParameter("@messageType", SqlDbType.NVarChar, 256),
                    new SqlParameter("@message", SqlDbType.NVarChar, 4000),
                    new SqlParameter("@conversationGroup", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@conversation", SqlDbType.UniqueIdentifier)
                };

                return matchAsync(
                    from command in commandFactory()
                    select command(),
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
                                    $"FROM [{queueName}]), TIMEOUT 5000;";

                                return await safeAsync(async () =>
                                {
                                    await query(cmd, cancelToken);

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
            Func<Either<Exception, Func<IDbCommand>>>,
            Func<IDbCommand, Task<int>>,
            BrokerMessage,
            Task<Either<Exception, Unit>>> sendAsync = (commandFactory, query, message) =>
            {
                var parameters = new[]
                {
                    new SqlParameter("@message", SqlDbType.NVarChar, message.Message.Length),
                    new SqlParameter("@conversation", SqlDbType.UniqueIdentifier)
                };

                return matchAsync(
                    from command in commandFactory()
                    select command(),
                    right: command =>
                        use(
                            command,
                            async cmd =>
                            {
                                iter(parameters, parameter => cmd.Parameters.Add(parameter));

                                cmd.CommandText = "SEND ON CONVERSATION @conversation MESSAGE TYPE [" + message.MessageType + "] (@message)";

                                return await safeAsync(async () => { await query(cmd); return Unit.Default; });
                            }),
                    left: error => error);
            };

        public static readonly Func<
            Func<Either<Exception, Func<IDbCommand>>>,
            Func<IDbCommand, Task<int>>,
            Guid,
            Task<Either<Exception, Unit>>> endDialogAsync = (commandFactory, query, conversation) =>
            {
                var parameters = new[]
                {
                    new SqlParameter("@conversation", SqlDbType.UniqueIdentifier)
                };

                return matchAsync(
                    from command in commandFactory()
                    select command(),
                    right: command =>
                        use(
                            command,
                            async cmd =>
                            {
                                iter(parameters, parameter => cmd.Parameters.Add(parameter));

                                cmd.CommandText = "END CONVERSATION @conversation";

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
        public static Unit dispose(IDisposable d)
        {
            d.Dispose();
            return Unit.Default;
        }
    }
}