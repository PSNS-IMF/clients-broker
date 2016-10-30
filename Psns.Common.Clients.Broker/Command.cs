using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics.Contracts;
using LanguageExt;
using static LanguageExt.Prelude;
using static LanguageExt.List;
using static Psns.Common.Clients.Broker.AppPrelude;

namespace Psns.Common.Clients.Broker
{
    [Pure]
    public class Command
    {
        readonly Func<Either<Exception, IDbCommand>> _commandFactory;

        internal Command(Func<Either<Exception, IDbCommand>> commandFactory)
        {
            _commandFactory = commandFactory;
        }

        public async Task<Either<Exception, BrokerMessage>> ReceiveAsync(
            Func<IDbCommand, CancellationToken, Task<int>> query,
            string queueName,
            CancellationToken cancelToken)
        {
            var parameters = new[]
            {
                new SqlParameter("@messageType", SqlDbType.NVarChar, 256),
                new SqlParameter("@message", SqlDbType.NVarChar, 4000),
                new SqlParameter("@conversationGroup", SqlDbType.UniqueIdentifier),
                new SqlParameter("@conversation", SqlDbType.UniqueIdentifier)
            };

            return await matchAsync(
                from command in _commandFactory()
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
        }

        public async Task<Either<Exception, Unit>> SendAsync(
            Func<IDbCommand, Task<int>> query,
            BrokerMessage message)
        {
            var parameters = new[]
            {
                new SqlParameter("@message", SqlDbType.NVarChar, message.Message.Length),
                new SqlParameter("@conversation", SqlDbType.UniqueIdentifier)
            };

            return await matchAsync(
                from command in _commandFactory()
                select command,
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
        }

        public async Task<Either<Exception, Unit>> EndDialogAsync(
            Func<IDbCommand, Task<int>> query,
            Guid conversation)
        {
            var parameters = new[]
            {
                new SqlParameter("@conversation", SqlDbType.UniqueIdentifier)
            };

            return await matchAsync(
                from command in _commandFactory()
                select command,
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
        }
    }
}