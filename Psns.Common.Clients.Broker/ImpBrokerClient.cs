using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using CSharpFunctionalExtensions;
using static Psns.Common.Clients.Broker.Functional;

namespace Psns.Common.Clients.Broker
{
    /// <summary>
    /// Provides communications with SQL Service Broker
    /// </summary>
    public class ImpBrokerClient : IBrokerClient
    {
        readonly DbConnection _connection;
        DbTransaction _transaction;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connection"></param>
        public ImpBrokerClient(DbConnection connection)
        {
            _connection = connection;
        }

        /// <summary>
        /// Opens the SQL connection
        /// </summary>
        /// <returns></returns>
        public Result Open()
        {
            return Try(_connection.Open);
        }

        /// <summary>
        /// Begins a SQL transaction
        /// </summary>
        /// <returns></returns>
        public Result BeginTransaction()
        {
            return Try(_connection.BeginTransaction)
                .OnFailure(error => Result.Fail(error))
                .OnSuccess(result =>
                {
                    _transaction = result;
                    return Result.Ok();
                });
        }

        /// <summary>
        /// Receive a message asynchronously from the Broker Service
        /// </summary>
        /// <param name="queueName"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<Result<BrokerMessage>> ReceiveAsync(string queueName, CancellationToken token)
        {
            var command = _connection.CreateCommand() as DbCommand;
            command.Transaction = _transaction;

            var parameters = new[]
            {
                new SqlParameter("@messageType", SqlDbType.NVarChar, 256),
                new SqlParameter("@message", SqlDbType.NVarChar, 4000),
                new SqlParameter("@conversationGroup", SqlDbType.UniqueIdentifier),
                new SqlParameter("@conversation", SqlDbType.UniqueIdentifier)
            };

            Array.ForEach(parameters, parameter =>
                {
                    parameter.Direction = ParameterDirection.Output;
                    command.Parameters.Add(parameter);
                });

            command.CommandText = "WAITFOR (RECEIVE TOP(1) @messageType = message_type_name, " +
                "@message = message_body, " +
                "@conversationGroup = conversation_group_id, " +
                "@conversation = conversation_handle " +
                "FROM [" + queueName + "]) " +
                ", TIMEOUT 5000;";

            return await Try(() => command.ExecuteNonQueryAsync(token))
                    .OnFailure(error => Result.Fail<BrokerMessage>(error))
                    .OnSuccess(result =>
                    {
                        if(!(parameters[0].Value is DBNull))
                        {
                            return Task.FromResult(Result.Ok(new BrokerMessage(
                                (string)parameters[0].Value,
                                (string)parameters[1].Value,
                                (Guid)parameters[2].Value,
                                (Guid)parameters[3].Value)));
                        }
                        else
                            return Task.FromResult(Result.Ok(BrokerMessage.Empty));
                    });
        }

        public Task<Result> SendAsync(BrokerMessage message)
        {
            throw new NotImplementedException();
        }

        public Task<Result> EndConversationAsync(Guid dialogHandle)
        {
            throw new NotImplementedException();
        }

        public Task<Result> EndConversationWithErrorAsync(Guid dialogHandle, int errorCode, string errorDescription)
        {
            throw new NotImplementedException();
        }

        public Result Commit()
        {
            throw new NotImplementedException();
        }

        public Result Rollback()
        {
            throw new NotImplementedException();
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if(!disposedValue)
            {
                if(disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~BrokerClient() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}