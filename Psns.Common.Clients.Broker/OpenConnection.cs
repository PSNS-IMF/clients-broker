using System;
using System.Data;
using LanguageExt;
using static Psns.Common.Clients.Broker.AppPrelude;

namespace Psns.Common.Clients.Broker
{
    /// <summary>
    /// A connection that has been opened
    /// </summary>
    public class OpenConnection : IDisposable
    {
        readonly IDbConnection _connection;

        internal OpenConnection(IDbConnection connection)
        {
            _connection = connection;
        }

        internal Either<string, IDbTransaction> BeginTransaction() =>
            safe(_connection.BeginTransaction);

        #region IDisposable Support
        bool disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if(!disposedValue)
            {
                if(disposing)
                    _connection.Dispose();

                disposedValue = true;
            }
        }
        public void Dispose() => Dispose(true);
        #endregion
    }
}
