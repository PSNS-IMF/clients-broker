using System;
using LanguageExt;
using Psns.Common.SystemExtensions;

namespace Psns.Common.Clients.Broker
{
    /// <summary>
    /// Some functional extensions
    /// </summary>
    public static class AppPrelude
    {
        /// <summary>
        /// Runs a function within a try/catch block
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fun"></param>
        /// <returns>A string representation of the exception on failure</returns>
        public static Either<string, T> Safe<T>(Func<T> fun)
        {
            try
            {
                return fun();
            }
            catch(Exception e)
            {
                return e.GetExceptionChainMessages();
            }
        }
    }
}