using Psns.Common.Functional;
using Psns.Common.SystemExtensions.Diagnostics;
using System.Diagnostics;
using static Psns.Common.SystemExtensions.Diagnostics.Prelude;

namespace Psns.Common.Clients.Broker
{
    internal static class LogExtensions
    {
        public static T Debug<T>(this Maybe<Log> self, T t) =>
            self.Log(t, TraceEventType.Verbose);

        public static T Debug<T>(this Maybe<Log> self, string message) =>
            self.Log<T>(message, TraceEventType.Verbose);

        public static T Debug<T>(this Maybe<Log> self, T t, string message) =>
            t.Tap(_ => self.Debug(message));

        public static T Info<T>(this Maybe<Log> self, T t) =>
            self.Log(t, TraceEventType.Information);

        public static T Info<T>(this Maybe<Log> self, string message) =>
            self.Log<T>(message, TraceEventType.Information);

        public static T Warning<T>(this Maybe<Log> self, T t) =>
            self.Log(t, TraceEventType.Warning);

        public static T Warning<T>(this Maybe<Log> self, string message) =>
            self.Log<T>(message, TraceEventType.Warning);

        public static T Error<T>(this Maybe<Log> self, T t) =>
            self.Log(t, TraceEventType.Error);

        public static T Error<T>(this Maybe<Log> self, string message) =>
            self.Log<T>(message, TraceEventType.Error);

        public static T Log<T>(this Maybe<Log> self, string message, TraceEventType type) =>
            default(T).Tap(t => self.Log(val: message, type: type));

        public static T Log<T>(this Maybe<Log> self, T val, TraceEventType type) =>
            self.Match(
                some: logger => val.Tap(_ => logger(val.ToString(), GeneralLogCategory, type)), 
                none: () => val);
    }
}
