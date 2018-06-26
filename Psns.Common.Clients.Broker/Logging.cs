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

        public static UnitValue Debug(this Maybe<Log> self, string message, string category = GeneralLogCategory) =>
            self.Log<UnitValue>(message, TraceEventType.Verbose, category);

        public static T Debug<T>(this Maybe<Log> self, T t, string message) =>
            t.Tap(_ => self.Log<T>(message, TraceEventType.Verbose));

        public static T Error<T>(this Maybe<Log> self, T t) =>
            self.Log(t, TraceEventType.Error, GeneralLogCategory);

        public static T Error<T>(this Maybe<Log> self, string message) =>
            self.Log<T>(message, TraceEventType.Error, GeneralLogCategory);

        public static T Log<T>(this Maybe<Log> self, string message, TraceEventType type, string category = GeneralLogCategory) =>
            default(T).Tap(t => self.Log(val: message, type: type, category: category));

        public static T Log<T>(this Maybe<Log> self, T val, TraceEventType type, string category = GeneralLogCategory) =>
            self.Match(
                some: logger => val.Tap(_ => logger(val.ToString(), category, type)), 
                none: () => val);
    }
}
