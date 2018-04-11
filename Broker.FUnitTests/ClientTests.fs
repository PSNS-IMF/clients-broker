module ClientTests

open System
open NUnit.Framework
open FsUnit
open System.Threading.Tasks
open System.Collections.Generic
open System.Data
open Psns.Common.Clients.Broker
open Psns.Common.Functional
open Foq
open System.Data.SqlClient
open System.Threading
open Psns.Common.SystemExtensions.Diagnostics

type ex = Psns.Common.Functional.Prelude

[<Test>]
let ``it should call observers independently.`` () =
    let resetEvent = new AutoResetEvent false

    let eCalls = ref 0
    let mockParamEnumerator = Mock<IEnumerator<SqlParameter>>().Setup(fun e -> <@ e.Current @>).Returns(new SqlParameter())
    let mockParamEnumerator = mockParamEnumerator.Setup(fun e -> <@ e.MoveNext() @>).Returns(fun () ->
        incr eCalls
        match eCalls.Value with
            | 0 -> true
            | _ -> false)

    let parms = Mock<IDataParameterCollection>().Setup(fun p -> <@ p.[It.IsAny<int>()] @>).Calls<int>(function
        | 0 -> (new SqlParameter("MessageType", "type") :> obj)
        | 1 -> (new SqlParameter("MessageType", "type") :> obj)
        | 2 -> (new SqlParameter("Message", "text") :> obj)
        | 3 -> (new SqlParameter("Conversation", Guid.Empty) :> obj)
        | 4 -> (new SqlParameter("ConversationId", Guid.Empty) :> obj)
        | _ -> raise <| ArgumentOutOfRangeException("index"))

    let parms = parms.Setup(fun p -> <@ p.Add(any()) @>).Calls<obj>(fun o ->
        (o :?> SqlParameter).SqlDbType |> function
            | SqlDbType.NVarChar ->
                (o :?> SqlParameter).Value |> function
                    | null -> (o :?> SqlParameter).Value <- "string"; 1
                    | content ->
                        match content.ToString() with
                        | "" -> (o :?> SqlParameter).Value <- "string"; 1
                        | _ -> 0
            | _ -> 0)

    let parms = parms.Setup(fun p -> <@ p.GetEnumerator() @>).Returns(mockParamEnumerator.Create())
        
    let mutable entries = List.empty<string>
    let log = new Log(fun msg -> fun cat -> fun eType ->
        entries <- List.Cons(msg, entries)
        ())

    let command = Mock<IDbCommand>().Setup(fun cmd -> <@ cmd.Parameters @>).Returns(parms.Create()).Create()
    let connection = Mock<IDbConnection>().Setup(fun conn -> <@ conn.CreateCommand() @>).Returns(command)
    let transaction = Mock<IDbTransaction>().Setup(fun trans -> <@ trans.Connection @>).Returns(connection.Create()).Create()
    let finalConnection = connection.Setup(fun conn -> <@ conn.BeginTransaction() @>).Returns(transaction)

    let factory = Func<IDbConnection> (fun () -> finalConnection.Create())
    let openAsync = ex.Some(new OpenAsync(fun conn -> conn.AsTask()))
    let execQueryAsync = ex.Some(new ExecuteNonQueryAsync(fun _ -> Task.FromResult(0)))
    let client = new BrokerClient(factory, openAsync, execQueryAsync, ex.Some(log), Maybe<TaskScheduler>.None)

    let observer = Mock<IObserver<BrokerMessage>>().Setup(fun o -> <@ o.OnNext(any()) @>).Calls<unit>(fun _ ->
        resetEvent.Set() |> ignore).Create()
    let observers = [ 1..9 ] |> List.map (fun _ -> Mock<IObserver<BrokerMessage>>().Create())
    let observers = List.Cons(observer, observers)
    observers |> List.map (fun obs -> client.Subscribe obs) |> ignore
    
    let running = client.ReceiveMessages("queue")
    resetEvent.WaitOne() |> ignore

    running.StopReceiving().Failed |> should equal false
    verify <@ observer.OnNext(is(fun msg -> msg.MessageType = "type")) @> atleastonce
    client.Subscribers.Count |> should equal 0
    entries |> List.contains "Calling Observers OnCompleted" |> should equal true