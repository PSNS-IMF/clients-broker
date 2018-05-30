module ClientTests

open System
open System.Collections.Generic
open System.Data
open System.Data.SqlClient
open System.Threading
open System.Threading.Tasks
open NUnit.Framework
open Foq
open FsUnit
open Psns.Common.Clients.Broker
open Psns.Common.SystemExtensions.Diagnostics

type ex = Psns.Common.Functional.Prelude
type tryex = Psns.Common.Functional.TryExtensions

let initParamEnumerator eCalls = Mock<IEnumerator<SqlParameter>>().Setup(fun e -> <@ e.Current @>).Returns(new SqlParameter()).Setup(fun e -> <@ e.MoveNext() @>).Returns(fun () ->
    incr eCalls
    match eCalls.Value with
        | 0 -> true
        | _ -> false)

let initLog logEntries = new Log(fun msg -> fun _ -> fun _ ->
    logEntries := List.Cons(msg, !logEntries)
    ())

let makeGuid() = new Guid(String.replicate 32 "a")
let guidMatch = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

module ``regarding observers`` =

    [<Test>]
    let ``it should call observers independently.`` () =
        let resetEvent = new AutoResetEvent false

        let eCalls = ref 0
        let mockParamEnumerator = initParamEnumerator eCalls

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
        
        let entries = ref List.empty<string>
        let log = initLog entries

        let command = Mock<IDbCommand>().Setup(fun cmd -> <@ cmd.Parameters @>).Returns(parms.Create()).Create()
        let connection = Mock<IDbConnection>().Setup(fun conn -> <@ conn.CreateCommand() @>).Returns(command)
        let transaction = Mock<IDbTransaction>().Setup(fun trans -> <@ trans.Connection @>).Returns(connection.Create()).Create()
        let finalConnection = connection.Setup(fun conn -> <@ conn.BeginTransaction() @>).Returns(transaction)

        let factory = Func<IDbConnection> (fun () -> finalConnection.Create())
        let openAsync = ex.Some(new OpenAsync(fun conn -> Task.FromResult(conn)))
        let execQueryAsync = ex.Some(new ExecuteNonQueryAsync(fun _ -> Task.FromResult(0)))
        let client = new BrokerClient(factory, openAsync, execQueryAsync, ex.Some(log), Psns.Common.Functional.Maybe<TaskScheduler>.None)

        let observer = Mock<IBrokerObserver>().Setup(fun o -> <@ o.OnNext(any()) @>).Calls<unit>(fun _ ->
            resetEvent.Set() |> ignore).Create()
        let observers = [ 1..9 ] |> List.map (fun _ -> Mock<IBrokerObserver>().Create())
        let observers = List.Cons(observer, observers)
        observers |> List.map (fun obs -> client.Subscribe obs) |> ignore
    
        let running = client.ReceiveMessages("queue")
        resetEvent.WaitOne() |> ignore

        running.StopReceiving().Failed |> should equal false
        verify <@ observer.OnNext(is(fun msg -> msg.MessageType = "type")) @> atleastonce
        client.Subscribers.Count |> should equal 0
        !entries |> List.contains "Calling Observers OnCompleted" |> should equal true

module ``beginning conversations`` =

    let initParams (convoParam: SqlParameter option) addedParams eCalls = (Mock<IDataParameterCollection>().Setup(fun p -> <@ p.[It.IsAny<int>()] @>).Calls<int>(function
        | 0 -> (convoParam |> function | Some param -> param | _ -> new SqlParameter("@conversation", makeGuid())) :> obj
        | _ -> raise <| ArgumentOutOfRangeException("index"))
        |> 
            fun ps ->
                ps.Setup(fun p -> <@ p.Add(any()) @>).Calls<obj>(fun o ->
                let param = (o :?> SqlParameter)
                addedParams := List.Cons(param, !addedParams)

                param.SqlDbType |> function
                    | SqlDbType.NVarChar ->
                        (o :?> SqlParameter).Value |> function
                            | null -> (o :?> SqlParameter).Value <- "string"; 1
                            | content ->
                                match content.ToString() with
                                | "" -> (o :?> SqlParameter).Value <- "string"; 1
                                | _ -> 0
                    | SqlDbType.UniqueIdentifier -> (o :?> SqlParameter).Value <- "Uid"; 1
                    | _ -> 0)
                |> fun ps -> ps.Setup(fun p -> <@ p.GetEnumerator() @>).Returns(initParamEnumerator(eCalls).Create()))

    let verifyParams (parms: SqlParameter list ref) =
        !parms |> Seq.length |> should equal 4
        !parms |> Seq.iter (fun param ->
            match param.ParameterName with
            | "@contract" -> "contract"
            | "@fromService" -> "sender"
            | "@toService" -> "receiver"
            | "@conversation" -> "Uid"
            | _ -> failwith "match not exhaustive"
            |> fun value ->
                param.Value |> should equal value
                param.Size |> should equal value.Length)

    let verifyCommand (command: IDbCommand) =
        verify <@ command.CommandText <- "BEGIN DIALOG CONVERSATION @conversation FROM SERVICE @fromService TO SERVICE @toService ON CONTRACT @contract WITH ENCRYPTION = OFF;" @> once

    [<Test>]
    let ``it should begin a conversation correctly.`` () =
        let eCalls = ref 0
        let addedParams = ref List.empty<SqlParameter>
        let parms = initParams None addedParams eCalls
        
        let logEntries = ref List.empty<string>
        let log = initLog logEntries

        let command = Mock<IDbCommand>().Setup(fun cmd -> <@ cmd.Parameters @>).Returns(parms.Create()).Create()
        let connection = Mock<IDbConnection>().Setup(fun conn -> <@ conn.CreateCommand() @>).Returns(command)
        let transaction = Mock<IDbTransaction>().Setup(fun trans -> <@ trans.Connection @>).Returns(connection.Create()).Create()
        let finalConnection = connection.Setup(fun conn -> <@ conn.BeginTransaction() @>).Returns(transaction)

        let factory = Func<IDbConnection> (fun () -> finalConnection.Create())
        let openAsync = ex.Some(new OpenAsync(fun conn -> Task.FromResult(conn)))
        let execQueryAsync = ex.Some(new ExecuteNonQueryAsync(fun _ -> Task.FromResult(0)))
        let client = new BrokerClient(factory, openAsync, execQueryAsync, ex.Some(log), Psns.Common.Functional.Maybe<TaskScheduler>.None)

        let conversationId = tryex.Match(client.BeginConversation("sender", "receiver", "contract"), (fun id -> id |> string), (fun ex -> ex.Message))

        conversationId |> should equal guidMatch
        verifyCommand command
        verifyParams addedParams

    [<Test>]
    let ``it should begin a conversation correctly when ConversationId is Null.`` () =
        let eCalls = ref 0
        let addedParams = ref List.empty<SqlParameter>
        let parms = initParams (Some(new SqlParameter("@conversation", DBNull.Value))) addedParams eCalls
        
        let logEntries = ref List.empty<string>
        let log = initLog logEntries

        let command = Mock<IDbCommand>().Setup(fun cmd -> <@ cmd.Parameters @>).Returns(parms.Create()).Create()
        let connection = Mock<IDbConnection>().Setup(fun conn -> <@ conn.CreateCommand() @>).Returns(command)
        let transaction = Mock<IDbTransaction>().Setup(fun trans -> <@ trans.Connection @>).Returns(connection.Create()).Create()
        let finalConnection = connection.Setup(fun conn -> <@ conn.BeginTransaction() @>).Returns(transaction)

        let factory = Func<IDbConnection> (fun () -> finalConnection.Create())
        let openAsync = ex.Some(new OpenAsync(fun conn -> Task.FromResult(conn)))
        let execQueryAsync = ex.Some(new ExecuteNonQueryAsync(fun _ -> Task.FromResult(0)))
        let client = new BrokerClient(factory, openAsync, execQueryAsync, ex.Some(log), Psns.Common.Functional.Maybe<TaskScheduler>.None)

        let conversationId = tryex.Match(client.BeginConversation("sender", "receiver", "contract"), (fun id -> id |> string), (fun ex -> ex.Message))

        conversationId |> should equal (Guid.Empty |> string)

        verifyCommand command
        verifyParams addedParams

module ``sending messages`` =
    
    let initParams addedParams eCalls = (Mock<IDataParameterCollection>().Setup(fun p -> <@ p.Add(any()) @>).Calls<obj>(fun o ->
        let param = (o :?> SqlParameter)
        addedParams := List.Cons(param, !addedParams)

        param.SqlDbType |> function
            | SqlDbType.NVarChar ->
                (o :?> SqlParameter).Value |> function
                    | null -> (o :?> SqlParameter).Value <- "string"; 1
                    | content ->
                        match content.ToString() with
                        | "" -> (o :?> SqlParameter).Value <- "string"; 1
                        | _ -> 0
            | SqlDbType.UniqueIdentifier -> (o :?> SqlParameter).Value |> function
                | null -> (o :?> SqlParameter).Value <- "Uid"; 1
                | _ -> 0
            | _ -> 0)
        |>
            fun ps -> ps.Setup(fun p -> <@ p.GetEnumerator() @>).Returns(initParamEnumerator(eCalls).Create()))

    [<Test>]
    let ``it should setup the Command correctly.`` () =

        let eCalls = ref 0
        let addedParams = ref List.empty<SqlParameter>
        let parms = initParams addedParams eCalls
        
        let logEntries = ref List.empty<string>
        let log = initLog logEntries

        let command = Mock<IDbCommand>().Setup(fun cmd -> <@ cmd.Parameters @>).Returns(parms.Create()).Create()
        let connection = Mock<IDbConnection>().Setup(fun conn -> <@ conn.CreateCommand() @>).Returns(command)
        let transaction = Mock<IDbTransaction>().Setup(fun trans -> <@ trans.Connection @>).Returns(connection.Create()).Create()
        let finalConnection = connection.Setup(fun conn -> <@ conn.BeginTransaction() @>).Returns(transaction)

        let factory = Func<IDbConnection> (fun () -> finalConnection.Create())
        let openAsync = ex.Some(new OpenAsync(fun conn -> Task.FromResult(conn)))
        let execQueryAsync = ex.Some(new ExecuteNonQueryAsync(fun _ -> Task.FromResult(0)))
        let client = new BrokerClient(factory, openAsync, execQueryAsync, ex.Some(log), Psns.Common.Functional.Maybe<TaskScheduler>.None)

        let res = tryex.Match(client.Send(new BrokerMessage("contract", "type", "msg", Guid.Empty, makeGuid())), (fun _ -> "ok"), (fun ex -> ex.Message))

        res |> should equal "ok"

        verify <@ command.CommandText <- "SEND ON CONVERSATION @conversation MESSAGE TYPE @messageType (@message)" @> once

        !addedParams |> Seq.length |> should equal 3
        !addedParams |> Seq.iter (fun param ->
            match param.ParameterName with
            | "@message" -> "msg"
            | "@messageType" -> "type"
            | "@conversation" -> guidMatch
            | _ -> failwith "match not exhaustive"
            |> fun value -> param.Value.ToString() |> should equal value)
