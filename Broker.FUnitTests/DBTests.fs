module DBTests

open System
open NUnit.Framework
open FsUnit
open System.Data
open Psns.Common.Functional
open Foq
open Psns.Common.SystemExtensions.Diagnostics

type ex = Psns.Common.Functional.Prelude
type db = Psns.Common.Clients.Broker.DB

[<Test>]
let ``it should call function in then sequence in the correct order.`` () =
    let ok = "result"

    let log = ex.Possible(new Log(fun msg -> fun cat -> fun eType -> ()))
    let count = ref 0
    let mutable callOrder = Map.empty<string, int>

    let updateCallsRet caller ret = incr count; callOrder <- callOrder.Add(caller, count.Value); ret
    let updateCalls caller = updateCallsRet caller ()
    let updateCallsInt caller = updateCallsRet caller 1

    let command = Mock<IDbCommand>.With(fun cmd -> 
        <@ 
            cmd.ExecuteNonQuery() --> updateCallsInt "query"
        @>)
    let mockConnection = Mock<IDbConnection>().Setup(fun conn ->
        <@ conn.CreateCommand() @>).Returns(fun () ->
            updateCalls "create cmd"; command)

    let transaction = Mock<IDbTransaction>.With(fun trans ->
        <@ 
            trans.Connection --> mockConnection.Create()
        @>)

    let mockConnection = mockConnection.Setup(fun conn -> <@ conn.BeginTransaction() @>).Returns(fun () -> transaction)
    let connection = mockConnection.Create()

    let connectionFactory = Func<IDbConnection> (fun () -> updateCalls "conn factory"; connection)
    let commandFactory = Func<IDbCommand, IDbCommand> (fun cmd -> updateCalls "cmd factory"; cmd)
    let withCommand = Func<IDbCommand, string> (fun _ -> updateCalls "with cmd"; ok)

    let result = db.CommandFactory<string>().Invoke(log, connectionFactory, commandFactory, withCommand)

    result.Match((fun res -> res), (fun ex -> ex.Message)) |> should equal ok

    callOrder.["conn factory"] |> should equal 2
    callOrder.["create cmd"] |> should equal 3
    callOrder.["cmd factory"] |> should equal 4
    callOrder.["query"] |> should equal 1
    callOrder.["with cmd"] |> should equal 5

    verify <@ command.Dispose() @> once
    verify <@ transaction.Commit() @> once
    verify <@ transaction.Dispose() @> once
    verify <@ connection.Dispose() @> once