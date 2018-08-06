module ProcessMessageTests

open System
open System.Threading
open System.Threading.Tasks
open Psns.Common.Clients.Broker
open Psns.Common.Functional
open Psns.Common.SystemExtensions.Diagnostics
open NUnit.Framework
open Foq
open FsUnit

type ex = Psns.Common.Functional.Prelude
type lib = Psns.Common.Clients.Broker.AppPrelude

let doTry (t: Either<exn, 'a>) = t.Match((fun x -> x), (fun ex -> failwith ex.Message))

let getLog () = ex.Some(new Log(fun _ -> fun _ -> fun _ -> ()))
let scheduler = TaskScheduler.Current
let getEndDialog calls = new EndDialog(fun _ -> incr calls; ex.unit.Ok())
let getObserver () = Mock<IBrokerObserver>().Create()
let run endCalls observer = lib.ProcessMessageFactory().Par(getLog(), scheduler, CancellationToken.None, getEndDialog endCalls, [observer])

[<Test>]
let ``it should return Unit if the Message is empty.`` () =
    let endCalls = ref 0
    let observer = getObserver()
    (run endCalls observer).Invoke(BrokerMessage.Empty) |> doTry |> should equal ex.unit

    !endCalls |> should equal 0

    verify <@ observer.OnNext(any()) @> never
    verify <@ observer.OnError(any(), any()) @> never

[<Test>]
let ``it should call endDialog if the Message is not empty.`` () =
    let endCalls = ref 0
    let observer = getObserver()
    (run endCalls observer).Invoke(new BrokerMessage("contract", "type", "msg", Guid.Empty, Guid.Empty)) |> doTry |> should equal ex.unit

    !endCalls |> should equal 1

    verify <@ observer.OnNext(any()) @> once
    verify <@ observer.OnError(any(), any()) @> never