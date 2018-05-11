module BrokerMessageTests

open System
open System.Xml.Linq
open System.Text.RegularExpressions
open NUnit.Framework
open FsUnit
open Psns.Common.Clients.Broker
open Psns.Common.Functional

type ex = Psns.Common.Functional.Prelude
let xname str = XName.op_Implicit str
let doTry (t: Try<'a>) = t.Match((fun x -> x), (fun ex -> failwith ex.Message))
let removeSpaces str = Regex.Replace(str, @"\s+", "")

[<Test>]
let ``it should serialize properly.`` () =
    let xml = new XElement(xname "root", new XElement(xname "child", "value"))

    let json = doTry ((new BrokerMessage ("contract", "type", removeSpaces (xml.ToString()), Guid.Empty, Guid.Empty)).AsJson())
    let message = doTry (json.FromJson())

    Seq.iter
        (fun pair -> fst pair |> should equal (snd pair))
        ["contract", message.Contract; "type", message.MessageType; "<root><child>value</child></root>", message.Message]