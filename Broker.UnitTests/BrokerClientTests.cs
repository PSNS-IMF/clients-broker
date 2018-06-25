using Moq;
using NUnit.Framework;
using Psns.Common.Clients.Broker;
using Psns.Common.Functional;
using Psns.Common.SystemExtensions;
using Psns.Common.SystemExtensions.Diagnostics;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static NUnit.StaticExpect.Expectations;
using static Psns.Common.Clients.Broker.Constants;
using static Psns.Common.Functional.Prelude;
using static Psns.Common.SystemExtensions.Diagnostics.Prelude;

namespace FluentBrokerClients.UnitTests
{
    public abstract class ClientTests
    {
        protected const string FailVal = "fail";
        protected const string QueueName = "TestQueue";
        protected const string MessageType = "MessageType";
        protected const string MessageText = "Message Text";

        protected BrokerClient _client;

        protected Mock<IDbConnection> _mockConnection;
        protected Mock<IDbTransaction> _mockTransaction;
        protected Mock<IDbCommand> _mockCommand;
        protected Mock<IDataParameterCollection> _mockParams;
        protected IDictionary<string, SqlParameter> _addedParams;

        protected Mock<OpenAsync> _mockOpenAsync;
        protected Mock<ExecuteNonQueryAsync> _mockExeNonQueryAsync;

        [SetUp]
        public void Setup()
        {
            _mockConnection = new Mock<IDbConnection>();
            _mockTransaction = new Mock<IDbTransaction>();
            _mockCommand = new Mock<IDbCommand>();
            _mockParams = new Mock<IDataParameterCollection>();
            _addedParams = new Dictionary<string, SqlParameter>();

            _mockConnection.Setup(c => c.BeginTransaction()).Returns(_mockTransaction.Object);
            _mockConnection.Setup(c => c.CreateCommand()).Returns(_mockCommand.Object);
            _mockTransaction.SetupGet(t => t.Connection).Returns(_mockConnection.Object);
            _mockCommand.SetupGet(c => c.Parameters).Returns(_mockParams.Object);

            var mockEnumerator = new Mock<IEnumerator<SqlParameter>>();
            mockEnumerator.SetupGet(e => e.Current).Returns(new SqlParameter());
            mockEnumerator.SetupSequence(e => e.MoveNext())
                .Returns(true)
                .Returns(false);

            _mockParams.Setup(p => p.GetEnumerator()).Returns(mockEnumerator.Object);

            _mockParams.Setup(p => p[It.IsAny<int>()]).Returns((int index) =>
            {
                var param = new SqlParameter();
                switch (index)
                {
                    case 0:
                    case 1:
                        param.Value = MessageType;
                        break;
                    case 2:
                        param.Value = MessageText;
                        break;
                    case 3:
                    case 4:
                        param.Value = Guid.Empty;
                        break;
                }

                return param as object;
            });

            _mockParams.Setup(p => p.Add(It.IsAny<object>())).Callback((object obj) =>
            {
                var param = obj as SqlParameter;

                if (param.SqlDbType == SqlDbType.NVarChar && (param.Value == null || string.IsNullOrEmpty(param.Value.ToString())))
                    param.Value = "string";

                _addedParams[param.ParameterName] = param;
            });

            _mockOpenAsync = new Mock<OpenAsync>();
            _mockOpenAsync.Setup(f => f(It.IsAny<IDbConnection>())).Returns((IDbConnection conn) => conn.AsTask());

            _mockExeNonQueryAsync = new Mock<ExecuteNonQueryAsync>();
            _mockExeNonQueryAsync.Setup(f => f(It.IsAny<IDbCommand>())).Returns(Task.FromResult(0));

            _client = new BrokerClient(
              () => _mockConnection.Object,
              _mockOpenAsync.Object,
              _mockExeNonQueryAsync.Object);
        }

        protected Mock<IBrokerObserver> CreateMockObserver() =>
            new Mock<IBrokerObserver>();

        protected IBrokerObserver CreateObserver() =>
            CreateMockObserver().Object;

        protected IDisposable AddSubscriber() =>
            _client.Subscribe(CreateObserver());

        protected Tuple<
            Mock<IDbConnection>,
            Mock<IDbTransaction>,
            Mock<IDbCommand>,
            Mock<IDataParameterCollection>,
            Dictionary<string, SqlParameter>> CreateMocks(string messageTypeToReceive = MessageType)
        {
            var addedParams = new Dictionary<string, SqlParameter>();
            var mockParams = new Mock<IDataParameterCollection>();

            var mockEnumerator = new Mock<IEnumerator<SqlParameter>>();
            mockEnumerator.SetupGet(e => e.Current).Returns(new SqlParameter());
            mockEnumerator.SetupSequence(e => e.MoveNext())
                .Returns(true)
                .Returns(false);

            mockParams.Setup(p => p.GetEnumerator()).Returns(mockEnumerator.Object);

            mockParams.Setup(p => p[It.IsAny<int>()]).Returns((int index) =>
            {
                var param = new SqlParameter();
                switch (index)
                {
                    case 0:
                    case 1:
                        param.Value = messageTypeToReceive;
                        break;
                    case 2:
                        param.Value = MessageText;
                        break;
                    case 3:
                    case 4:
                        param.Value = Guid.Empty;
                        break;
                }

                return param as object;
            });

            mockParams.Setup(p => p.Add(It.IsAny<object>())).Callback((object obj) =>
            {
                var param = obj as SqlParameter;

                if (param.SqlDbType == SqlDbType.NVarChar && (param.Value == null || string.IsNullOrEmpty(param.Value.ToString())))
                    param.Value = "string";

                addedParams[param.ParameterName] = param;
            });

            var mockCommand = new Mock<IDbCommand>();
            mockCommand.Setup(c => c.Parameters).Returns(mockParams.Object);
            var mockTransaction = new Mock<IDbTransaction>();
            var mockConnection = new Mock<IDbConnection>();
            mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
            mockConnection.Setup(c => c.BeginTransaction()).Returns(mockTransaction.Object);
            mockTransaction.Setup(t => t.Connection).Returns(mockConnection.Object);

            return System.Tuple.Create(mockConnection, mockTransaction, mockCommand, mockParams, addedParams);
        }
    }

    [TestFixture]
    public class BrokerClientTests : ClientTests
    {
        [Test]
        public void Unsubscribing_RemovesSubscriberFromClient()
        {
            var subscription = AddSubscriber();

            Expect(_client.Subscribers.Count, EqualTo(1));

            subscription.Dispose();

            Expect(_client.Subscribers.Count, EqualTo(0));
        }

        [Test]
        public void Subscribing_SubscriberResubscribes_NotAddedAgain()
        {
            var subscriber = CreateObserver();

            _client.Subscribe(subscriber);
            _client.Subscribe(subscriber);
            _client.Subscribe(CreateObserver());

            Expect(_client.Subscribers.Count, EqualTo(2));
        }

        [Test]
        public void GettingSubscriberList_ReturnsCopy()
        {
            AddSubscriber();
            _client.Subscribers.Add(CreateObserver());

            Expect(_client.Subscribers.Count, EqualTo(1));
        }

        [Test]
        public void StopReceiving_NoErrors_ResultIsNotFailure_SubscribersCleared()
        {
            var mocks = CreateMocks();
            var mockLogger = new Mock<Log>();

            var client = new BrokerClient(
                () => mocks.Item1.Object,
                _mockOpenAsync.Object,
                _mockExeNonQueryAsync.Object,
                mockLogger.Object);

            var mockObserver1 = CreateMockObserver();
            mockObserver1.Setup(f => f.OnCompleted()).Throws<InvalidOperationException>();
            var mockObserver2 = CreateMockObserver();

            client.Subscribe(mockObserver1.Object);
            client.Subscribe(mockObserver2.Object);

            Expect(client.ReceiveMessages(QueueName).StopReceiving().Failed, True);
            Expect(client.Subscribers.Count, EqualTo(0));

            mockObserver1.Verify(o => o.OnCompleted(), Times.Once());
            mockObserver2.Verify(o => o.OnCompleted(), Times.Once());
            mockLogger.Verify(f => f(It.IsRegex("Receiver stopped"), It.IsAny<string>(), It.IsAny<TraceEventType>()), Times.Once());
        }

        [Test]
        public void StopReceiving_WithError_ResultIsFailure_SubscribersCleared()
        {
            Enumerable.Range(1, 2).Iter(i =>
            {
                var observer = CreateMockObserver();
                observer.Setup(o => o.OnCompleted()).Throws(new Exception(FailVal));
                _client.Subscribe(observer.Object);
            });

            var result = _client.ReceiveMessages(QueueName).StopReceiving();

            Expect(result.Failed, True);
            Expect(result.Exceptions.InnerExceptions.Count, EqualTo(2));
            result.Exceptions.Handle(e => 
            {
                Expect(e.Message, EqualTo(FailVal));
                return true;
            });
            Expect(_client.Subscribers.Count, EqualTo(0));
        }

        [Test]
        public void StopReceiving_WithErrorAndSuccess_ResultIsFailure_SubscribersCleared()
        {
            Enumerable.Range(1, 2).Iter(i => AddSubscriber());

            Enumerable.Range(1, 2).Iter(i =>
            {
                var observer = CreateMockObserver();
                observer.Setup(o => o.OnCompleted()).Throws(new Exception(FailVal));
                _client.Subscribe(observer.Object);
            });

            var result = _client.ReceiveMessages(QueueName).StopReceiving();

            Expect(result.Failed, True);
            Expect(result.Exceptions.InnerExceptions.Count, EqualTo(2));
            result.Exceptions.Handle(e =>
            {
                Expect(e.Message, EqualTo(FailVal));
                return true;
            });
            Expect(_client.Subscribers.Count, EqualTo(0));
        }
    }

    [TestFixture]
    public class ReceiveMessageTests : ClientTests
    {
        AutoResetEvent _wait;

        [SetUp]
        public void ThisSetup()
        {
            _wait = new AutoResetEvent(false);
        }

        void ExpectAll(Tuple<Mock<IDbConnection>, Mock<IDbTransaction>, Mock<IDbCommand>, Mock<IDataParameterCollection>, Dictionary<string, SqlParameter>> mocks = null)
        {
            ExpectEndDialog(mocks);
            ExpectReceiveMessage(mocks);
            ExpectAllDisposed(mocks);
        }

        void ExpectEndDialog(Tuple<Mock<IDbConnection>, Mock<IDbTransaction>, Mock<IDbCommand>, Mock<IDataParameterCollection>, Dictionary<string, SqlParameter>> mocks = null)
        {
            var mockCommand = mocks?.Item3 ?? _mockCommand;
            var mockParams = mocks?.Item4 ?? _mockParams;

            mockParams.Verify(p => p.Add(It.Is<object>(o => (o as SqlParameter).ParameterName == "@conversation")));
            mockCommand.VerifySet(c => c.CommandText = "END CONVERSATION @conversation");
        }

        void ExpectReceiveMessage(Tuple<Mock<IDbConnection>, Mock<IDbTransaction>, Mock<IDbCommand>, Mock<IDataParameterCollection>, Dictionary<string, SqlParameter>> mocks = null)
        {
            var mockCommand = mocks?.Item3 ?? _mockCommand;
            var mockParams = mocks?.Item4 ?? _mockParams;
            var addedParams = mocks?.Item5 ?? _addedParams;

            Expect(
                Enumerable.SequenceEqual(
                    addedParams.Select(p => p.Key),
                    Cons("@contract", "@messageType", "@message", "@conversationGroup", "@conversation")),
                True);

            Expect(
                addedParams.Values.Select(p => p.Direction).Distinct().First(),
                EqualTo(ParameterDirection.Output));

            mockCommand.VerifySet(c => c.CommandText = "WAITFOR (RECEIVE TOP(1) " +
                "@contract = service_contract_name, " +
                "@messageType = message_type_name, " +
                "@message = message_body, " +
                "@conversationGroup = conversation_group_id, " +
                "@conversation = conversation_handle " +
                $"FROM [{QueueName}]), TIMEOUT 5000;");
        }

        void ExpectAllDisposed(Tuple<Mock<IDbConnection>, Mock<IDbTransaction>, Mock<IDbCommand>, Mock<IDataParameterCollection>, Dictionary<string, SqlParameter>> mocks = null)
        {
            var mockConnection = mocks?.Item1 ?? _mockConnection;
            var mockTransaction = mocks?.Item2 ?? _mockTransaction;
            var mockCommand = mocks?.Item3 ?? _mockCommand;

            mockCommand.VerifySet(c => c.Transaction = mockTransaction.Object);
            mockTransaction.Verify(t => t.Commit());
            _mockOpenAsync.Verify(f => f(It.IsAny<IDbConnection>()));

            mockCommand.Verify(c => c.Dispose());
            mockTransaction.Verify(t => t.Dispose());
            mockConnection.Verify(c => c.Dispose());
        }

        [Test]
        public void ReceiveMessages_CommandCorrect_ObserversOnNextCalled()
        {
            var mocks = CreateMocks(MessageType);
            var mockConnection = mocks.Item1;
            var mockObserver = CreateMockObserver();

            mockObserver.Setup(o => o.OnNext(It.IsAny<BrokerMessage>()))
                .Callback(() => _wait.Set());

            var client = new BrokerClient(
                () => mockConnection.Object,
                _mockOpenAsync.Object, 
                _mockExeNonQueryAsync.Object);

            client.Subscribe(mockObserver.Object);
            var running = client.ReceiveMessages(QueueName);
            _wait.WaitOne();
            running.StopReceiving();

            mockObserver.Verify(o => o.OnNext(It.Is<BrokerMessage>(m => m.MessageType == MessageType)));

            ExpectAll(mocks);
        }

        [Test]
        public void ReceiveMessages_ObserverOnNextErrors_ObserversOnErrorsCalled()
        {
            var mockObserver = CreateMockObserver();
            mockObserver.SetupSequence(o => o.OnNext(It.IsAny<BrokerMessage>()))
                .Throws(new Exception(FailVal))
                .Pass();
            mockObserver.Setup(o => o.OnError(It.IsAny<Exception>(), It.IsAny<Maybe<BrokerMessage>>())).Callback(() => _wait.Set());

            _client.Subscribe(mockObserver.Object);
            var running = _client.ReceiveMessages("TestQueue");
            _wait.WaitOne();
            running.StopReceiving();

            mockObserver.Verify(o => o.OnError(It.Is<Exception>(e => e.Message == FailVal), It.IsAny<Maybe<BrokerMessage>>()));

            ExpectAll();
        }

        [Test]
        public void ReceiveMessages_SBErrorMessageReceived_ObserversOnErrorsCalled()
        {
            var mockObserver = CreateMockObserver();
            mockObserver.Setup(o => o.OnError(It.IsAny<Exception>(), It.IsAny<Maybe<BrokerMessage>>())).Callback(() => _wait.Set());

            var mocks = CreateMocks(ServiceBrokerErrorMessageType);
            var client = new BrokerClient(() => mocks.Item1.Object, _mockOpenAsync.Object, _mockExeNonQueryAsync.Object);

            client.Subscribe(mockObserver.Object);
            var running = client.ReceiveMessages(QueueName);
            _wait.WaitOne();
            running.StopReceiving();

            mockObserver.Verify(o => o.OnError(It.Is<Exception>(e => e.Message == MessageText), It.IsAny<Maybe<BrokerMessage>>()));

            ExpectAll(mocks);
        }

        [Test]
        public void ReceiveMessages_SBEndDialogMessageReceived_EndDialogCalled()
        {
            var mockObserver = CreateMockObserver();
            var mocks = CreateMocks(ServiceBrokerEndDialogMessageType);
            var mockConnection = mocks.Item1;
            var mockCommand = mocks.Item3;
            var mockParams = mocks.Item4;

            mockConnection.Setup(c => c.Dispose()).Callback(fun(() =>
            {
                var count = 0;

                return new Action(() =>
                {
                    if(++count == 2)
                        _wait.Set();
                });
            })());

            var client = new BrokerClient(() => mocks.Item1.Object, _mockOpenAsync.Object, _mockExeNonQueryAsync.Object);

            client.Subscribe(mockObserver.Object);
            var running = client.ReceiveMessages(QueueName);
            _wait.WaitOne();
            running.StopReceiving();

            ExpectEndDialog(mocks);
            ExpectAllDisposed(mocks);
        }

        [Test]
        public void ReceiveMessages_ObserverOnErrorErrors_ErrorLogged()
        {
            var mockLogger = new Mock<Log>();

            mockLogger
                .Setup(f => f(It.IsAny<string>(), GeneralLogCategory, It.Is<TraceEventType>(t => t == TraceEventType.Error)))
                .Callback(() => _wait.Set());

            var mocks = CreateMocks();
            var mockConnection = mocks.Item1;

            var client = new BrokerClient(
                () => mockConnection.Object, 
                _mockOpenAsync.Object, 
                _mockExeNonQueryAsync.Object, 
                mockLogger.Object);

            var mockObserver = CreateMockObserver();

            mockObserver.SetupSequence(o => o.OnNext(It.IsAny<BrokerMessage>()))
                .Throws(new Exception(FailVal))
                .Throws(new Exception($"{FailVal} 2"))
                .Pass();

            mockObserver.SetupSequence(o => o.OnError(It.IsAny<Exception>(), It.IsAny<Maybe<BrokerMessage>>()))
                .Pass()
                .Throws(new Exception("OnError Fail"));

            client.Subscribe(mockObserver.Object);
            var running = client.ReceiveMessages("TestQueue");
            _wait.WaitOne();
            running.StopReceiving();

            mockObserver.Verify(o => o.OnError(It.Is<Exception>(e => e.Message == FailVal), It.IsAny<Maybe<BrokerMessage>>()));
            mockLogger.Verify(l => l(It.IsRegex($"{FailVal} 2"), GeneralLogCategory, TraceEventType.Error));

            ExpectAll(mocks);
        }

        [Test]
        public void ReceiveMessages_OpenAsyncThrows_ObserversOnErrorCalled()
        {
            _mockOpenAsync.Setup(f => f(It.IsAny<IDbConnection>())).ThrowsAsync(new Exception(FailVal));

            var mockObserver = CreateMockObserver();
            mockObserver.Setup(o => o.OnError(It.IsAny<Exception>(), It.IsAny<Maybe<BrokerMessage>>()))
                .Callback(() => _wait.Set());

            _client.Subscribe(mockObserver.Object);
            var running = _client.ReceiveMessages("TestQueue");
            _wait.WaitOne();
            running.StopReceiving();

            mockObserver.Verify(o => o.OnError(It.Is<Exception>(m => m.Message == FailVal), It.IsAny<Maybe<BrokerMessage>>()));
        }

        [Test]
        public void ReceiveMessages_BeginTransactionThrows_ObserversOnErrorCalled()
        {
            _mockConnection.Setup(c => c.BeginTransaction()).Throws(new Exception(FailVal));

            var mockObserver = CreateMockObserver();
            mockObserver.Setup(o => o.OnError(It.IsAny<Exception>(), It.IsAny<Maybe<BrokerMessage>>()))
                .Callback(() => _wait.Set());

            _client.Subscribe(mockObserver.Object);
            var running = _client.ReceiveMessages("TestQueue");
            _wait.WaitOne();
            running.StopReceiving();

            mockObserver.Verify(o => o.OnError(It.Is<Exception>(m => m.Message == FailVal), It.IsAny<Maybe<BrokerMessage>>()));

            _mockConnection.Verify(c => c.Dispose());
        }

        [Test]
        public void ReceiveMessages_CreateCommandThrows_ObserversOnErrorCalled()
        {
            _mockConnection.Setup(c => c.CreateCommand()).Throws(new Exception(FailVal));

            var mockObserver = CreateMockObserver();
            mockObserver.Setup(o => o.OnError(It.IsAny<Exception>(), It.IsAny<Maybe<BrokerMessage>>()))
                .Callback(() => _wait.Set());

            _client.Subscribe(mockObserver.Object);
            var running = _client.ReceiveMessages("TestQueue");
            _wait.WaitOne();
            running.StopReceiving();

            mockObserver.Verify(o => o.OnError(It.Is<Exception>(m => m.Message == FailVal), It.IsAny<Maybe<BrokerMessage>>()));

            _mockConnection.Verify(c => c.Dispose());
            _mockTransaction.Verify(t => t.Dispose());
        }

        [Test]
        public void ReceiveMessages_ExeQueryErrors_ObserversOnErrorCalled()
        {
            var mockObserver = CreateMockObserver();

            _mockExeNonQueryAsync.Setup(f => f(It.IsAny<IDbCommand>())).Throws(new Exception(FailVal));

            mockObserver.Setup(o => o.OnError(It.IsAny<Exception>(), It.IsAny<Maybe<BrokerMessage>>()))
                .Callback(() => _wait.Set());

            _client.Subscribe(mockObserver.Object);
            var running = _client.ReceiveMessages("TestQueue");
            _wait.WaitOne();
            running.StopReceiving();

            mockObserver.Verify(o => o.OnError(It.Is<Exception>(m => m.Message == FailVal), It.IsAny<Maybe<BrokerMessage>>()));

            ExpectReceiveMessage();
            ExpectAllDisposed();
        }

        [Test]
        public void ReceiveMessages_ExeQueryFirstParamIsDBNull_ObserverOnNextNotCalled()
        {
            var mocks = CreateMocks();
            var mockConnection = mocks.Item1;
            var mockTransaction = mocks.Item2;
            var mockCommand = mocks.Item3;
            var mockParams = mocks.Item4;
            var mockObserver = CreateMockObserver();
            var client = new BrokerClient(() => mockConnection.Object, _mockOpenAsync.Object, _mockExeNonQueryAsync.Object);
            
            mockConnection.Setup(c => c.Dispose()).Callback(() => _wait.Set());
            mockParams.Setup(p => p[It.IsAny<int>()])
                .Callback(() => new SqlParameter("", DBNull.Value));

            client.Subscribe(mockObserver.Object);
            var running = client.ReceiveMessages(QueueName);
            _wait.WaitOne();
            running.StopReceiving();

            mockObserver.Verify(o => o.OnNext(It.Is<BrokerMessage>(msg => msg == BrokerMessage.Empty)), Times.Never());

            ExpectReceiveMessage(mocks);
            ExpectAllDisposed(mocks);
        }
    }
}