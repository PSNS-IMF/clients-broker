using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using LanguageExt;
using Moq;
using NUnit.Framework;
using Psns.Common.Clients.Broker;
using static LanguageExt.List;
using static LanguageExt.Prelude;
using static Psns.Common.Clients.Broker.AppPrelude;

namespace Broker.UnitTests
{
    [TestFixture]
    public class OpenConnectionTests : AssertionHelper
    {
        Mock<IDbConnection> _mockConnection;

        [SetUp]
        public void Setup() => _mockConnection = new Mock<IDbConnection>();

        [Test]
        public void OpenConnection_CallsOpenOnConnection()
        {
            var connection = openConnection(_mockConnection.Object);

            _mockConnection.Verify(c => c.Open(), Times.Once());
        }

        [Test]
        public void OpenThrows_ReturnsError()
        {
            _mockConnection.Setup(c => c.Open()).Throws(new Exception("error"));

            var connection = openConnection(_mockConnection.Object);

            Expect(connection.Match(con => string.Empty, error => error.Message), Does.Contain("error"));
        }

        [Test]
        public void Dispose_ShouldDisposeOfConnection()
        {
            var mockTransaction = new Mock<IDbTransaction>();
            mockTransaction.Setup(t => t.Connection).Returns(_mockConnection.Object);

            var mockCommand = new Mock<IDbCommand>();
            var mockParams = new Mock<IDataParameterCollection>();
            mockCommand.Setup(c => c.Parameters).Returns(mockParams.Object);
            mockCommand.Setup(c => c.Connection).Returns(_mockConnection.Object);
            mockCommand.Setup(c => c.Transaction).Returns(mockTransaction.Object);

            _mockConnection.Setup(c => c.BeginTransaction()).Returns(mockTransaction.Object);
            _mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);

            var result = match(
                from conn in openConnection(_mockConnection.Object)
                from trans in beginTransaction(conn)
                from cmd in createCommandFactory(trans)
                from unit in endConversationAsync(s => { }, cmd, c => Task.FromResult(1), Guid.Empty).Result
                from ut in trans.Commit()
                select safe(() => dispose(conn)),
                u => u,
                err => err);

            Expect(result.IsRight, Is.True);
            _mockConnection.Verify(c => c.Dispose(), Times.Once());
        }
    }

    [TestFixture]
    public class TransactionTests : AssertionHelper
    {
        Mock<IDbConnection> _mockDbConnection;
        Mock<IDbTransaction> _mockTransaction;
        Either<Exception, OpenConnection> _connection;

        [SetUp]
        public void Setup()
        {
            _mockDbConnection = new Mock<IDbConnection>();
            _mockTransaction = new Mock<IDbTransaction>();
            _mockDbConnection.Setup(c => c.BeginTransaction()).Returns(_mockTransaction.Object);

            _connection = openConnection(_mockDbConnection.Object);
        }

        [Test]
        public void ConnectionBeginTransactionOk_ReturnsTransaction()
        {
            var transaction = match(
                from connection in _connection
                select AppPrelude.beginTransaction(connection),
                conn => conn,
                err => err);

            Expect(transaction.IsRight, Is.True);
        }

        [Test]
        public void ConnectionBeginTransactionThrows_ReturnsError()
        {
            _mockDbConnection.Setup(c => c.BeginTransaction()).Throws(new Exception("error"));

            var transaction = match(
                from connection in _connection
                select AppPrelude.beginTransaction(connection),
                Right: c => c,
                Left: err => err);

            Expect(match(transaction, trans => "", error => error.Message), Does.Contain("error"));
        }

        [Test]
        public void TransactionCommitOk_ReturnsUnit()
        {
            var unit = match(
                from connection in _connection
                from trans in beginTransaction(connection)
                select trans.Commit(),
                t => t,
                err => err);

            Expect(unit.IsRight, Is.True);
        }

        [Test]
        public void TransactionCommitThrows_ReturnsError()
        {
            _mockTransaction.Setup(t => t.Commit()).Throws(new Exception("error"));

            var unit = match(
                from connection in _connection
                from trans in beginTransaction(connection)
                select trans.Commit(),
                Right: u => u,
                Left: err => err);

            Expect(match(unit, u => string.Empty, e => e.Message), Does.Contain("error"));
        }
    }

    public class CommandTests : AssertionHelper
    {
        protected Mock<IDbConnection> _mockDbConnection;
        protected Mock<IDbTransaction> _mockTransaction;
        protected Mock<IDbCommand> _mockCommand;
        protected Mock<IDataParameterCollection> _mockParams;
        protected List<SqlParameter> _addedParams;
        protected Either<Exception, Func<Either<Exception, IDbCommand>>> _commandFactory;

        [SetUp]
        public void Setup()
        {
            _mockDbConnection = new Mock<IDbConnection>();
            _mockTransaction = new Mock<IDbTransaction>();
            _mockCommand = new Mock<IDbCommand>();
            _addedParams = new List<SqlParameter>();
            _mockParams = new Mock<IDataParameterCollection>();

            _mockParams.Setup(p => p.Add(It.IsAny<object>())).Callback((object obj) =>
            {
                var param = obj as SqlParameter;
                if(param.SqlDbType == SqlDbType.NVarChar && (param.Value == null || string.IsNullOrEmpty(param.Value.ToString())))
                    param.Value = "string";
                else if(param.SqlDbType == SqlDbType.UniqueIdentifier)
                    param.Value = Guid.Empty;

                _addedParams.Add(param);
            });
            _mockDbConnection.Setup(c => c.BeginTransaction()).Returns(_mockTransaction.Object);
            _mockDbConnection.Setup(c => c.CreateCommand()).Returns(_mockCommand.Object);
            _mockTransaction.Setup(t => t.Connection).Returns(_mockDbConnection.Object);
            _mockCommand.Setup(c => c.Parameters).Returns(_mockParams.Object);
            _mockCommand.Setup(c => c.Connection).Returns(_mockDbConnection.Object);
            _mockCommand.Setup(c => c.Transaction).Returns(_mockTransaction.Object);

            _commandFactory = match(
                from conn in openConnection(_mockDbConnection.Object)
                from trans in beginTransaction(conn)
                from factory in createCommandFactory(trans)
                select factory,
                c => Right<Exception, Func<Either<Exception, IDbCommand>>>(c),
                err => err);
        }
    }

    [TestFixture]
    public class DisposeCalled : CommandTests
    {
        [TearDown]
        public void TearDown() => _mockCommand.Verify(c => c.Dispose(), Times.Once());

        [Test]
        public async Task ConnectionBeginTransactionFactoryIsRight_ReturnsCommand()
        {
            var result = await matchAsync(
                endConversationAsync(s => { }, _commandFactory, c => Task.FromResult(1), Guid.Empty),
                right: c => Right<Exception, Unit>(c),
                left: err => Left<Exception, Unit>(err));

            Expect(result.IsRight, Is.True);
            _mockCommand.VerifySet(c => c.Transaction = _mockTransaction.Object, Times.Once());
            _mockCommand.Verify(c => c.Dispose(), Times.Once());
        }

        [Test]
        public async Task BeginConversationOk_ReturnsGuid()
        {
            var guid = Guid.NewGuid();

            _mockParams.Setup(p => p.Add(It.IsAny<object>())).Callback((object obj) =>
            {
                var param = obj as SqlParameter;
                if(param.ParameterName == "@conversation")
                    param.Value = guid;

                _addedParams.Add(param);
            });

            var result = await matchAsync(
                beginConversationAsync(s => { }, _commandFactory, c => Task.FromResult(1), "from", "to", "contract"),
                right: val => Right<Exception, Guid>(val),
                left: err => err);

            Expect(match(result, g => g.ToString(), err => "error"), EqualTo(guid.ToString()));

            _mockCommand.VerifySet(c => c.CommandText = "BEGIN DIALOG CONVERSATION @conversation " +
                                    "FROM SERVICE @fromService " +
                                    "TO SERVICE @toService " +
                                    "ON CONTRACT @contract " +
                                    "WITH ENCRYPTION = OFF;", Times.Once());

            iter(
                new[] { Tuple("@conversation", guid.ToString()), Tuple("@fromService", "from"), Tuple("@toService", "to"), Tuple("@contract", "contract") },
                msgConv => Expect(
                    exists(
                        _addedParams,
                        param => param.ParameterName == msgConv.Item1 && param.Value.ToString() == msgConv.Item2),
                    Is.True));

            Expect(_addedParams.Find(param => param.ParameterName == "@conversation").Direction, EqualTo(ParameterDirection.Output));
        }

        [Test]
        public async Task BeginConversationQueryThrows_ReturnsError()
        {
            var result = await matchAsync(
                beginConversationAsync(s => { }, _commandFactory, c => failwith<Task<int>>("error"), "from", "to", "contract"),
                right: val => Right<Exception, Guid>(val),
                left: err => err);

            Expect(match(result, u => string.Empty, error => error.Message), Does.Contain("error"));
        }

        [Test]
        public async Task SendAsyncQueryOk_ReturnsUnit()
        {
            _mockParams.Setup(p => p.Add(It.IsAny<object>())).Callback((object obj) =>
            {
                var param = obj as SqlParameter;
                _addedParams.Add(param);
            });

            var unit = await matchAsync(
                sendAsync(s => { }, _commandFactory, c => Task.FromResult(1), new BrokerMessage("contract", "type", "string", Guid.Empty, Guid.Empty)),
                right: val => Right<Exception, Unit>(val),
                left: err => err);

            Expect(unit.IsRight, Is.True);

            _mockCommand.VerifySet(c => c.CommandText = "SEND ON CONVERSATION @conversation MESSAGE TYPE @messageType (@message)", Times.Once());

            iter(
                new[] { Tuple("@message", "string"), Tuple("@messageType", "type"), Tuple("@conversation", Guid.Empty.ToString()) },
                msgConv => Expect(
                    exists(
                        _addedParams, 
                        param => param.ParameterName == msgConv.Item1 && param.Value.ToString() == msgConv.Item2), 
                    Is.True));

            Expect(
                exists(_addedParams, param => param.Size == 6 && param.SqlDbType == SqlDbType.NVarChar), 
                Is.True);
        }

        [Test]
        public async Task SendAsyncQueryThrows_ReturnsError()
        {
            var unit = await matchAsync(
                sendAsync(s => { }, _commandFactory, c => { throw new Exception("error"); }, BrokerMessage.Empty),
                right: val => Right<Exception, Unit>(val),
                left: err => err);

            Expect(match(from r in unit select r, u => string.Empty, error => error.Message), Does.Contain("error"));
        }

        [Test]
        public async Task ReceiveAsync_ShouldExecuteQueryAndReturnNonEmptyMessageWhenMessageHasValue()
        {
            _mockParams.Setup(p => p.Add(It.IsAny<object>())).Callback((object obj) =>
            {
                var param = obj as SqlParameter;

                if(param.ParameterName == "@contract")
                    param.Value = "contract";
                else if(param.ParameterName == "@messageType")
                    param.Value = DBNull.Value;
                else if(param.SqlDbType == SqlDbType.NVarChar && (param.Value == null || string.IsNullOrEmpty(param.Value.ToString())))
                    param.Value = "string";
                else if(param.SqlDbType == SqlDbType.UniqueIdentifier)
                    param.Value = Guid.Empty;

                _addedParams.Add(param);
            });

            var message = await matchAsync(
                receiveAsync(s => { }, _commandFactory, cmd => Task.FromResult(1), "queue"),
                right: val => Right<Exception, BrokerMessage>(val),
                left: err => err);

            Expect(message, EqualTo(new BrokerMessage("contract", string.Empty, "string", Guid.Empty, Guid.Empty)));

            _mockCommand.VerifySet(c => c.CommandText = "WAITFOR (RECEIVE TOP(1) " +
                "@contract = service_contract_name, " +
                "@messageType = message_type_name, " +
                "@message = message_body, " +
                "@conversationGroup = conversation_group_id, " +
                "@conversation = conversation_handle " +
                "FROM [queue]), TIMEOUT 5000;", Times.Once());

            iter(new[] { "@contract", "@messageType", "@message", "@conversationGroup", "@conversation" }, name =>
            {
                Expect(exists(_addedParams, param => param.ParameterName == name), Is.True);
            });
        }

        [Test]
        public async Task ReceiveAsync_ShouldExecuteQueryAndReturnEmptyMessageWhenMessageIsNull()
        {
            _mockParams.Setup(p => p.Add(It.IsAny<object>())).Callback((object obj) =>
            {
                var param = obj as SqlParameter;
                param.Value = DBNull.Value;

                _addedParams.Add(param);
            });

            var message = await matchAsync(
                receiveAsync(s => { }, _commandFactory, cmd => Task.FromResult(1), "queue"),
                right: val => Right<Exception, BrokerMessage>(val),
                left: err => err);

            Expect(message, EqualTo(BrokerMessage.Empty));

            _mockCommand.VerifySet(c => c.CommandText = "WAITFOR (RECEIVE TOP(1) " +
                "@contract = service_contract_name, " +
                "@messageType = message_type_name, " +
                "@message = message_body, " +
                "@conversationGroup = conversation_group_id, " +
                "@conversation = conversation_handle " +
                "FROM [queue]), TIMEOUT 5000;", Times.Once());

            iter(new[] { "@messageType", "@message", "@conversationGroup", "@conversation" }, name =>
            {
                Expect(exists(_addedParams, param => param.ParameterName == name), Is.True);
            });
        }

        [Test]
        public async Task ReceiveAsyncQueryThrows_ReturnsError()
        {
            var message = await matchAsync(
                receiveAsync(s => { }, _commandFactory, cmd => { throw new Exception("error"); }, "queue"),
                right: val => Right<Exception, BrokerMessage>(val),
                left: err => err);

            Expect(match(from r in message select r, u => string.Empty, error => error.Message), Does.Contain("error"));
        }

        [Test]
        public async Task EndConversationQueryOk_ReturnsUnit()
        {
            var unit = await matchAsync(
                endConversationAsync(s => { }, _commandFactory, c => Task.FromResult(1), Guid.Empty),
                u => Right<Exception, Unit>(u),
                err => err);

            Expect(unit.IsRight, Is.True);

            _mockCommand.VerifySet(c => c.CommandText = "END CONVERSATION @conversation", Times.Once());

            Expect(exists(_addedParams, param => param.ParameterName == "@conversation" && param.Value.ToString() == Guid.Empty.ToString()), Is.True);
        }

        [Test]
        public async Task EndConversationQueryThrows_ReturnsError()
        {
            var unit = await matchAsync(
                endConversationAsync(s => { }, _commandFactory, c => { throw new Exception("error"); }, Guid.Empty),
                u => Right<Exception, Unit>(u),
                err => err);

            Expect(match(unit, msg => string.Empty, error => error.Message), Does.Contain("error"));
        }
    }

    [TestFixture]
    public class DisposeNotCalled : CommandTests
    {
        [Test]
        public async Task ConnectionBeginTransactionFactoryIsLeft_ReturnsError()
        {
            _mockDbConnection.Setup(c => c.CreateCommand()).Throws(new Exception("error"));

            var result = await matchAsync(
                from conn in openConnection(_mockDbConnection.Object)
                from trans in beginTransaction(conn)
                from command in createCommandFactory(trans)
                select endConversationAsync(s => { }, _commandFactory, c => Task.FromResult(1), Guid.Empty),
                c => c,
                err => err);

            Expect(match(result, cmd => "cmd", error => error.Message), Does.Contain("error"));
        }
    }

    [TestFixture]
    public class MultiCalls : CommandTests
    {
        [Test]
        public void SendAndEndConversationQueryOk_CreateCommandCalledTwiceReturnsUnit()
        {
            var unit = match(
                from sendResult in sendAsync(s => { }, _commandFactory, cmd => Task.FromResult(1), BrokerMessage.Empty).Result
                from endResult in endConversationAsync(s => { }, _commandFactory, c => Task.FromResult(1), Guid.Empty).Result
                select endResult,
                u => Right<Exception, Unit>(u),
                err => err);

            Expect(unit.IsRight, Is.True);

            _mockDbConnection.Verify(c => c.CreateCommand(), Times.Exactly(2));
            _mockCommand.Verify(cmd => cmd.Dispose(), Times.Exactly(2));
        }
    }
}