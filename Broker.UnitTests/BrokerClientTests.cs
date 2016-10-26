using System;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Moq;
using Psns.Common.Clients.Broker;
using LanguageExt;
using static LanguageExt.Prelude;
using static Psns.Common.Clients.Broker.BrokerClient;
using static LanguageExt.List;

namespace Broker.UnitTests
{
    [TestFixture]
    public class OpenTests : AssertionHelper
    {
        [Test]
        public void OpenOk_ReturnsConnection()
        {
            var mockConnection = new Mock<IDbConnection>();

            var connection = Open(() => mockConnection.Object);

            Expect(connection, Is.EqualTo(mockConnection.Object));
        }

        [Test]
        public void OpenThrows_ReturnsError()
        {
            var mockConnection = new Mock<IDbConnection>();
            mockConnection.Setup(c => c.Open()).Throws(new Exception("error"));

            var connection = Open(() => mockConnection.Object);

            Expect(connection.Match(con => string.Empty, error => error), Does.Contain("error"));
        }
    }

    [TestFixture]
    public class BeginTransationTests : AssertionHelper
    {
        [Test]
        public void ConnectionBeginTransactionOk_ReturnsTransaction()
        {
            var mockConnection = new Mock<IDbConnection>();
            mockConnection.Setup(c => c.BeginTransaction()).Returns(new Mock<IDbTransaction>().Object);

            var transaction = BeginTransaction(Right<string, IDbConnection>(mockConnection.Object));

            Expect(transaction.IsRight, Is.True);
        }

        [Test]
        public void ConnectionBeginTransactionThrows_ReturnsError()
        {
            var mockConnection = new Mock<IDbConnection>();
            mockConnection.Setup(c => c.BeginTransaction()).Throws(new Exception("error"));

            var transaction = BeginTransaction(Right<string, IDbConnection>(mockConnection.Object));

            Expect(transaction.Match(trans => string.Empty, error => error), Does.Contain("error"));
        }

        [Test]
        public void ConnectionIsLeft_ReturnsError()
        {
            var transaction = BeginTransaction(Left<string, IDbConnection>("error"));

            Expect(transaction.Match(trans => string.Empty, error => error), Does.Contain("error"));
        }
    }

    [TestFixture]
    public class CreateCommandTests : AssertionHelper
    {
        [Test]
        public void ConnectionBeginTransactionFactoryIsRight_ReturnsCommand()
        {
            var mockCommand = new Mock<IDbCommand>();
            var mockConnection = new Mock<IDbConnection>();
            mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
            var mockTransaction = new Mock<IDbTransaction>();

            var command = CreateCommand(() => mockConnection.Object, Right<string, IDbTransaction>(mockTransaction.Object));

            Expect(command, EqualTo(mockCommand.Object));
            mockCommand.VerifySet(c => c.Transaction = mockTransaction.Object, Times.Once());
        }

        [Test]
        public void ConnectionBeginTransactionFactoryIsLeft_ReturnsError()
        {
            var mockCommand = new Mock<IDbCommand>();
            var mockConnection = new Mock<IDbConnection>();
            mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
            var mockTransaction = new Mock<IDbTransaction>();

            var command = CreateCommand(() => mockConnection.Object, Left<string, IDbTransaction>("error"));

            Expect(match(command, cmd => "cmd", error => error), EqualTo("error"));
            mockCommand.VerifySet(c => c.Transaction = mockTransaction.Object, Times.Never());
        }
    }

    [TestFixture]
    public class SendAsyncTests : AssertionHelper
    {
        Mock<IDbCommand> _mockCommand;
        Mock<IDataParameterCollection> _mockParams;
        List<SqlParameter> _addedParams;

        [SetUp]
        public void Setup()
        {
            _addedParams = new List<SqlParameter>();
            _mockParams = new Mock<IDataParameterCollection>();
            _mockCommand = new Mock<IDbCommand>();
            _mockCommand.Setup(c => c.Parameters).Returns(_mockParams.Object);
        }

        [Test]
        public async Task SendAsync_CommandIsRight_ReturnsError()
        {
            _mockParams.Setup(p => p.Add(It.IsAny<object>())).Callback((object obj) =>
            {
                var param = obj as SqlParameter;
                if(param.SqlDbType == SqlDbType.NVarChar)
                    param.Value = "string";
                else if(param.SqlDbType == SqlDbType.UniqueIdentifier)
                    param.Value = Guid.Empty;

                _addedParams.Add(param);
            });

            var unit = await SendAsync(
                () => Task.FromResult(1),
                new BrokerMessage("type", string.Empty, Guid.Empty, Guid.Empty),
                Right<string, IDbCommand>(_mockCommand.Object));

            Expect(unit, EqualTo(Unit.Default));

            _mockCommand.VerifySet(c => c.CommandText = "SEND ON CONVERSATION @conversation MESSAGE TYPE [type] (@message)", Times.Once());

            iter(new[] { "@message", "@conversation" }, name =>
            {
                Expect(exists(_addedParams, param => param.ParameterName == name), Is.True);
            });
        }

        [Test]
        public async Task SendAsync_CommandIsLeft_ReturnsUnit()
        {
            _mockParams.Setup(p => p.Add(It.IsAny<object>())).Callback((object obj) =>
            {
                var param = obj as SqlParameter;
                if(param.SqlDbType == SqlDbType.NVarChar)
                    param.Value = "string";
                else if(param.SqlDbType == SqlDbType.UniqueIdentifier)
                    param.Value = Guid.Empty;

                _addedParams.Add(param);
            });

            var result = await SendAsync(
                () => Task.FromResult(1),
                new BrokerMessage("type", string.Empty, Guid.Empty, Guid.Empty),
                Left<string, IDbCommand>("error"));

            Expect(match(from r in result select r, unit => string.Empty, error => error), Does.Contain("error"));
        }
    }

    [TestFixture]
    public class ReceiveAsyncTests : AssertionHelper
    {
        Mock<IDbCommand> _mockCommand;
        Mock<IDataParameterCollection> _mockParams;
        List<SqlParameter> _addedParams;

        [SetUp]
        public void Setup()
        {
            _addedParams = new List<SqlParameter>();
            _mockParams = new Mock<IDataParameterCollection>();
            _mockCommand = new Mock<IDbCommand>();
            _mockCommand.Setup(c => c.Parameters).Returns(_mockParams.Object);
        }

        [Test]
        public async Task ReceiveAsync_ShouldExecuteQueryAndReturnNonEmptyMessageWhenMessageHasValue()
        {
            _mockParams.Setup(p => p.Add(It.IsAny<object>())).Callback((object obj) =>
            {
                var param = obj as SqlParameter;
                if(param.SqlDbType == SqlDbType.NVarChar)
                    param.Value = "string";
                else if(param.SqlDbType == SqlDbType.UniqueIdentifier)
                    param.Value = Guid.Empty;

                _addedParams.Add(param);
            });

            var message = await ReceiveAsync(
                token => Task.FromResult(1),
                "queue",
                new CancellationTokenSource().Token,
                Right<string, IDbCommand>(_mockCommand.Object));

            Expect(message, EqualTo(new BrokerMessage("string", "string", Guid.Empty, Guid.Empty)));

            _mockCommand.VerifySet(c => c.CommandText = "WAITFOR (RECEIVE TOP(1) " +
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
        public async Task ReceiveAsync_ShouldExecuteQueryAndReturnEmptyMessageWhenMessageIsNull()
        {
            _mockParams.Setup(p => p.Add(It.IsAny<object>())).Callback((object obj) =>
            {
                var param = obj as SqlParameter;
                param.Value = DBNull.Value;

                _addedParams.Add(param);
            });

            var message = await ReceiveAsync(
                token => Task.FromResult(1),
                "queue",
                new CancellationTokenSource().Token,
                Right<string, IDbCommand>(_mockCommand.Object));

            Expect(message, EqualTo(BrokerMessage.Empty));

            _mockCommand.VerifySet(c => c.CommandText = "WAITFOR (RECEIVE TOP(1) " +
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
        public async Task ReceiveAsync_CommandIsLeft_ReturnsError()
        {
            var message = await ReceiveAsync(
                token => Task.FromResult(1),
                "queue",
                new CancellationTokenSource().Token,
                Left<string, IDbCommand>("error"));

            Expect(match(message, msg => string.Empty, error => error), Does.Contain("error"));
        }
    }

    [TestFixture]
    public class EndDialogAsyncTests : AssertionHelper
    {
        Mock<IDbCommand> _mockCommand;
        Mock<IDataParameterCollection> _mockParams;
        List<SqlParameter> _addedParams;

        [SetUp]
        public void Setup()
        {
            _addedParams = new List<SqlParameter>();
            _mockParams = new Mock<IDataParameterCollection>();
            _mockCommand = new Mock<IDbCommand>();
            _mockCommand.Setup(c => c.Parameters).Returns(_mockParams.Object);
        }

        [Test]
        public async Task CommandIsRight_ReturnsUnit()
        {
            _mockParams.Setup(p => p.Add(It.IsAny<object>())).Callback((object obj) =>
            {
                var param = obj as SqlParameter;
                if(param.SqlDbType == SqlDbType.UniqueIdentifier)
                    param.Value = Guid.Empty;

                _addedParams.Add(param);
            });

            var unit = await EndDialogAsync(
                () => Task.FromResult(1),
                Guid.Empty,
                Right<string, IDbCommand>(_mockCommand.Object));

            Expect(unit, EqualTo(Unit.Default));

            _mockCommand.VerifySet(c => c.CommandText = "END CONVERSATION @conversation", Times.Once());

            Expect(exists(_addedParams, param => param.ParameterName == "@conversation"), Is.True);
        }

        [Test]
        public async Task CommandIsLeft_ReturnsUnit()
        {
            _mockParams.Setup(p => p.Add(It.IsAny<object>())).Callback((object obj) =>
            {
                var param = obj as SqlParameter;
                if(param.SqlDbType == SqlDbType.UniqueIdentifier)
                    param.Value = Guid.Empty;

                _addedParams.Add(param);
            });

            var result = await EndDialogAsync(
                () => Task.FromResult(1),
                Guid.Empty,
                Left<string, IDbCommand>("error"));

            Expect(match(result, msg => string.Empty, error => error), Does.Contain("error"));
        }
    }

    [TestFixture]
    public class CommitTests : AssertionHelper
    {
        [Test]
        public void CommitOk_ReturnsUnit()
        {
            var unit = Commit(Right<string, IDbTransaction>(new Mock<IDbTransaction>().Object));

            Expect(unit, EqualTo(Unit.Default));
        }

        [Test]
        public void CommitThrows_ReturnsError()
        {
            var mockTransaction = new Mock<IDbTransaction>();
            mockTransaction.Setup(t => t.Commit()).Throws(new Exception("error"));

            var unit = Commit(Right<string, IDbTransaction>(mockTransaction.Object));

            Expect(match(unit, u => string.Empty, e => e), Does.Contain("error"));
        }

        [Test]
        public void TransactionIsLeft_ReturnsError()
        {
            var unit = Commit(Left<string, IDbTransaction>("error"));

            Expect(unit.Match(u => string.Empty, error => error), Does.Contain("error"));
        }
    }
}