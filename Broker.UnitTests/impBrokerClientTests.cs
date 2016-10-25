using System;
using System.Data.Common;
using System.Threading;
using NUnit.Framework;
using Moq;
using Psns.Common.Clients.Broker;

namespace Broker.UnitTests
{
    [TestFixture]
    public class ImpBrokerClientTests : AssertionHelper
    {
        ImpBrokerClient _client;
        Mock<DbConnection> _mockDbConnection;
        Mock<DbTransaction> _mockDbTransaction;
        Mock<DbCommand> _mockDbCommand;

        [SetUp]
        public void Setup()
        {
            _mockDbTransaction = new Mock<DbTransaction>();
            _mockDbCommand = new Mock<DbCommand>();
            _mockDbConnection = new Mock<DbConnection>();
            _mockDbConnection.Setup(c => c.BeginTransaction()).Returns(_mockDbTransaction.Object);
            _mockDbConnection.Setup(c => c.CreateCommand()).Returns(_mockDbCommand.Object);

            _client = new ImpBrokerClient(_mockDbConnection.Object);
        }

        [Test]
        public void BeginTransaction_ShouldCallConnectionBeginTransactionAndReturnOk()
        {
            Assert.That(_client.BeginTransaction().IsSuccess, Is.True);

            _mockDbConnection.Verify(c => c.BeginTransaction(), Times.Once());
        }

        [Test]
        public void BeginTransaction_ConnectionBeginTransactionThrows_ShouldReturnFailure()
        {
            _mockDbConnection.Setup(c => c.BeginTransaction()).Throws(new Exception("error"));

            Assert.That(_client.BeginTransaction().IsFailure, Is.True);
        }

        [Test]
        public void Open_ShouldCallConnectionOpenAndReturnOk()
        {
            Assert.That(_client.Open().IsSuccess, Is.True);

            _mockDbConnection.Verify(c => c.Open(), Times.Once());
        }

        [Test]
        public void Open_ConnectionOpenThrows_ShouldReturnFailure()
        {
            _mockDbConnection.Setup(c => c.Open()).Throws(new Exception("error"));

            Assert.That(_client.Open().IsFailure, Is.True);
        }

        [Test]
        public void ReceiveAsync_ShouldCallConnectionReceiveAsyncAndReturnOk()
        {
            Assert.That(_client.ReceiveAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()).Result.IsSuccess, Is.True);

            _mockDbCommand.Verify(c => c.ExecuteNonQueryAsync(It.IsAny<CancellationToken>()), Times.Once());
        }

        [Test]
        public void ReceiveAsync_ConnectionReceiveAsyncThrows_ShouldReturnFailure()
        {
            _mockDbConnection.Setup(c => c.Open()).Throws(new Exception("error"));

            Assert.That(_client.Open().IsFailure, Is.True);
        }
    }
}
