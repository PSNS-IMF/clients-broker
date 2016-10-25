using System;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Moq;
using Psns.Common.Clients.Broker;
using static Psns.Common.Clients.Broker.BrokerClient;
using static LanguageExt.List;

namespace Broker.UnitTests
{
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

        [TearDown]
        public void Teardown()
        {
            Expect(_addedParams, Has.Count.EqualTo(4));

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

            var message = await ReceiveAsync("queue", new CancellationTokenSource().Token, () => _mockCommand.Object, token => Task.FromResult(1));

            Expect(message, EqualTo(new BrokerMessage("string", "string", Guid.Empty, Guid.Empty)));
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

            var message = await ReceiveAsync("queue", new CancellationTokenSource().Token, () => _mockCommand.Object, token => Task.FromResult(1));

            Expect(message, EqualTo(BrokerMessage.Empty));
        }
    }
}