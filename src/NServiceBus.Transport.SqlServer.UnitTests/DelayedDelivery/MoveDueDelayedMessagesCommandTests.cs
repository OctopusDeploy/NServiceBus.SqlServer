namespace NServiceBus.Transport.SqlServer.UnitTests.DelayedDelivery;

using Microsoft.Data.SqlClient;
using NUnit.Framework;
using NServiceBus.Transport.Sql.Shared;

public class MoveDueDelayedMessagesCommandTests
{
    [Test]
    public void Populates_command_text_and_batch_size()
    {
        var sqlConstants = new SqlServerConstants();
        var moveDueCommand = new MoveDueDelayedMessagesCommand(sqlConstants, "[delayed]", "[input]");
        using var command = new SqlCommand();

        moveDueCommand.Populate(command, 42);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(command.CommandText, Is.EqualTo(string.Format(sqlConstants.MoveDueDelayedMessageText, "[delayed]", "[input]")));
            Assert.That(command.Parameters["BatchSize"].Value, Is.EqualTo(42));
            Assert.That(command.Parameters, Has.Count.EqualTo(1));
        }
    }
}
