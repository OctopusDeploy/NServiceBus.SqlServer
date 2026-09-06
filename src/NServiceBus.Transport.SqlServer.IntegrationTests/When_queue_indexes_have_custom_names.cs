namespace NServiceBus.Transport.SqlServer.IntegrationTests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using NServiceBus.Transport.Sql.Shared;
    using NUnit.Framework;
    using SqlServer;
    using Transport;

    // Regression test for the plan-pinning hint: installations that create the queue tables
    // themselves name the indexes differently (Octopus: IX_NSB_<Endpoint>_Row_Version), so the
    // hint must be resolved from the table's actual index names rather than hardcoded — a
    // hardcoded INDEX(Index_RowVersion) makes every receive throw SQL error 308.
    public class When_queue_indexes_have_custom_names
    {
        SqlServerConstants sqlConstants = new();

        [Test]
        public async Task Receives_work_and_resolve_the_hint_from_the_actual_index_name()
        {
            using (var connection = await dbConnectionFactory.OpenNewConnection())
            {
                await Send(connection, "m1");
                await Send(connection, "m2");

                var first = await queue.TryReceive(connection, null, 0);
                var second = await queue.TryReceive(connection, null, first.RowVersion);
                var empty = await queue.TryReceive(connection, null, second.RowVersion);
                var headRescan = await queue.TryReceive(connection, null, 0);

                Assert.Multiple(() =>
                {
                    Assert.That(first.Message.Headers[Headers.MessageId], Is.EqualTo("m1"));
                    Assert.That(second.Message.Headers[Headers.MessageId], Is.EqualTo("m2"));
                    Assert.That(empty.Successful, Is.False);
                    Assert.That(headRescan.Successful, Is.False);
                });
            }
        }

        async Task Send(System.Data.Common.DbConnection connection, string messageId, CancellationToken cancellationToken = default)
        {
            var headers = new System.Collections.Generic.Dictionary<string, string> { [Headers.MessageId] = messageId };
            var message = new OutgoingMessage(messageId, headers, new byte[0]);
            await queue.Send(message, TimeSpan.MaxValue, connection, null, cancellationToken);
        }

        [SetUp]
        public void Prepare()
        {
            PrepareAsync().GetAwaiter().GetResult();
        }

        async Task PrepareAsync(CancellationToken cancellationToken = default)
        {
            var addressTranslator = new QueueAddressTranslator("nservicebus", "dbo", null, new QueueSchemaAndCatalogOptions());

            var connectionString = Environment.GetEnvironmentVariable("SqlServerTransportConnectionString") ?? @"Data Source=.\SQLEXPRESS;Initial Catalog=nservicebus;Integrated Security=True;TrustServerCertificate=true";

            dbConnectionFactory = new SqlServerDbConnectionFactory(connectionString);

            var queueCreator = new QueueCreator(sqlConstants, dbConnectionFactory, addressTranslator.Parse);
            await queueCreator.CreateQueueIfNecessary(new[] { QueueName }, new CanonicalQueueAddress("Delayed", "dbo", "nservicebus"), cancellationToken);

            // rename the transport-created index to an Octopus-style name (idempotent)
            using (var connection = await dbConnectionFactory.OpenNewConnection(cancellationToken))
            using (var rename = connection.CreateCommand())
            {
                rename.CommandText = @"
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.CustomIndexNameTests') AND name = 'Index_RowVersion')
    EXEC sp_rename 'dbo.CustomIndexNameTests.Index_RowVersion', 'IX_NSB_CustomIndexNameTests_Row_Version', 'INDEX';";
                await rename.ExecuteNonQueryAsync(cancellationToken);
            }

            var queueAddress = addressTranslator.Parse(QueueName);
            queue = new SqlTableBasedQueue(sqlConstants, queueAddress, queueAddress.Address, true);

            var purger = new QueuePurger(dbConnectionFactory);
            await purger.Purge(queue, cancellationToken);
        }

        TableBasedQueue queue;
        SqlServerDbConnectionFactory dbConnectionFactory;

        const string QueueName = "CustomIndexNameTests";
    }
}
