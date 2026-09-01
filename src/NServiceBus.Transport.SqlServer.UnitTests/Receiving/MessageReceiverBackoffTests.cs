namespace NServiceBus.Transport.SqlServer.UnitTests.Receiving;

using System;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Extensibility;
using NServiceBus.Transport;
using NServiceBus.Transport.Sql.Shared;
using NServiceBus.Unicast.Messages;
using NUnit.Framework;

public class MessageReceiverBackoffTests
{
    [Test]
    public async Task Probes_an_empty_queue_with_single_backed_off_receives()
    {
        // Simulates a node in a multi-node (competing consumer) setup whose queue is empty (or
        // whose messages keep being won by other nodes): every receive finds nothing. The pump
        // must throttle to one probe receive per backoff interval instead of fanning out
        // concurrency-wide waves in a hot loop.
        var strategy = new CountingEmptyReceiveStrategy();
        var receiver = CreateReceiver(strategy, emptyBatchBackoff: TimeSpan.FromMilliseconds(200));

        await receiver.Initialize(new PushRuntimeSettings(8), (_, _) => Task.CompletedTask, (_, _) => Task.FromResult(ErrorHandleResult.Handled)).ConfigureAwait(false);
        await receiver.StartReceive().ConfigureAwait(false);
        await Task.Delay(TimeSpan.FromMilliseconds(700)).ConfigureAwait(false);
        await receiver.StopReceive().ConfigureAwait(false);

        // ~700ms with a 200ms backoff allows a handful of single-receive probes; an unthrottled
        // pump reaches thousands of receive attempts.
        Assert.That(strategy.ReceiveAttempts, Is.LessThanOrEqualTo(8));
    }

    static MessageReceiver CreateReceiver(ProcessStrategy strategy, TimeSpan emptyBatchBackoff)
    {
        var queue = new FakeQueue();
        var classifier = new SqlServerExceptionClassifier();

        return new MessageReceiver(
            new SqlServerTransport("Server=unused;Trusted_Connection=True"),
            "receiver",
            "queue",
            "error",
            (_, _, _) => { },
            _ => strategy,
            _ => queue,
            new FakePurger(),
            TimeSpan.FromSeconds(30),
            emptyBatchBackoff,
            new FakeSubscriptionManager(),
            false,
            classifier);
    }

    class CountingEmptyReceiveStrategy : ProcessStrategy
    {
        int receiveAttempts;

        public CountingEmptyReceiveStrategy()
            : base(null, new SqlServerExceptionClassifier(), null)
        {
        }

        public int ReceiveAttempts => Volatile.Read(ref receiveAttempts);

        public override Task ProcessMessage(CancellationTokenSource stopBatchCancellationTokenSource, ReceiveCountdownEvent.Signaler receiveCountdownEventSignaler, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref receiveAttempts);
            receiveCountdownEventSignaler.Signal(false);
            stopBatchCancellationTokenSource.Cancel();
            return Task.CompletedTask;
        }
    }

    class FakeQueue : TableBasedQueue
    {
        public FakeQueue() : base(new SqlServerConstants(), "[dbo].[queue]", "queue", false)
        {
        }

        protected override Task SendRawMessage(MessageRow message, System.Data.Common.DbConnection connection, System.Data.Common.DbTransaction transaction, CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }

    class FakePurger : IPurgeQueues
    {
        public Task<int> Purge(TableBasedQueue queue, CancellationToken cancellationToken = default) => Task.FromResult(0);
    }

    class FakeSubscriptionManager : ISubscriptionManager
    {
        public Task SubscribeAll(MessageMetadata[] eventTypes, ContextBag context, CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task Unsubscribe(MessageMetadata eventType, ContextBag context, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }
}
