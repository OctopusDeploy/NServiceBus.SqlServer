namespace NServiceBus.Transport.SqlServer.UnitTests.Receiving;

using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using NServiceBus.Transport.Sql.Shared;
using NUnit.Framework;

public class ReceiveAttemptTests
{
    [Test]
    public async Task Signals_the_latch_and_stops_the_batch_when_nothing_was_received()
    {
        var latch = new ReceiveCountdownEvent(1);
        var stopBatch = new CancellationTokenSource();
        var state = new ReceiveState(TimeSpan.FromSeconds(1), new FakeTimeProvider());
        _ = state.BeginBatch();
        var attempt = new ReceiveAttempt(new FakeQueue(MessageReadResult.NoMessage), state, latch.GetSignaler(), stopBatch);

        _ = await attempt.Receive(null, null, CancellationToken).ConfigureAwait(false);

        Assert.Multiple(() =>
        {
            Assert.That(latch.WaitAsync(CancellationToken).IsCompleted, Is.True, "latch signalled");
            Assert.That(stopBatch.IsCancellationRequested, Is.True, "empty receive stops the batch");
            Assert.That(state.BeginBatch(), Is.False, "nothing received");
        });
    }

    [Test]
    public async Task Signals_the_latch_and_marks_the_batch_when_a_message_was_received()
    {
        var latch = new ReceiveCountdownEvent(1);
        var stopBatch = new CancellationTokenSource();
        var state = new ReceiveState(TimeSpan.FromSeconds(1), new FakeTimeProvider());
        _ = state.BeginBatch();
        var message = MessageReadResult.Success(new Message("1", string.Empty, Array.Empty<byte>(), false), 1);
        var attempt = new ReceiveAttempt(new FakeQueue(message), state, latch.GetSignaler(), stopBatch);

        _ = await attempt.Receive(null, null, CancellationToken).ConfigureAwait(false);

        Assert.Multiple(() =>
        {
            Assert.That(latch.WaitAsync(CancellationToken).IsCompleted, Is.True, "latch signalled");
            Assert.That(stopBatch.IsCancellationRequested, Is.False, "batch continues");
            Assert.That(state.BeginBatch(), Is.True, "message received");
        });
    }

    [Test]
    public async Task Receives_only_once()
    {
        var state = new ReceiveState(TimeSpan.FromSeconds(1), new FakeTimeProvider());
        var attempt = new ReceiveAttempt(new FakeQueue(MessageReadResult.NoMessage), state, new ReceiveCountdownEvent(1).GetSignaler(), new CancellationTokenSource());

        _ = await attempt.Receive(null, null, CancellationToken).ConfigureAwait(false);

        Assert.ThrowsAsync<InvalidOperationException>(() => attempt.Receive(null, null, CancellationToken));
    }

    static CancellationToken CancellationToken => TestContext.CurrentContext.CancellationToken;

    class FakeQueue(MessageReadResult result) : TableBasedQueue(new SqlServerConstants(), "[dbo].[queue]", "queue", false)
    {
        public override Task<MessageReadResult> TryReceive(DbConnection connection, DbTransaction transaction, long anchor, CancellationToken cancellationToken = default)
            => Task.FromResult(result);

        protected override Task SendRawMessage(MessageRow message, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }
}
