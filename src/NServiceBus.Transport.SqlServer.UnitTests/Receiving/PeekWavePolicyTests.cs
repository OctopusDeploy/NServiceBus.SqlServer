namespace NServiceBus.Transport.SqlServer.UnitTests.Receiving;

using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Transport.Sql.Shared;
using NUnit.Framework;

public class PeekWavePolicyTests
{
    [Test]
    public async Task An_empty_peek_backs_off_and_skips_the_wave()
    {
        var peeker = new FakePeeker(PeekResult.Empty);
        var policy = CreatePolicy(peeker, new ReceiveState(anchoringEnabled: true));

        var size = await policy.NextWaveSize(256, CancellationToken).ConfigureAwait(false);

        Assert.Multiple(() =>
        {
            Assert.That(size, Is.Zero);
            Assert.That(peeker.Delays, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Starts_as_many_receives_as_the_peek_estimates_and_anchors_on_its_lowest_row()
    {
        var peeker = new FakePeeker(new PeekResult(700, 21));
        var state = new ReceiveState(anchoringEnabled: true);
        var policy = CreatePolicy(peeker, state);

        var size = await policy.NextWaveSize(256, CancellationToken).ConfigureAwait(false);

        Assert.Multiple(() =>
        {
            Assert.That(size, Is.EqualTo(700), "not capped at the concurrency limit");
            Assert.That(state.GetAnchor(), Is.EqualTo(ReceiveAnchor.Fast(20)));
            Assert.That(peeker.Delays, Is.Zero);
        });
    }

    [Test]
    public async Task Backs_off_only_after_a_wave_that_found_nothing()
    {
        var peeker = new FakePeeker(new PeekResult(3, 1));
        var policy = CreatePolicy(peeker, new ReceiveState(anchoringEnabled: true));

        await policy.WaveCompleted(3, 1, CancellationToken).ConfigureAwait(false);
        await policy.WaveCompleted(3, 0, CancellationToken).ConfigureAwait(false);

        Assert.That(peeker.Delays, Is.EqualTo(1));
    }

    static PeekWavePolicy CreatePolicy(FakePeeker peeker, ReceiveState state)
    {
        var policy = new PeekWavePolicy(peeker, state);
        policy.Start(new FakeQueue(), null);
        return policy;
    }

    static CancellationToken CancellationToken => TestContext.CurrentContext.CancellationToken;

    class FakePeeker(PeekResult result) : IPeekMessagesInQueue
    {
        public int Delays { get; private set; }

        public TimeSpan PeekDelay => TimeSpan.Zero;

        public Task<PeekResult> Peek(TableBasedQueue inputQueue, RepeatedFailuresOverTimeCircuitBreaker circuitBreaker, CancellationToken cancellationToken = default) =>
            Task.FromResult(result);

        public Task WaitForPeekDelay(CancellationToken cancellationToken = default)
        {
            Delays++;
            return Task.CompletedTask;
        }
    }

    class FakeQueue() : TableBasedQueue(new SqlServerConstants(), "[dbo].[queue]", "queue", false)
    {
        protected override Task SendRawMessage(MessageRow message, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }
}
