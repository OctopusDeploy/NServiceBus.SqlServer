namespace NServiceBus.Transport.SqlServer.UnitTests.Receiving;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using NServiceBus.Transport.Sql.Shared;
using NUnit.Framework;

public class RampedWavePolicyTests
{
    [Test]
    public async Task Probes_with_a_single_receive_first()
    {
        var policy = CreatePolicy();

        Assert.That(await policy.NextWaveSize(256, CancellationToken).ConfigureAwait(false), Is.EqualTo(1));
    }

    [Test]
    public async Task Doubles_while_every_receive_finds_a_message_up_to_the_cap()
    {
        var policy = CreatePolicy(maxWaveSize: 20);
        var sizes = new List<int>();

        for (var i = 0; i < 7; i++)
        {
            var size = await policy.NextWaveSize(256, CancellationToken).ConfigureAwait(false);
            sizes.Add(size);
            await policy.WaveCompleted(size, size, CancellationToken).ConfigureAwait(false);
        }

        Assert.That(sizes, Is.EqualTo(new[] { 1, 2, 4, 8, 16, 20, 20 }));
    }

    [Test]
    public async Task Is_capped_at_the_concurrency_limit()
    {
        var policy = CreatePolicy();
        await RampTo(policy, 16, CancellationToken).ConfigureAwait(false);

        Assert.That(await policy.NextWaveSize(5, CancellationToken).ConfigureAwait(false), Is.EqualTo(5));
    }

    [Test]
    public async Task Grows_from_what_a_capped_wave_found()
    {
        // a wave cut short (the concurrency limit, or a triggered circuit breaker) doubles from what it found
        var policy = CreatePolicy();
        await RampTo(policy, 16, CancellationToken).ConfigureAwait(false);

        await policy.WaveCompleted(1, 1, CancellationToken).ConfigureAwait(false);

        Assert.That(await policy.NextWaveSize(256, CancellationToken).ConfigureAwait(false), Is.EqualTo(2));
    }

    [Test]
    public async Task A_partial_wave_sizes_the_next_to_the_messages_found_without_backing_off()
    {
        var policy = CreatePolicy();
        await RampTo(policy, 8, CancellationToken).ConfigureAwait(false);

        var completed = policy.WaveCompleted(8, 3, CancellationToken);
        var next = await policy.NextWaveSize(256, CancellationToken).ConfigureAwait(false);

        Assert.Multiple(() =>
        {
            Assert.That(completed.IsCompleted, Is.True, "no backoff");
            Assert.That(next, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task An_empty_wave_backs_off_then_probes_with_a_single_receive_from_the_head()
    {
        var timeProvider = new FakeTimeProvider();
        var state = new ReceiveState(anchoringEnabled: true);
        state.AdvanceAnchor(50);
        var policy = CreatePolicy(state, timeProvider);
        await RampTo(policy, 4, CancellationToken).ConfigureAwait(false);

        var completed = policy.WaveCompleted(4, 0, CancellationToken);

        Assert.That(completed.IsCompleted, Is.False, "backing off");

        timeProvider.Advance(Backoff);
        await completed.ConfigureAwait(false);
        var next = await policy.NextWaveSize(256, CancellationToken).ConfigureAwait(false);

        Assert.Multiple(() =>
        {
            Assert.That(next, Is.EqualTo(1));
            Assert.That(state.GetAnchor(), Is.EqualTo(ReceiveAnchor.Sweep(0)), "the probe finds rows stranded behind the anchor");
        });
    }

    [Test]
    public async Task Starting_again_probes_with_a_single_receive()
    {
        var policy = CreatePolicy();
        await RampTo(policy, 8, CancellationToken).ConfigureAwait(false);

        policy.Start(null, null);

        Assert.That(await policy.NextWaveSize(256, CancellationToken).ConfigureAwait(false), Is.EqualTo(1));
    }

    static async Task RampTo(RampedWavePolicy policy, int waveSize, CancellationToken cancellationToken)
    {
        int size;
        while ((size = await policy.NextWaveSize(int.MaxValue, cancellationToken).ConfigureAwait(false)) < waveSize)
        {
            await policy.WaveCompleted(size, size, cancellationToken).ConfigureAwait(false);
        }
    }

    static RampedWavePolicy CreatePolicy(int maxWaveSize = 64) =>
        new(new ReceiveState(anchoringEnabled: true), Backoff, maxWaveSize, new FakeTimeProvider());

    static RampedWavePolicy CreatePolicy(ReceiveState state, TimeProvider timeProvider) =>
        new(state, Backoff, 64, timeProvider);

    static readonly TimeSpan Backoff = TimeSpan.FromSeconds(1);

    static CancellationToken CancellationToken => TestContext.CurrentContext.CancellationToken;
}
