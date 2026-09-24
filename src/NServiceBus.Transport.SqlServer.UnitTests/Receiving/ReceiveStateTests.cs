namespace NServiceBus.Transport.SqlServer.UnitTests.Receiving;

using System;
using Microsoft.Extensions.Time.Testing;
using NServiceBus.Transport.Sql.Shared;
using NUnit.Framework;

public class ReceiveStateTests
{
    [Test]
    public void Starts_at_the_head_of_the_queue()
    {
        var state = new ReceiveState(TimeSpan.FromSeconds(1), new FakeTimeProvider());

        Assert.That(state.GetAnchor(), Is.EqualTo(0));
    }

    [Test]
    public void Advances_monotonically()
    {
        var state = new ReceiveState(TimeSpan.FromSeconds(1), new FakeTimeProvider());

        state.AdvanceAnchor(10);
        state.AdvanceAnchor(5); // out-of-order completion of a concurrent receive must not move the anchor back

        Assert.That(state.GetAnchor(), Is.EqualTo(10));
    }

    [Test]
    public void Reset_moves_back_to_the_head()
    {
        var state = new ReceiveState(TimeSpan.FromSeconds(1), new FakeTimeProvider());

        state.AdvanceAnchor(10);
        state.ResetAnchor();

        Assert.That(state.GetAnchor(), Is.EqualTo(0));
    }

    [Test]
    public void Periodically_forces_a_scan_from_the_head()
    {
        var timeProvider = new FakeTimeProvider();
        var state = new ReceiveState(TimeSpan.FromSeconds(1), timeProvider);

        state.AdvanceAnchor(10);
        Assert.That(state.GetAnchor(), Is.EqualTo(10));

        timeProvider.Advance(TimeSpan.FromSeconds(1.5));

        Assert.Multiple(() =>
        {
            // one head rescan is due, subsequent receives resume from the anchor
            Assert.That(state.GetAnchor(), Is.EqualTo(0));
            Assert.That(state.GetAnchor(), Is.EqualTo(10));
        });
    }

    [Test]
    public void Advancing_does_not_postpone_the_head_rescan()
    {
        var timeProvider = new FakeTimeProvider();
        var state = new ReceiveState(TimeSpan.FromSeconds(1), timeProvider);

        for (var i = 1; i <= 4; i++)
        {
            state.AdvanceAnchor(i);
            timeProvider.Advance(TimeSpan.FromSeconds(0.4));
        }

        // 1.6s elapsed with continuous receives: a head rescan must still have become due
        Assert.That(state.GetAnchor(), Is.EqualTo(0));
    }

    [Test]
    public void Only_one_empty_fallback_head_scan_runs_at_a_time()
    {
        // With wide processing concurrency, many receives can hit an empty anchored seek at the
        // same moment; only one of them may pay the expensive from-head fallback scan.
        var state = new ReceiveState(TimeSpan.FromSeconds(1), new FakeTimeProvider());

        Assert.Multiple(() =>
        {
            Assert.That(state.TryEnterHeadScan(), Is.True, "first caller wins the gate");
            Assert.That(state.TryEnterHeadScan(), Is.False, "concurrent caller must not also scan");
        });

        state.ExitHeadScan();

        Assert.That(state.TryEnterHeadScan(), Is.True, "gate reopens after the scan completes");
    }

    [Test]
    public void First_batch_does_not_back_off()
    {
        var state = new ReceiveState(TimeSpan.FromSeconds(1), new FakeTimeProvider());

        Assert.That(state.BeginBatch(), Is.True);
    }

    [Test]
    public void Reports_whether_the_previous_batch_received_anything()
    {
        var state = new ReceiveState(TimeSpan.FromSeconds(1), new FakeTimeProvider());
        _ = state.BeginBatch();

        Assert.That(state.BeginBatch(), Is.False, "nothing received in the previous batch");

        state.MarkReceived();

        Assert.Multiple(() =>
        {
            Assert.That(state.BeginBatch(), Is.True, "previous batch received a message");
            Assert.That(state.BeginBatch(), Is.False, "starting a batch clears the flag");
        });
    }
}
