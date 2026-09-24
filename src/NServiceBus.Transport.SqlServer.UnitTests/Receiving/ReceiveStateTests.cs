namespace NServiceBus.Transport.SqlServer.UnitTests.Receiving;

using NServiceBus.Transport.Sql.Shared;
using NUnit.Framework;

public class ReceiveStateTests
{
    [Test]
    public void Starts_at_the_head_of_the_queue()
    {
        var state = new ReceiveState();

        Assert.That(state.GetAnchor(), Is.EqualTo(0));
    }

    [Test]
    public void Advances_monotonically()
    {
        var state = new ReceiveState();

        state.AdvanceAnchor(10);
        state.AdvanceAnchor(5); // out-of-order completion of a concurrent receive must not move the anchor back

        Assert.That(state.GetAnchor(), Is.EqualTo(10));
    }

    [Test]
    public void Retreats_to_just_before_a_rolled_back_row()
    {
        var state = new ReceiveState();

        state.AdvanceAnchor(10);
        state.RetreatAnchor(7);

        // anchored receives seek strictly past the anchor, so row 7 is visible again
        Assert.That(state.GetAnchor(), Is.EqualTo(6));
    }

    [Test]
    public void Retreating_never_moves_the_anchor_forward()
    {
        var state = new ReceiveState();

        state.AdvanceAnchor(5);
        state.RetreatAnchor(10); // the rolled back row is already past the anchor

        Assert.That(state.GetAnchor(), Is.EqualTo(5));
    }

    [Test]
    public void Setting_before_the_lowest_available_row_moves_the_anchor_in_either_direction()
    {
        var state = new ReceiveState();

        state.SetAnchorBefore(20);
        Assert.That(state.GetAnchor(), Is.EqualTo(19), "forward past the churned head");

        state.SetAnchorBefore(7);
        Assert.That(state.GetAnchor(), Is.EqualTo(6), "back to a row that became visible behind the anchor");
    }

    [Test]
    public void First_batch_does_not_back_off()
    {
        var state = new ReceiveState();

        Assert.That(state.BeginBatch(), Is.True);
    }

    [Test]
    public void Reports_whether_the_previous_batch_received_anything()
    {
        var state = new ReceiveState();
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
