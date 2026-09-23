namespace NServiceBus.Transport.SqlServer.UnitTests.Receiving;

using NServiceBus.Transport.Sql.Shared;
using NUnit.Framework;

public class ReceiveAnchorTests
{
    [Test]
    public void Starts_at_the_head_of_the_queue()
    {
        var anchor = new ReceiveAnchor();

        Assert.That(anchor.Current, Is.EqualTo(0));
    }

    [Test]
    public void Advances_monotonically()
    {
        var anchor = new ReceiveAnchor();

        anchor.Advance(10);
        anchor.Advance(5); // out-of-order completion of a concurrent receive must not move the anchor back

        Assert.That(anchor.Current, Is.EqualTo(10));
    }

    [Test]
    public void Rewinds_to_include_a_lower_visible_row()
    {
        var anchor = new ReceiveAnchor();

        anchor.Advance(10);
        anchor.RewindToInclude(7);

        Assert.That(anchor.Current, Is.EqualTo(6));
    }

    [TestCase(0)]
    [TestCase(11)]
    public void Does_not_move_forward_on_rewind(long lowestVisible)
    {
        var anchor = new ReceiveAnchor();

        anchor.Advance(10);
        anchor.RewindToInclude(lowestVisible);

        Assert.That(anchor.Current, Is.EqualTo(10));
    }

    [Test]
    public void Reset_moves_back_to_the_head()
    {
        var anchor = new ReceiveAnchor();

        anchor.Advance(10);
        anchor.Reset();

        Assert.That(anchor.Current, Is.EqualTo(0));
    }
}
