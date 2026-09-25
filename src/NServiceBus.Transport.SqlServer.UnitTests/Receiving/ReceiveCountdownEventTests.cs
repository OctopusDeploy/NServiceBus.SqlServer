namespace NServiceBus.Transport.SqlServer.UnitTests.Receiving;

using System.Threading;
using NServiceBus.Transport.Sql.Shared;
using NUnit.Framework;

public class ReceiveCountdownEventTests
{
    [Test]
    public void Counts_the_receives_that_found_a_message()
    {
        var latch = new ReceiveCountdownEvent(3);

        latch.GetSignaler().Signal(messageFound: true);
        latch.GetSignaler().Signal(messageFound: false);
        latch.GetSignaler().Signal(messageFound: true);

        Assert.Multiple(() =>
        {
            Assert.That(latch.WaitAsync(CancellationToken).IsCompleted, Is.True);
            Assert.That(latch.MessagesFound, Is.EqualTo(2));
        });
    }

    [Test]
    public void Disposing_an_unsignalled_signaler_reports_no_message()
    {
        var latch = new ReceiveCountdownEvent(1);

        latch.GetSignaler().Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(latch.WaitAsync(CancellationToken).IsCompleted, Is.True);
            Assert.That(latch.MessagesFound, Is.Zero);
        });
    }

    [Test]
    public void A_signaler_reports_only_once()
    {
        var latch = new ReceiveCountdownEvent(2);
        var signaler = latch.GetSignaler();

        signaler.Signal(messageFound: true);
        signaler.Signal(messageFound: true);
        signaler.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(latch.WaitAsync(CancellationToken).IsCompleted, Is.False, "the other receive has not reported");
            Assert.That(latch.MessagesFound, Is.EqualTo(1));
        });
    }

    [Test]
    public void Skipping_the_receives_that_were_never_started_completes_the_wait()
    {
        var latch = new ReceiveCountdownEvent(3);

        latch.GetSignaler().Signal(messageFound: true);
        latch.Skip(2);

        Assert.Multiple(() =>
        {
            Assert.That(latch.WaitAsync(CancellationToken).IsCompleted, Is.True);
            Assert.That(latch.MessagesFound, Is.EqualTo(1));
        });
    }

    [Test]
    public void Skipping_does_not_complete_the_wait_while_a_started_receive_has_not_reported()
    {
        var latch = new ReceiveCountdownEvent(3);

        latch.Skip(2);

        Assert.That(latch.WaitAsync(CancellationToken).IsCompleted, Is.False);
    }

    static CancellationToken CancellationToken => TestContext.CurrentContext.CancellationToken;
}
