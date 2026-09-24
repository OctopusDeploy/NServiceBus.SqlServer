namespace NServiceBus.Transport.SqlServer.UnitTests.Receiving;

using System.Threading;
using NServiceBus.Transport.Sql.Shared;
using NUnit.Framework;

public class ReceiveCountdownEventTests
{
    [Test]
    public void Completes_once_the_started_receives_signal_after_the_rest_are_skipped()
    {
        var latch = new ReceiveCountdownEvent(3);
        latch.Skip(2);

        Assert.That(latch.WaitAsync(CancellationToken).IsCompleted, Is.False, "the started receive has not signalled");

        latch.GetSignaler().Signal();

        Assert.That(latch.WaitAsync(CancellationToken).IsCompleted, Is.True);
    }

    [Test]
    public void Completes_when_skipping_after_the_started_receives_signalled()
    {
        var latch = new ReceiveCountdownEvent(3);
        latch.GetSignaler().Signal();

        latch.Skip(2);

        Assert.That(latch.WaitAsync(CancellationToken).IsCompleted, Is.True);
    }

    static CancellationToken CancellationToken => TestContext.CurrentContext.CancellationToken;
}
