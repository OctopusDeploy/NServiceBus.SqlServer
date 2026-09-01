namespace NServiceBus.Transport.Sql.Shared;

using System;
using System.Threading;
using System.Threading.Tasks;

class ReceiveCountdownEvent
{
    int count;
    int messagesFound;
    readonly TaskCompletionSource completionSource;

    public ReceiveCountdownEvent(int count)
    {
        this.count = count;
        completionSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        if (count <= 0)
        {
            completionSource.SetResult();
        }
    }

    /// <summary>
    /// Number of receives in this batch that found a message, reported via
    /// <see cref="Signaler.Signal(bool)"/>. Read after <see cref="WaitAsync"/> completes.
    /// </summary>
    public int MessagesFound => Volatile.Read(ref messagesFound);

    public async Task WaitAsync(CancellationToken cancellationToken = default)
    {
        var registration = cancellationToken.Register(static state => ((TaskCompletionSource)state).TrySetResult(), completionSource);
        await using var _ = registration.ConfigureAwait(false);
        await completionSource.Task.ConfigureAwait(false);
    }

    public Signaler GetSignaler() => new(this);

    void Signal(bool messageFound)
    {
        if (messageFound)
        {
            _ = Interlocked.Increment(ref messagesFound);
        }

        if (Interlocked.Decrement(ref count) == 0)
        {
            _ = completionSource.TrySetResult();
        }
    }

    public sealed class Signaler(ReceiveCountdownEvent parent) : IDisposable
    {
        bool signalled;

        public void Signal() => Signal(false);

        public void Signal(bool messageFound)
        {
            if (signalled)
            {
                return;
            }

            parent.Signal(messageFound);
            signalled = true;
        }

        public void Dispose()
        {
            if (signalled)
            {
                return;
            }

            parent.Signal(false);
            signalled = true;
        }
    }
}
