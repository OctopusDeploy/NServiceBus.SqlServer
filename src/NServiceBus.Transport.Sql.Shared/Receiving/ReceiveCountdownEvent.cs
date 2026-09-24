namespace NServiceBus.Transport.Sql.Shared;

using System;
using System.Threading;
using System.Threading.Tasks;

class ReceiveCountdownEvent
{
    int count;
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

    public async Task WaitAsync(CancellationToken cancellationToken = default)
    {
        var registration = cancellationToken.Register(static state => ((TaskCompletionSource)state).TrySetResult(), completionSource);
        await using var _ = registration.ConfigureAwait(false);
        await completionSource.Task.ConfigureAwait(false);
    }

    public Signaler GetSignaler() => new(this);

    /// <summary>
    /// Accounts for receives that will not be started because the batch ended early, so waiting
    /// completes once the started ones have signalled.
    /// </summary>
    public void Skip(int skipped)
    {
        if (skipped > 0 && Interlocked.Add(ref count, -skipped) == 0)
        {
            _ = completionSource.TrySetResult();
        }
    }

    void Signal()
    {
        if (Interlocked.Decrement(ref count) == 0)
        {
            _ = completionSource.TrySetResult();
        }
    }

    public sealed class Signaler(ReceiveCountdownEvent parent) : IDisposable
    {
        bool signalled;

        public void Signal()
        {
            if (signalled)
            {
                return;
            }

            parent.Signal();
            signalled = true;
        }

        public void Dispose()
        {
            if (signalled)
            {
                return;
            }

            parent.Signal();
            signalled = true;
        }
    }
}