namespace NServiceBus.Transport.Sql.Shared
{
    using System;
    using System.Threading;

    /// <summary>
    /// Tracks the highest row version this receiver has consumed from its input queue so that
    /// receive queries can seek past the contended head of the queue index. The head accumulates
    /// other receivers' in-flight (locked, delete-pending) rows and ghost records of recently
    /// deleted rows; scanning over it makes every receive more expensive as more competing
    /// instances are added. Rows can become visible behind the anchor (committed late by a sender,
    /// or rolled back on another instance); each peek rewinds the anchor to the lowest visible row,
    /// and a periodic scan from the head every <c>headRescanInterval</c> acts as a backstop.
    /// </summary>
    class ReceiveAnchor
    {
        public ReceiveAnchor(TimeSpan headRescanInterval, TimeProvider timeProvider = null)
        {
            this.timeProvider = timeProvider ?? TimeProvider.System;
            this.headRescanInterval = headRescanInterval;
            lastHeadRescanTimestamp = this.timeProvider.GetTimestamp();
        }

        public long GetCurrent()
        {
            var lastRescan = Interlocked.Read(ref lastHeadRescanTimestamp);
            if (timeProvider.GetElapsedTime(lastRescan) >= headRescanInterval)
            {
                // only one caller wins the rescan slot; the others keep using the anchor
                if (Interlocked.CompareExchange(ref lastHeadRescanTimestamp, timeProvider.GetTimestamp(), lastRescan) == lastRescan)
                {
                    return 0;
                }
            }

            return Interlocked.Read(ref value);
        }

        public void Advance(long rowVersion)
        {
            var current = Interlocked.Read(ref value);
            while (rowVersion > current)
            {
                var witnessed = Interlocked.CompareExchange(ref value, rowVersion, current);
                if (witnessed == current)
                {
                    break;
                }

                current = witnessed;
            }
        }

        public void Reset() => Interlocked.Exchange(ref value, 0);

        /// <summary>
        /// Moves the anchor back so the next seek includes <paramref name="lowestVisible"/>, the lowest
        /// unlocked row the peek saw. Row versions are allocated on insert but become visible on
        /// commit, so a sender's long-running transaction can commit a row behind rows this
        /// receiver has already consumed.
        /// </summary>
        public void RewindToInclude(long lowestVisible)
        {
            if (lowestVisible <= 0)
            {
                return;
            }

            var target = lowestVisible - 1;
            var current = Interlocked.Read(ref value);
            while (current > target)
            {
                var witnessed = Interlocked.CompareExchange(ref value, target, current);
                if (witnessed == current)
                {
                    break;
                }

                current = witnessed;
            }
        }

        /// <summary>
        /// Gates the empty-receive fallback scan from the head of the queue. With wide processing
        /// concurrency, many receives can hit an empty anchored seek at the same moment; a single
        /// from-head probe settles whether the queue is really empty, so only the gate winner runs
        /// it and the rest report no message.
        /// </summary>
        public bool TryEnterHeadScan() => Interlocked.CompareExchange(ref headScanActive, 1, 0) == 0;

        public void ExitHeadScan() => Interlocked.Exchange(ref headScanActive, 0);

        readonly TimeProvider timeProvider;
        readonly TimeSpan headRescanInterval;
        long value;
        long lastHeadRescanTimestamp;
        int headScanActive;
    }
}
