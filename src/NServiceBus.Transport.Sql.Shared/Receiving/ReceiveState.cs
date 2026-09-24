namespace NServiceBus.Transport.Sql.Shared
{
    using System;
    using System.Threading;

    /// <summary>
    /// Per-receiver state shared between the receive loop and the concurrent receives it starts.
    /// </summary>
    /// <remarks>
    /// The anchor is the highest row version this receiver has consumed from its input queue so
    /// that receive queries can seek past the contended head of the queue index. The head
    /// accumulates other receivers' in-flight (locked, delete-pending) rows and ghost records of
    /// recently deleted rows; scanning over it makes every receive more expensive as more
    /// competing instances are added. Periodically the anchor forces a scan from the head so
    /// messages that reappeared behind it (for example rolled back on another instance) are picked
    /// up within <c>headRescanInterval</c>.
    /// </remarks>
    class ReceiveState
    {
        public ReceiveState(TimeSpan headRescanInterval, TimeProvider timeProvider = null)
        {
            this.timeProvider = timeProvider ?? TimeProvider.System;
            this.headRescanInterval = headRescanInterval;
            lastHeadRescanTimestamp = this.timeProvider.GetTimestamp();
        }

        public long GetAnchor()
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

            return Interlocked.Read(ref anchor);
        }

        public void AdvanceAnchor(long rowVersion)
        {
            var current = Interlocked.Read(ref anchor);
            while (rowVersion > current)
            {
                var witnessed = Interlocked.CompareExchange(ref anchor, rowVersion, current);
                if (witnessed == current)
                {
                    break;
                }

                current = witnessed;
            }
        }

        public void ResetAnchor() => Interlocked.Exchange(ref anchor, 0);

        public void Apply(ProcessOutcome outcome)
        {
            switch (outcome.Kind)
            {
                case ProcessOutcomeKind.Committed:
                    AdvanceAnchor(outcome.RowVersion);
                    break;
                case ProcessOutcomeKind.RolledBack:
                    ResetAnchor();
                    break;
                case ProcessOutcomeKind.NoMessage:
                default:
                    break;
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

        public void MarkReceived() => Interlocked.Exchange(ref receivedInBatch, 1);

        /// <summary>
        /// Starts a new receive batch and reports whether the previous one received anything, so
        /// the receive loop can back off when a whole batch came up empty.
        /// </summary>
        public bool BeginBatch() => Interlocked.Exchange(ref receivedInBatch, 0) == 1;

        readonly TimeProvider timeProvider;
        readonly TimeSpan headRescanInterval;
        long anchor;
        long lastHeadRescanTimestamp;
        int headScanActive;
        // starts set so the first batch does not wait for the peek delay
        int receivedInBatch = 1;
    }
}
