namespace NServiceBus.Transport.Sql.Shared
{
    using System.Threading;

    /// <summary>
    /// Per-receiver state shared between the receive loop and the concurrent receives it starts.
    /// </summary>
    /// <remarks>
    /// The anchor lets receive queries seek past the contended head of the queue index. The head
    /// accumulates other receivers' in-flight (locked, delete-pending) rows and ghost records of
    /// recently deleted rows; scanning over it makes every receive more expensive as more
    /// competing instances are added. Each batch starts the anchor just before the lowest row the
    /// peek found available, so messages that reappeared behind it (for example rolled back on
    /// another instance) are picked up by the next batch.
    /// </remarks>
    class ReceiveState
    {
        public long GetAnchor() => Interlocked.Read(ref anchor);

        /// <summary>
        /// Moves the anchor to just before the lowest available row, in either direction
        /// </summary>
        public void SetAnchorBefore(long lowestRowVersion) => Interlocked.Exchange(ref anchor, lowestRowVersion - 1);

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

        /// <summary>
        /// Moves the anchor back to just before a row that rolled back, so we rescan a failed row
        /// </summary>
        public void RetreatAnchor(long rowVersion)
        {
            var target = rowVersion - 1;
            var current = Interlocked.Read(ref anchor);
            while (target < current)
            {
                var witnessed = Interlocked.CompareExchange(ref anchor, target, current);
                if (witnessed == current)
                {
                    break;
                }

                current = witnessed;
            }
        }

        /// <summary>
        /// Starts a new "receive" batch and returns true if the previous batch received anything
        /// </summary>
        public bool BeginBatch() => Interlocked.Exchange(ref receivedInBatch, 0) == 1;
        public void MarkReceived() => Interlocked.Exchange(ref receivedInBatch, 1);

        long anchor;
        // starts set so the first batch does not wait for the peek delay
        int receivedInBatch = 1;
    }
}
