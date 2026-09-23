namespace NServiceBus.Transport.Sql.Shared
{
    using System.Threading;

    /// <summary>
    /// Tracks the highest row version this receiver has consumed from its input queue so that
    /// receive queries can seek past the contended head of the queue index. The head accumulates
    /// other receivers' in-flight (locked, delete-pending) rows and ghost records of recently
    /// deleted rows; scanning over it makes every receive more expensive as more competing
    /// instances are added.
    /// </summary>
    /// <remarks>
    /// Shared by all concurrent receives of one receiver and updated lock-free. Rows can become
    /// visible behind the anchor: rolled back locally (<see cref="Reset"/>), or committed late by a
    /// sender or rolled back on another instance (<see cref="RewindToInclude"/>, driven by each peek).
    /// A concurrent <see cref="Advance"/> can move the anchor past such a row again; the next peek
    /// rewinds it.
    /// </remarks>
    class ReceiveAnchor
    {
        public long Current => Interlocked.Read(ref value);

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

        long value;
    }
}
