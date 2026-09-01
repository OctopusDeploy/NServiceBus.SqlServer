namespace NServiceBus.Transport.SqlServer
{
    using System.Text;
    using NServiceBus.Transport.Sql.Shared;

    /// <summary>
    /// Identification and live diagnostics for the Octopus performance patch
    /// (Octopus.NServiceBus.Transport.SqlServer). Log <see cref="Describe"/> at startup (or any
    /// time) to confirm which patch version the process actually loaded and what the receive
    /// pump is doing. The output is guaranteed to stay under 4000 characters.
    /// </summary>
    public static class SqlServerTransportPatch
    {
        /// <summary>
        /// The patch version baked into this assembly.
        /// </summary>
        public static string Version => "9.0.1-anchored-receive.6";

        /// <summary>
        /// What this patch changes relative to the official NServiceBus.Transport.SqlServer 9.0.1.
        /// </summary>
        public static string Summary =>
            "Octopus performance patch for NServiceBus.Transport.SqlServer 9.0.1 (branch sql-fix-9.0.1). " +
            "Fixes DB load growing superlinearly with the number of competing-consumer instances. " +
            "(.1) ANCHORED RECEIVE: each receiver tracks the highest RowVersion it consumed and dequeues " +
            "WHERE RowVersion > @Anchor - an index seek past the contended head of the queue index " +
            "(other instances' locked in-flight rows + ghost records of recent deletes) instead of a scan " +
            "through it. Safety: an empty anchored receive falls back to one from-head receive before the " +
            "queue is treated as empty; the anchor resets on local rollback so immediate retries still work; " +
            "a periodic from-head rescan bounds how long a message rolled back on ANOTHER instance can wait. " +
            "(.2) The rescan interval is floored at 1s so an aggressive peek delay (Octopus: 100ms) cannot " +
            "turn the rescan back into a hot head-scan loop. " +
            "(.3) The empty-receive fallback head scan is gated to ONE prober per receiver at a time - with " +
            "wide concurrency (64 x cores) hundreds of simultaneous empty seeks previously each paid a full " +
            "head scan, which measured WORSE than the unpatched transport in shallow-queue steady state. " +
            "(.4) The peek query is REMOVED (it no longer runs at all). Its max-min backlog estimate counts " +
            "gaps left by in-flight rows and wildly over-reports under competing consumers, causing huge " +
            "waves of empty receives. The pump now probes with real receives and adapts: one probe after " +
            "idle/empty; the wave doubles while fully successful; a partial wave sets the next wave to the " +
            "observed availability (no backoff, so a busy node keeps up); a fully empty wave resets to one " +
            "probe plus the backoff (QueuePeekerOptions.Delay). Waves are capped at 64 per instance. " +
            "(.5) This diagnostics facade. " +
            "(.6) DELAYED-MOVER ELECTION: only one instance at a time moves due delayed messages " +
            "(sp_getapplock, transaction-owned, per delayed table; SQL Server only). Previously every " +
            "instance polled and moved from the same delayed table, scanning the matured head of the [Due] " +
            "index past each other's locked batches - the receive-path contention pattern all over again, " +
            "painful on endpoints with many delayed messages. Losers skip the table without touching it and " +
            "re-check in ~0.9s; delayedMoverWon/Skipped counters show the election working. " +
            "Measured (SQL Server 2022, concurrency 256, 100ms peek delay, 50ms handler, DB CPU ms/msg at " +
            "1/6/12 nodes): steady 400 msg/s 2.86/3.00/3.32 vs unpatched 3.49/5.88/6.69; drain of a 20k " +
            "backlog at 12 nodes 2.13 vs 6.28 at equal throughput, receive p99 278ms -> 16ms. " +
            "Trade-offs: cross-node redelivery of a rolled-back message is bounded by the rescan interval " +
            "(~1s) instead of immediate; same-node retries unaffected; best-effort FIFO slightly coarser; " +
            "receive ramps 1->2->4->...->64 over a few sub-second waves after idle instead of jumping to the " +
            "peek estimate.";

        /// <summary>
        /// Version, change summary and a live counter snapshot, for logging.
        /// </summary>
        public static string Describe()
        {
            var builder = new StringBuilder(4000);
            builder.Append("[SqlServerTransportPatch ").Append(Version).Append("] ");
            TransportPatchDiagnostics.AppendSnapshot(builder);
            builder.Append(" | ").Append(Summary);

            if (builder.Length > 3999)
            {
                builder.Length = 3999;
            }

            return builder.ToString();
        }
    }
}
