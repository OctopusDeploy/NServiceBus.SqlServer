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
        public static string Version => "9.0.1-anchored-receive.11";

        /// <summary>
        /// Ceiling for the receive dispatch wave per receiver (default 64). Takes effect
        /// immediately, so it can be tuned at runtime.
        /// </summary>
        public static int MaxDispatchWave
        {
            get => Sql.Shared.TransportPatchKnobs.MaxDispatchWave;
            set => Sql.Shared.TransportPatchKnobs.MaxDispatchWave = value;
        }

        /// <summary>
        /// Minimum interval between from-head rescans of a receiver's anchor (default 1s).
        /// Longer = less head contention, slower cross-instance pickup of rolled-back messages.
        /// Set BEFORE Endpoint.Start.
        /// </summary>
        public static System.TimeSpan HeadRescanFloor
        {
            get => Sql.Shared.TransportPatchKnobs.HeadRescanFloor;
            set => Sql.Shared.TransportPatchKnobs.HeadRescanFloor = value;
        }

        /// <summary>
        /// Whether only one instance at a time moves due delayed messages (default true;
        /// SQL Server only). Set BEFORE Endpoint.Start.
        /// </summary>
        public static bool DelayedMoverElectionEnabled
        {
            get => Sql.Shared.TransportPatchKnobs.DelayedMoverElectionEnabled;
            set => Sql.Shared.TransportPatchKnobs.DelayedMoverElectionEnabled = value;
        }

        /// <summary>
        /// Whether receive/mover statements pin their plans with an INDEX hint resolved at
        /// runtime from the table's actual index names (default true; SQL Server only). Kill
        /// switch; read on each queue's first receive/move after start.
        /// </summary>
        public static bool PlanPinningHintsEnabled
        {
            get => Sql.Shared.TransportPatchKnobs.PlanPinningHintsEnabled;
            set => Sql.Shared.TransportPatchKnobs.PlanPinningHintsEnabled = value;
        }

        /// <summary>
        /// What this patch changes relative to the official NServiceBus.Transport.SqlServer 9.0.1.
        /// </summary>
        public static string Summary =>
            "Octopus perf patch for NServiceBus.Transport.SqlServer 9.0.1 (branch sql-fix-9.0.1). Fixes DB " +
            "load growing superlinearly with competing-consumer instance count. " +
            "(.1) ANCHORED RECEIVE: each receiver dequeues WHERE RowVersion > @Anchor (highest consumed) - " +
            "an index seek past the contended queue-index head (other instances' locked in-flight rows + " +
            "ghosts of recent deletes) instead of scanning it. Safety: empty anchored receive falls back to " +
            "one from-head receive; anchor resets on local rollback (immediate retries preserved); periodic " +
            "from-head rescan bounds pickup of messages rolled back on OTHER instances. " +
            "(.2) Rescan interval floored at 1s so a 100ms peek delay cannot make rescans a hot loop. " +
            "(.3) Fallback head scan gated to ONE prober per receiver - ungated, wide-concurrency empty " +
            "seeks each paid a head scan, measuring WORSE than stock in shallow steady state. " +
            "(.4) Peek REMOVED (its max-min estimate counts in-flight gaps and wildly over-reports, causing " +
            "empty-receive storms). The pump probes with real receives: wave doubles while fully successful, " +
            "partial wave continues at observed availability, empty wave resets to 1 probe + backoff " +
            "(QueuePeekerOptions.Delay). " +
            "(.5) This diagnostics facade. " +
            "(.6) DELAYED-MOVER ELECTION: one instance at a time moves due delayed messages (transaction-" +
            "owned sp_getapplock per delayed table; SQL Server only); losers skip the table and re-check in " +
            "~0.9s. See delayedMoverWon/Skipped counters. " +
            "(.7) Knobs on this class: MaxDispatchWave (immediate), HeadRescanFloor and " +
            "DelayedMoverElectionEnabled (before Endpoint.Start). Process-wide. " +
            "(.8) MaxDispatchWave default 16 (was 64) - swept 64/32/16 at 6 nodes x concurrency 128; 16 " +
            "tied steady state, won backlog drain; suits small instances. " +
            "(.9) Self-identifies: logged at WARN once per process at startup + final counters at shutdown. " +
            "(.10/.11) INDEX hints pin the receive to the RowVersion index and the mover to the Due index: " +
            "auto-stats on a near-empty table periodically flipped the receive to TableScan+Sort, and heap " +
            "deletes never release pages, so an empty 45k-page (352MB) heap cost 100ms/45,000 pages per " +
            "receive vs 0.5ms/8 (61% of statement CPU over 18min). Index names are resolved per table from " +
            "sys.indexes by leading column on first use (Octopus names them IX_NSB_..., not the transport " +
            "defaults); when none matches, statements stay unhinted and a WARN is logged. Kill switch: " +
            "PlanPinningHintsEnabled. " +
            "Measured (SQL2022, concurrency 256, 100ms delay, 50ms handler; DB CPU ms/msg at 1/6/12 nodes): " +
            "steady 400/s 2.86/3.00/3.32 vs stock 3.49/5.88/6.69; 20k drain at 12 nodes 2.13 vs 6.28 at " +
            "equal throughput, receive p99 278ms -> 16ms. " +
            "Trade-offs: cross-node redelivery of a rolled-back message bounded by rescan interval (~1s) " +
            "instead of immediate (same-node unaffected); best-effort FIFO slightly coarser; receive ramps " +
            "up over a few sub-second waves after idle.";

        /// <summary>
        /// Version, change summary and a live counter snapshot, for logging.
        /// </summary>
        public static string Describe()
        {
            var builder = new StringBuilder(4000);
            builder.Append("[SqlServerTransportPatch ").Append(Version).Append("] ");
            builder.Append("knobs: MaxDispatchWave=").Append(MaxDispatchWave);
            builder.Append(" HeadRescanFloor=").Append(HeadRescanFloor);
            builder.Append(" DelayedMoverElectionEnabled=").Append(DelayedMoverElectionEnabled);
            builder.Append(" PlanPinningHintsEnabled=").Append(PlanPinningHintsEnabled).Append("; ");
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
