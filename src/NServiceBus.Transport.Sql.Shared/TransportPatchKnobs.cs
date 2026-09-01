namespace NServiceBus.Transport.Sql.Shared
{
    using System;

    /// <summary>
    /// Process-wide tuning knobs for the Octopus performance patch, settable through the public
    /// facade (SqlServerTransportPatch). Defaults match the measured-best configuration.
    /// MaxDispatchWave takes effect immediately; the others are read when an endpoint starts.
    /// </summary>
    static class TransportPatchKnobs
    {
        /// <summary>
        /// Ceiling for the receive dispatch wave per receiver. Read per wave. Default tuned for
        /// small (2-core) instances: swept 64/32/16 at 6 nodes x concurrency 128 — 16 matched 64
        /// in steady state and beat it draining a backlog (2,263/s @ 1.95 cpu-ms/msg vs
        /// 2,164/s @ 2.07), with real small-core clients favouring small waves further.
        /// </summary>
        public static int MaxDispatchWave = 16;

        /// <summary>
        /// Minimum interval between from-head rescans of a receiver's anchor. Applied when a
        /// receiver is created (endpoint start).
        /// </summary>
        public static TimeSpan HeadRescanFloor = TimeSpan.FromSeconds(1);

        /// <summary>
        /// Whether only one instance at a time moves due delayed messages (SQL Server only).
        /// Applied when an endpoint starts.
        /// </summary>
        public static bool DelayedMoverElectionEnabled = true;
    }
}
