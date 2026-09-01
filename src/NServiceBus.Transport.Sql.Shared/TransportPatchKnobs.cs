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
        /// <summary>Ceiling for the receive dispatch wave per receiver. Read per wave.</summary>
        public static int MaxDispatchWave = 64;

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
