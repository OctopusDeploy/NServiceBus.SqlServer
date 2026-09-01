namespace NServiceBus.Transport.Sql.Shared
{
    using System;
    using System.Text;
    using System.Threading;

    /// <summary>
    /// Lightweight counters for the Octopus performance patch, so a running endpoint can report
    /// which patch is loaded and what the receive pump is actually doing. Read through the public
    /// facade (SqlServerTransportPatch.Describe()).
    /// </summary>
    static class TransportPatchDiagnostics
    {
        public static long AnchoredReceives;
        public static long AnchoredReceivesFound;
        public static long HeadScanReceives;
        public static long HeadScanReceivesFound;
        public static long HeadScanGateSkips;
        public static long WavesDispatched;
        public static long EmptyWaveBackoffs;
        public static long PartialWaves;
        public static long DelayedMoverWon;
        public static long DelayedMoverSkipped;
        public static int LastWaveSize;

        static readonly DateTime StartedUtc = DateTime.UtcNow;

        public static void AppendSnapshot(StringBuilder builder)
        {
            builder.Append("counters since ").Append(StartedUtc.ToString("O")).Append(": ");
            builder.Append("anchoredReceives=").Append(Interlocked.Read(ref AnchoredReceives));
            builder.Append(" (found=").Append(Interlocked.Read(ref AnchoredReceivesFound)).Append(')');
            builder.Append(" headScanReceives=").Append(Interlocked.Read(ref HeadScanReceives));
            builder.Append(" (found=").Append(Interlocked.Read(ref HeadScanReceivesFound)).Append(')');
            builder.Append(" headScanGateSkips=").Append(Interlocked.Read(ref HeadScanGateSkips));
            builder.Append(" wavesDispatched=").Append(Interlocked.Read(ref WavesDispatched));
            builder.Append(" partialWaves=").Append(Interlocked.Read(ref PartialWaves));
            builder.Append(" emptyWaveBackoffs=").Append(Interlocked.Read(ref EmptyWaveBackoffs));
            builder.Append(" lastWaveSize=").Append(Volatile.Read(ref LastWaveSize));
            builder.Append(" delayedMoverWon=").Append(Interlocked.Read(ref DelayedMoverWon));
            builder.Append(" delayedMoverSkipped=").Append(Interlocked.Read(ref DelayedMoverSkipped));
        }
    }
}
