namespace NServiceBus.Transport.Sql.Shared
{
    /// <summary>
    /// What a <see cref="ProcessStrategy"/> did with the row it received, so the receiver can keep
    /// its <see cref="ReceiveState"/> anchor in step with the queue.
    /// </summary>
    readonly struct ProcessOutcome
    {
        ProcessOutcome(ProcessOutcomeKind kind, long rowVersion)
        {
            Kind = kind;
            RowVersion = rowVersion;
        }

        public static readonly ProcessOutcome NoMessage = new(ProcessOutcomeKind.NoMessage, 0);
        public static readonly ProcessOutcome RolledBack = new(ProcessOutcomeKind.RolledBack, 0);
        public static ProcessOutcome Committed(long rowVersion) => new(ProcessOutcomeKind.Committed, rowVersion);

        public ProcessOutcomeKind Kind { get; }
        public long RowVersion { get; }
    }

    enum ProcessOutcomeKind
    {
        NoMessage,
        Committed,
        RolledBack
    }
}
