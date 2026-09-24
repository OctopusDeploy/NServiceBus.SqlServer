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

        /// <summary>
        /// The receive transaction rolled back, so the message is visible at the head of the queue
        /// again and the next receive has to scan from the head to find it.
        /// </summary>
        public static readonly ProcessOutcome RolledBack = new(ProcessOutcomeKind.RolledBack, 0);

        /// <summary>
        /// The received row is durably gone from the queue.
        /// </summary>
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
