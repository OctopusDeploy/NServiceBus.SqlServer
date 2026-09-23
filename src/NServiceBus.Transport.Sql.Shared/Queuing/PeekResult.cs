namespace NServiceBus.Transport.Sql.Shared
{
    /// <param name="MessageCount">Estimated number of messages available to receive.</param>
    /// <param name="LowestSequence">Lowest row version (sequence) visible to this receiver, or 0 when the queue is empty.</param>
    readonly record struct PeekResult(int MessageCount, long LowestSequence)
    {
        public static readonly PeekResult Empty = new(0, 0);
    }
}
