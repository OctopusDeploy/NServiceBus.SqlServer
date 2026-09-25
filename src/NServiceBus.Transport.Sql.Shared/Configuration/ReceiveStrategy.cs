namespace NServiceBus
{
    /// <summary>
    /// How the message pump decides how many receives to start at once.
    /// </summary>
    public enum ReceiveStrategy
    {
        /// <summary>
        /// Peeks the queue for an estimate of the number of messages, then starts that many receives.
        /// </summary>
        /// <remarks>
        /// Under competing consumers the estimate counts rows locked by other instances, so it can
        /// start many more receives than there are messages available.
        /// </remarks>
        PeekReceive,

        /// <summary>
        /// Starts receives in waves, without peeking. After an empty wave a single receive probes the queue;
        /// the wave doubles while every receive in it finds a message, and shrinks to the number found otherwise.
        /// </summary>
        /// <remarks>
        /// A wave that finds nothing backs off for <see cref="QueuePeekerOptions.Delay"/>. Waves are capped at
        /// <see cref="QueuePeekerOptions.MaxReceiveWave"/> and at the message processing concurrency.
        /// </remarks>
        RampedReceive
    }
}
