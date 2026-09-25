namespace NServiceBus.Transport.Sql.Shared
{
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Decides how many receives the message pump starts in each wave, and whether it backs off between waves.
    /// </summary>
    interface IReceiveWavePolicy
    {
        void Start(TableBasedQueue inputQueue, RepeatedFailuresOverTimeCircuitBreaker receivingCircuitBreaker);

        /// <summary>
        /// The number of receives to start next; zero skips the wave.
        /// </summary>
        Task<int> NextWaveSize(int maxConcurrency, CancellationToken cancellationToken = default);

        /// <summary>
        /// Called once every receive started in the wave has reported whether it found a message.
        /// </summary>
        Task WaveCompleted(int started, int messagesFound, CancellationToken cancellationToken = default);
    }
}
