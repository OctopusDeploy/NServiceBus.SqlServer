namespace NServiceBus.Transport.Sql.Shared
{
    using System.Threading;
    using System.Threading.Tasks;

#pragma warning disable CA1711 // Identifiers should not have incorrect suffix
    interface IPeekMessagesInQueue
#pragma warning restore CA1711 // Identifiers should not have incorrect suffix
    {
        Task<PeekResult> Peek(TableBasedQueue inputQueue, RepeatedFailuresOverTimeCircuitBreaker circuitBreaker, CancellationToken cancellationToken = default);

        /// <summary>
        /// Peeks without delaying on an empty queue or reporting to a circuit breaker; failures are thrown.
        /// </summary>
        Task<PeekResult> PeekImmediately(TableBasedQueue inputQueue, CancellationToken cancellationToken = default);
    }
}