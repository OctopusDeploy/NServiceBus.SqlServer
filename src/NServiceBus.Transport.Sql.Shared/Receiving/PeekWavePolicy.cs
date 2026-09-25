namespace NServiceBus.Transport.Sql.Shared
{
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Peeks the queue before each wave and starts as many receives as the peek estimates there are messages.
    /// </summary>
    sealed class PeekWavePolicy(IPeekMessagesInQueue queuePeeker, ReceiveState receiveState) : IReceiveWavePolicy
    {
        public void Start(TableBasedQueue inputQueue, RepeatedFailuresOverTimeCircuitBreaker receivingCircuitBreaker)
        {
            inputQueue.FormatPeekCommand();
            this.inputQueue = inputQueue;
            this.receivingCircuitBreaker = receivingCircuitBreaker;
        }

        public async Task<int> NextWaveSize(int maxConcurrency, CancellationToken cancellationToken = default)
        {
            var peekResult = await queuePeeker.Peek(inputQueue, receivingCircuitBreaker, cancellationToken).ConfigureAwait(false);

            if (peekResult.MessageCount == 0)
            {
                await queuePeeker.WaitForPeekDelay(cancellationToken).ConfigureAwait(false);
                return 0;
            }

            receiveState.ApplyPeekResult(peekResult.LowestRowVersion);

            return peekResult.MessageCount;
        }

        // the receives came up empty because competing instances consumed the messages the peek saw;
        // back off like an empty peek rather than sending every instance into a hot peek/receive loop
        public Task WaveCompleted(int started, int messagesFound, CancellationToken cancellationToken = default) =>
            messagesFound == 0 ? queuePeeker.WaitForPeekDelay(cancellationToken) : Task.CompletedTask;

        TableBasedQueue inputQueue;
        RepeatedFailuresOverTimeCircuitBreaker receivingCircuitBreaker;
    }
}
