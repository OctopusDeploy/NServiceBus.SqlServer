namespace NServiceBus.Transport.Sql.Shared
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Logging;

    /// <summary>
    /// Probes the queue with receives instead of peeking, sizing each wave on what the previous one found.
    /// </summary>
    /// <remarks>
    /// Under competing consumers the peek's estimate counts rows locked by other instances, so it starts
    /// many receives that come up empty, each a full connection and transaction round trip. A receive costs
    /// about what a peek does, but when it succeeds it already has the message.
    /// </remarks>
    sealed class RampedWavePolicy(ReceiveState receiveState, TimeSpan emptyWaveBackoff, int maxWaveSize, TimeProvider timeProvider) : IReceiveWavePolicy
    {
        public void Start(TableBasedQueue inputQueue, RepeatedFailuresOverTimeCircuitBreaker receivingCircuitBreaker) => nextWaveSize = 1;

        public Task<int> NextWaveSize(int maxConcurrency, CancellationToken cancellationToken = default) =>
            Task.FromResult(Math.Min(nextWaveSize, maxConcurrency));

        public async Task WaveCompleted(int started, int messagesFound, CancellationToken cancellationToken = default)
        {
            if (messagesFound == 0)
            {
                nextWaveSize = 1;

                if (Logger.IsDebugEnabled)
                {
                    Logger.Debug($"Input queue empty. Next receive will be delayed for {emptyWaveBackoff}.");
                }

                await Task.Delay(emptyWaveBackoff, timeProvider, cancellationToken).ConfigureAwait(false);

                // The peek this replaces found rows stranded behind the anchor by other instances. Probing from
                // the head costs the same scan, so an idle queue does not wait for the periodic head sweep.
                receiveState.SweepFromHead();
            }
            else if (messagesFound < started)
            {
                // keep up with the observed availability without backing off
                nextWaveSize = messagesFound;
            }
            else
            {
                nextWaveSize = (int)Math.Min(maxWaveSize, messagesFound * 2L);
            }
        }

        int nextWaveSize = 1;

        static readonly ILog Logger = LogManager.GetLogger<RampedWavePolicy>();
    }
}
