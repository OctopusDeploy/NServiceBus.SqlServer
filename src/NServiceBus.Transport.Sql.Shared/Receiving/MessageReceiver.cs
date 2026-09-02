namespace NServiceBus.Transport.Sql.Shared
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Logging;

    class MessageReceiver : IMessageReceiver
    {
        public MessageReceiver(
            TransportDefinition transport,
            string receiverId,
            string receiveAddress,
            string errorQueueAddress,
            Action<string, Exception, CancellationToken> criticalErrorAction,
            Func<TransportTransactionMode, ProcessStrategy> processStrategyFactory,
            Func<string, TableBasedQueue> queueFactory,
            IPurgeQueues queuePurger,
            TimeSpan waitTimeCircuitBreaker,
            TimeSpan emptyBatchBackoff,
            ISubscriptionManager subscriptionManager,
            bool purgeAllMessagesOnStartup,
            IExceptionClassifier exceptionClassifier)
        {
            this.transport = transport;
            this.processStrategyFactory = processStrategyFactory;
            this.queuePurger = queuePurger;
            this.queueFactory = queueFactory;
            this.waitTimeCircuitBreaker = waitTimeCircuitBreaker;
            this.emptyBatchBackoff = emptyBatchBackoff;
            // The head rescan is the expensive from-head receive that picks up messages which
            // reappeared behind the anchor (e.g. rolled back on another instance). Its interval
            // trades that pickup latency against paying the old contended head-scan cost, so it
            // must not shrink with an aggressively tuned peek delay (e.g. 100ms) — floor it.
            var headRescanFloor = TransportPatchKnobs.HeadRescanFloor;
            var headRescanInterval = emptyBatchBackoff > headRescanFloor ? emptyBatchBackoff : headRescanFloor;
            receiveAnchor = new ReceiveAnchor(headRescanInterval);
            this.errorQueueAddress = errorQueueAddress;
            this.criticalErrorAction = criticalErrorAction;
            this.purgeAllMessagesOnStartup = purgeAllMessagesOnStartup;
            this.exceptionClassifier = exceptionClassifier;
            Subscriptions = subscriptionManager;
            Id = receiverId;
            ReceiveAddress = receiveAddress;
        }

        public virtual async Task Initialize(PushRuntimeSettings limitations, OnMessage onMessage, OnError onError,
            CancellationToken cancellationToken = default)
        {
            this.limitations = limitations;

            processStrategy = processStrategyFactory(transport.TransportTransactionMode);

            messageReceivingCircuitBreaker = new RepeatedFailuresOverTimeCircuitBreaker("message receiving",
                waitTimeCircuitBreaker,
                ex => criticalErrorAction("Failed to peek " + ReceiveAddress, ex,
                    messageProcessingCancellationTokenSource.Token));
            messageProcessingCircuitBreaker = new RepeatedFailuresOverTimeCircuitBreaker("message processing",
                waitTimeCircuitBreaker,
                ex => criticalErrorAction("Failed to receive from " + ReceiveAddress, ex,
                    messageProcessingCancellationTokenSource.Token));

            inputQueue = queueFactory(ReceiveAddress);
            errorQueue = queueFactory(errorQueueAddress);

            processStrategy.Init(inputQueue, errorQueue, receiveAnchor, onMessage, onError, criticalErrorAction);

            if (purgeAllMessagesOnStartup)
            {
                try
                {
                    var purgedRowsCount = await queuePurger.Purge(inputQueue, cancellationToken).ConfigureAwait(false);

                    Logger.InfoFormat("{0:N0} messages purged from queue {1}", purgedRowsCount, ReceiveAddress);
                }
                catch (Exception ex) when (!exceptionClassifier.IsOperationCancelled(ex, cancellationToken))
                {
                    Logger.Warn("Failed to purge input queue on startup.", ex);
                }
            }
        }

        public Task StartReceive(CancellationToken cancellationToken = default)
        {
            inputQueue.FormatPeekCommand();
            maxConcurrency = limitations.MaxConcurrency;
            concurrencyLimiter = new SemaphoreSlim(limitations.MaxConcurrency);

            messageReceivingCancellationTokenSource = new CancellationTokenSource();
            messageProcessingCancellationTokenSource = new CancellationTokenSource();
            messageReceivingCircuitBreaker = new RepeatedFailuresOverTimeCircuitBreaker("message receiving", waitTimeCircuitBreaker, ex => criticalErrorAction("Failed to peek " + ReceiveAddress, ex, messageProcessingCancellationTokenSource.Token));
            messageProcessingCircuitBreaker = new RepeatedFailuresOverTimeCircuitBreaker("message processing", waitTimeCircuitBreaker, ex => criticalErrorAction("Failed to receive from " + ReceiveAddress, ex, messageProcessingCancellationTokenSource.Token));

            // Task.Run() so the call returns immediately instead of waiting for the first await or return down the call stack
            messageReceivingTask =
                Task.Run(() => ReceiveMessagesAndSwallowExceptions(messageReceivingCancellationTokenSource.Token),
                    CancellationToken.None);

            return Task.CompletedTask;
        }

        public async Task ChangeConcurrency(PushRuntimeSettings newLimitations,
            CancellationToken cancellationToken = default)
        {
            SemaphoreSlim oldLimiter;
            int oldMaxConcurrency;

            lock (lockObject)
            {
                oldLimiter = concurrencyLimiter;
                oldMaxConcurrency = maxConcurrency;
                concurrencyLimiter = new SemaphoreSlim(newLimitations.MaxConcurrency);
                limitations = newLimitations;
                maxConcurrency = limitations.MaxConcurrency;
            }

            try
            {
                //Drain and dispose of the old semaphore
                while (oldLimiter.CurrentCount != oldMaxConcurrency)
                {
                    await Task.Delay(50, cancellationToken).ConfigureAwait(false);
                }

                oldLimiter.Dispose();
            }
            catch (Exception ex) when (exceptionClassifier.IsOperationCancelled(ex, cancellationToken))
            {
                //Ignore, we are stopping anyway
            }
        }

        public virtual async Task StopReceive(CancellationToken cancellationToken = default)
        {
            if (messageReceivingCancellationTokenSource == null)
            {
                // already stopped or never started
                return;
            }

            await messageReceivingCancellationTokenSource.CancelAsync().ConfigureAwait(false);

            await using (cancellationToken.Register(() => messageProcessingCancellationTokenSource?.Cancel()))
            {
                await messageReceivingTask.ConfigureAwait(false);

                while (concurrencyLimiter.CurrentCount != maxConcurrency)
                {
                    // Pass CancellationToken.None so that no exceptions will be thrown while waiting
                    // for the message receiver to gracefully shut down. The cancellation tokens passed to
                    // ProcessMessages (and thus the message processing pipelines) will be responsible
                    // for more forcefully shutting down message processing after the user's shutdown SLA
                    // is reached
                    await Task.Delay(50, CancellationToken.None).ConfigureAwait(false);
                }
            }

            messageReceivingCircuitBreaker.Dispose();
            messageProcessingCircuitBreaker.Dispose();
            concurrencyLimiter.Dispose();
            messageReceivingCancellationTokenSource.Dispose();
            messageReceivingCancellationTokenSource = null;
            messageProcessingCancellationTokenSource.Dispose();
        }

        async Task ReceiveMessagesAndSwallowExceptions(CancellationToken messageReceivingCancellationToken)
        {
            while (!messageReceivingCancellationToken.IsCancellationRequested)
            {
                try
                {
                    try
                    {
                        await ReceiveMessages(messageReceivingCancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (!exceptionClassifier.IsOperationCancelled(ex, messageReceivingCancellationToken))
                    {
                        Logger.Error("Message receiving failed", ex);
                        await messageReceivingCircuitBreaker.Failure(ex, messageReceivingCancellationToken)
                            .ConfigureAwait(false);
                    }
                }
                catch (Exception ex) when (exceptionClassifier.IsOperationCancelled(ex, messageReceivingCancellationToken))
                {
                    // private token, receiver is being stopped, log the exception in case the stack trace is ever needed for debugging
                    Logger.Debug("Operation canceled while stopping the message receiver.", ex);
                    break;
                }
            }
        }

        // Instead of peeking, the pump probes with receives directly and adapts the dispatch
        // wave to what it actually finds: after an idle/empty period a single receive probes the
        // queue; the wave doubles while every receive in it finds a message, tracks the observed
        // availability on a partial wave, and resets with a backoff when a whole wave comes up
        // empty. This removes the peek query entirely and — more importantly for clusters with
        // high concurrency settings — stops fanning out hundreds of receives on the strength of
        // the peek's gap-inflated (max-min) backlog estimate, which under competing consumers
        // wasted an empty receive round-trip many times per message. The wave is additionally
        // capped: with very high concurrency limits, one synchronized burst of receives per
        // instance produces latch/lock spikes on the queue head without improving throughput.
        async Task ReceiveMessages(CancellationToken messageReceivingCancellationToken)
        {
            // If either the receiving or processing circuit breakers are triggered, probe with one receive at a time.
            var maxDispatchWave = TransportPatchKnobs.MaxDispatchWave;
            var wave = messageProcessingCircuitBreaker.IsTriggered || messageReceivingCircuitBreaker.IsTriggered
                ? 1
                : Math.Min(dispatchRamp, Math.Min(maxDispatchWave, maxConcurrency));

            messageReceivingCancellationToken.ThrowIfCancellationRequested();

            // We cannot dispose this token source because of potential race conditions of concurrent processing
            var stopBatchCancellationSource = new CancellationTokenSource();

            var receiveLatch = new ReceiveCountdownEvent(wave);
            var dispatched = 0;
            for (var i = 0; i < wave; i++)
            {
                if (stopBatchCancellationSource.IsCancellationRequested)
                {
                    break;
                }

                var localConcurrencyLimiter = concurrencyLimiter;

                await localConcurrencyLimiter.WaitAsync(messageReceivingCancellationToken).ConfigureAwait(false);
                dispatched++;

                _ = ProcessMessagesSwallowExceptionsAndReleaseConcurrencyLimiter(stopBatchCancellationSource,
                    localConcurrencyLimiter, receiveLatch, messageProcessingCancellationTokenSource.Token);
            }

            // Release the latch slots of receives that were never dispatched (early batch stop)
            for (var i = dispatched; i < wave; i++)
            {
                receiveLatch.GetSignaler().Signal();
            }

            // Wait for all receive operations to complete before returning (and thus dispatching again)
            await receiveLatch.WaitAsync(stopBatchCancellationSource.Token).ConfigureAwait(false);

            Interlocked.Increment(ref TransportPatchDiagnostics.WavesDispatched);
            Volatile.Write(ref TransportPatchDiagnostics.LastWaveSize, dispatched);

            var messagesFound = receiveLatch.MessagesFound;
            if (messagesFound == 0)
            {
                // the queue is empty: probe again with a single receive after the backoff
                dispatchRamp = 1;
                Interlocked.Increment(ref TransportPatchDiagnostics.EmptyWaveBackoffs);
                await Task.Delay(emptyBatchBackoff, messageReceivingCancellationToken).ConfigureAwait(false);
            }
            else if (messagesFound < dispatched)
            {
                // partial wave: match the observed availability and keep going without a backoff,
                // otherwise a busy instance cannot keep up with its own arrival rate
                dispatchRamp = Math.Max(1, messagesFound);
                Interlocked.Increment(ref TransportPatchDiagnostics.PartialWaves);
            }
            else
            {
                dispatchRamp = Math.Min(maxDispatchWave, dispatchRamp * 2);
            }
        }

        async Task ProcessMessagesSwallowExceptionsAndReleaseConcurrencyLimiter(
            CancellationTokenSource stopBatchCancellationTokenSource, SemaphoreSlim localConcurrencyLimiter,
            ReceiveCountdownEvent receiveLatch, CancellationToken messageProcessingCancellationToken)
        {
            using var latchSignaler = receiveLatch.GetSignaler();
            try
            {
                try
                {
                    // We need to force the method to continue asynchronously because SqlConnection
                    // in combination with TransactionScope will apply connection pooling and enlistment synchronous in ctor.
                    await Task.Yield();

                    await processStrategy.ProcessMessage(stopBatchCancellationTokenSource, latchSignaler,
                        messageProcessingCancellationToken)
                        .ConfigureAwait(false);

                    messageProcessingCircuitBreaker.Success();
                }
                catch (Exception ex) when (!exceptionClassifier.IsOperationCancelled(ex, messageProcessingCancellationToken))
                {
                    Logger.Warn("Message processing failed", ex);

                    if (!exceptionClassifier.IsDeadlockException(ex))
                    {
                        await messageProcessingCircuitBreaker.Failure(ex, messageProcessingCancellationToken)
                            .ConfigureAwait(false);
                    }
                }
            }
            catch (Exception ex) when (exceptionClassifier.IsOperationCancelled(ex, messageProcessingCancellationToken))
            {
                Logger.Debug("Message processing canceled.", ex);
            }
            finally
            {
                localConcurrencyLimiter.Release();
            }
        }

        protected TableBasedQueue inputQueue;
        TableBasedQueue errorQueue;
        readonly object lockObject = new();
        readonly TransportDefinition transport;
        readonly string errorQueueAddress;
        readonly Action<string, Exception, CancellationToken> criticalErrorAction;
        readonly Func<TransportTransactionMode, ProcessStrategy> processStrategyFactory;
        readonly IPurgeQueues queuePurger;
        readonly Func<string, TableBasedQueue> queueFactory;
        readonly bool purgeAllMessagesOnStartup;
        readonly IExceptionClassifier exceptionClassifier;
        TimeSpan waitTimeCircuitBreaker;
        readonly TimeSpan emptyBatchBackoff;
        readonly ReceiveAnchor receiveAnchor;
        int dispatchRamp = 1;
        volatile SemaphoreSlim concurrencyLimiter;
        CancellationTokenSource messageReceivingCancellationTokenSource;
        CancellationTokenSource messageProcessingCancellationTokenSource;
        int maxConcurrency;
        RepeatedFailuresOverTimeCircuitBreaker messageReceivingCircuitBreaker;
        RepeatedFailuresOverTimeCircuitBreaker messageProcessingCircuitBreaker;
        Task messageReceivingTask;
        ProcessStrategy processStrategy;

        static readonly ILog Logger = LogManager.GetLogger<MessageReceiver>();
        PushRuntimeSettings limitations;


        public ISubscriptionManager Subscriptions { get; }
        public string Id { get; }
        public string ReceiveAddress { get; }
    }
}
