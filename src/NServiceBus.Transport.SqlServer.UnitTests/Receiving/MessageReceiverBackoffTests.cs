namespace NServiceBus.Transport.SqlServer.UnitTests.Receiving;

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Extensibility;
using NServiceBus.Transport;
using NServiceBus.Transport.Sql.Shared;
using NServiceBus.Unicast.Messages;
using NUnit.Framework;

public class MessageReceiverBackoffTests
{
    [Test]
    public async Task Backs_off_peeking_when_receives_come_up_empty()
    {
        // Simulates a node in a multi-node (competing consumer) setup that keeps losing the race
        // for messages: peek reports a backlog, but every receive comes up empty because other
        // nodes grabbed the messages. Without a backoff the pump re-peeks in a hot loop, which
        // multiplies the load on the queue table by the number of nodes.
        var peeker = new CountingPeeker(peekDelay: TimeSpan.FromMilliseconds(200));
        var receiver = CreateReceiver(state => new PeekWavePolicy(peeker, state), new FakeQueue(_ => MessageReadResult.NoMessage));

        await RunFor(receiver, concurrency: 8, TimeSpan.FromMilliseconds(700), CancellationToken).ConfigureAwait(false);

        // ~700ms with a 200ms backoff allows for a handful of peek iterations; an unthrottled
        // pump reaches hundreds.
        Assert.That(peeker.PeekCount, Is.LessThanOrEqualTo(6));
    }

    [Test]
    public async Task Does_not_back_off_when_the_batch_received_messages()
    {
        // each batch receives one message and then comes up empty, ending the batch early
        var messageAvailable = 0;
        var queue = new FakeQueue(_ => Interlocked.Exchange(ref messageAvailable, 0) == 1 ? Message() : MessageReadResult.NoMessage);
        var peeker = new CountingPeeker(peekDelay: TimeSpan.FromMilliseconds(200), onPeek: () => Interlocked.Exchange(ref messageAvailable, 1));
        var receiver = CreateReceiver(state => new PeekWavePolicy(peeker, state), queue);

        await RunFor(receiver, concurrency: 1, TimeSpan.FromMilliseconds(700), CancellationToken).ConfigureAwait(false);

        Assert.That(peeker.PeekCount, Is.GreaterThan(20));
    }

    [Test]
    public async Task Ramped_receive_backs_off_when_the_queue_is_empty()
    {
        var queue = new FakeQueue(_ => MessageReadResult.NoMessage);
        var receiver = CreateReceiver(RampedPolicy(), queue);

        await RunFor(receiver, concurrency: 8, TimeSpan.FromMilliseconds(700), CancellationToken).ConfigureAwait(false);

        // one probe per 200ms backoff; an unthrottled pump reaches thousands
        Assert.That(queue.ReceiveCount, Is.LessThanOrEqualTo(6));
    }

    [Test]
    public async Task Ramped_receive_does_not_back_off_after_a_partial_wave()
    {
        // Two of every three receives find a message, so waves alternate between one receive that
        // finds a message and two receives of which one does. Results are delayed so that both
        // receives of a wave start before the empty one stops it.
        var queue = new FakeQueue(call => call % 3 != 0 ? Message() : MessageReadResult.NoMessage, TimeSpan.FromMilliseconds(5));
        var receiver = CreateReceiver(RampedPolicy(), queue);

        await RunFor(receiver, concurrency: 8, TimeSpan.FromMilliseconds(700), CancellationToken).ConfigureAwait(false);

        Assert.That(queue.ReceiveCount, Is.GreaterThan(20));
    }

    [Test]
    public async Task Ramped_waves_never_start_more_receives_than_the_cap()
    {
        var queue = new FakeQueue(_ => Message(), TimeSpan.FromMilliseconds(20));
        var receiver = CreateReceiver(RampedPolicy(maxWaveSize: 8), queue);

        await receiver.Initialize(new PushRuntimeSettings(256), (_, _) => Task.CompletedTask, (_, _) => Task.FromResult(ErrorHandleResult.Handled), CancellationToken).ConfigureAwait(false);
        await receiver.StartReceive(CancellationToken).ConfigureAwait(false);
        await WaitUntil(() => queue.ReceiveCount >= 100, CancellationToken).ConfigureAwait(false);
        await receiver.StopReceive(CancellationToken).ConfigureAwait(false);

        Assert.That(queue.MaxConcurrentReceives, Is.EqualTo(8));
    }

    [Test]
    public async Task Judges_a_wave_only_once_every_started_receive_has_reported()
    {
        // the empty receive reports first and stops the wave, but the other receive in it still finds a message
        var queue = new FakeQueue(
            call => call % 2 == 0 ? MessageReadResult.NoMessage : Message(),
            call => call % 2 == 0 ? TimeSpan.FromMilliseconds(5) : TimeSpan.FromMilliseconds(100));
        var policy = new FirstWaveRecordingPolicy(waveSize: 2);
        var receiver = CreateReceiver(_ => policy, queue);

        await receiver.Initialize(new PushRuntimeSettings(8), (_, _) => Task.CompletedTask, (_, _) => Task.FromResult(ErrorHandleResult.Handled), CancellationToken).ConfigureAwait(false);
        await receiver.StartReceive(CancellationToken).ConfigureAwait(false);
        var firstWave = await policy.FirstWave.WaitAsync(TimeSpan.FromSeconds(10), CancellationToken).ConfigureAwait(false);
        await receiver.StopReceive(CancellationToken).ConfigureAwait(false);

        Assert.That(firstWave, Is.EqualTo((Started: 2, MessagesFound: 1)));
    }

    static async Task RunFor(MessageReceiver receiver, int concurrency, TimeSpan duration, CancellationToken cancellationToken)
    {
        await receiver.Initialize(new PushRuntimeSettings(concurrency), (_, _) => Task.CompletedTask, (_, _) => Task.FromResult(ErrorHandleResult.Handled), cancellationToken).ConfigureAwait(false);
        await receiver.StartReceive(cancellationToken).ConfigureAwait(false);
        await Task.Delay(duration, cancellationToken).ConfigureAwait(false);
        await receiver.StopReceive(cancellationToken).ConfigureAwait(false);
    }

    static async Task WaitUntil(Func<bool> condition, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        while (!condition())
        {
            if (stopwatch.Elapsed > TimeSpan.FromSeconds(10))
            {
                Assert.Fail("Timed out waiting for the condition");
            }

            await Task.Delay(10, cancellationToken).ConfigureAwait(false);
        }
    }

    static Func<ReceiveState, IReceiveWavePolicy> RampedPolicy(int maxWaveSize = 64) =>
        state => new RampedWavePolicy(state, TimeSpan.FromMilliseconds(200), maxWaveSize, TimeProvider.System);

    static MessageReadResult Message() => MessageReadResult.Success(new Message("1", string.Empty, Array.Empty<byte>(), false), 0);

    static MessageReceiver CreateReceiver(Func<ReceiveState, IReceiveWavePolicy> wavePolicyFactory, FakeQueue queue)
    {
        var classifier = new SqlServerExceptionClassifier();

        return new MessageReceiver(
            new SqlServerTransport("Server=unused;Trusted_Connection=True"),
            "receiver",
            "queue",
            "error",
            (_, _, _) => { },
            _ => new ReceivingStrategy(classifier),
            _ => queue,
            new FakePurger(),
            wavePolicyFactory,
            null,
            TimeSpan.FromSeconds(30),
            new FakeSubscriptionManager(),
            false,
            classifier,
            TimeProvider.System);
    }

    static CancellationToken CancellationToken => TestContext.CurrentContext.CancellationToken;

    class CountingPeeker(TimeSpan peekDelay, Action onPeek = null) : IPeekMessagesInQueue
    {
        int peekCount;

        public TimeSpan PeekDelay => peekDelay;

        public int PeekCount => Volatile.Read(ref peekCount);

        public Task<PeekResult> Peek(TableBasedQueue inputQueue, RepeatedFailuresOverTimeCircuitBreaker circuitBreaker, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref peekCount);
            onPeek?.Invoke();
            return Task.FromResult(new PeekResult(10, 1));
        }

        public Task WaitForPeekDelay(CancellationToken cancellationToken = default) => Task.Delay(peekDelay, cancellationToken);
    }

    class FirstWaveRecordingPolicy(int waveSize) : IReceiveWavePolicy
    {
        readonly TaskCompletionSource<(int Started, int MessagesFound)> firstWave = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<(int Started, int MessagesFound)> FirstWave => firstWave.Task;

        public void Start(TableBasedQueue inputQueue, RepeatedFailuresOverTimeCircuitBreaker receivingCircuitBreaker)
        {
        }

        public async Task<int> NextWaveSize(int maxConcurrency, CancellationToken cancellationToken = default)
        {
            if (firstWave.Task.IsCompleted)
            {
                // only the first wave is of interest; wait for the receiver to stop
                await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
            }

            return waveSize;
        }

        public Task WaveCompleted(int started, int messagesFound, CancellationToken cancellationToken = default)
        {
            _ = firstWave.TrySetResult((started, messagesFound));
            return Task.CompletedTask;
        }
    }

    class ReceivingStrategy(IExceptionClassifier exceptionClassifier) : ProcessStrategy(null, exceptionClassifier, null)
    {
        public override async Task<ProcessOutcome> ProcessMessage(ReceiveAttempt receiveAttempt, CancellationToken cancellationToken = default)
        {
            var receiveResult = await receiveAttempt.Receive(null, null, cancellationToken).ConfigureAwait(false);

            return receiveResult == MessageReadResult.NoMessage
                ? ProcessOutcome.NoMessage
                : ProcessOutcome.Committed;
        }
    }

    /// <summary>
    /// Numbers each receive from 1 in the order they start, and can delay the result per receive.
    /// </summary>
    class FakeQueue(Func<int, MessageReadResult> receive, Func<int, TimeSpan> delay = null) : TableBasedQueue(new SqlServerConstants(), "[dbo].[queue]", "queue", false)
    {
        public FakeQueue(Func<int, MessageReadResult> receive, TimeSpan delay) : this(receive, _ => delay)
        {
        }

        public int ReceiveCount => Volatile.Read(ref receiveCount);

        public int MaxConcurrentReceives => Volatile.Read(ref maxConcurrentReceives);

        public override async Task<MessageReadResult> TryReceive(System.Data.Common.DbConnection connection, System.Data.Common.DbTransaction transaction, long anchor, CancellationToken cancellationToken = default)
        {
            var call = Interlocked.Increment(ref receiveCount);
            var concurrent = Interlocked.Increment(ref concurrentReceives);
            InterlockedMax(ref maxConcurrentReceives, concurrent);

            try
            {
                if (delay?.Invoke(call) is { } wait && wait > TimeSpan.Zero)
                {
                    await Task.Delay(wait, cancellationToken).ConfigureAwait(false);
                }

                return receive(call);
            }
            finally
            {
                _ = Interlocked.Decrement(ref concurrentReceives);
            }
        }

        protected override Task SendRawMessage(MessageRow message, System.Data.Common.DbConnection connection, System.Data.Common.DbTransaction transaction, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        static void InterlockedMax(ref int target, int value)
        {
            var current = Volatile.Read(ref target);
            while (value > current)
            {
                var witnessed = Interlocked.CompareExchange(ref target, value, current);
                if (witnessed == current)
                {
                    break;
                }

                current = witnessed;
            }
        }

        int receiveCount;
        int concurrentReceives;
        int maxConcurrentReceives;
    }

    class FakePurger : IPurgeQueues
    {
        public Task<int> Purge(TableBasedQueue queue, CancellationToken cancellationToken = default) => Task.FromResult(0);
    }

    class FakeSubscriptionManager : ISubscriptionManager
    {
        public Task SubscribeAll(MessageMetadata[] eventTypes, ContextBag context, CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task Unsubscribe(MessageMetadata eventType, ContextBag context, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }
}
