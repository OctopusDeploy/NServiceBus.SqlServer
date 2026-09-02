namespace NServiceBus.Transport.SqlServer;

using System;
using System.Threading;
using System.Threading.Tasks;
using Logging;
using Microsoft.Data.SqlClient;
using NServiceBus.Transport.Sql.Shared;

class SqlServerMessageReceiver : MessageReceiver
{
    public SqlServerMessageReceiver(SqlServerTransport transport, string receiverId, string receiveAddress,
        string errorQueueAddress, Action<string, Exception, CancellationToken> criticalErrorAction,
        Func<TransportTransactionMode, ProcessStrategy> processStrategyFactory,
        Func<string, TableBasedQueue> queueFactory, IPurgeQueues queuePurger,
        IExpiredMessagesPurger expiredMessagesPurger,
        SchemaInspector schemaInspector, TimeSpan waitTimeCircuitBreaker, TimeSpan emptyBatchBackoff,
        ISubscriptionManager subscriptionManager, bool purgeAllMessagesOnStartup, IExceptionClassifier exceptionClassifier)
        : base(transport, receiverId,
        receiveAddress, errorQueueAddress, criticalErrorAction, processStrategyFactory, queueFactory, queuePurger,
        waitTimeCircuitBreaker, emptyBatchBackoff, subscriptionManager, purgeAllMessagesOnStartup, exceptionClassifier)
    {
        this.expiredMessagesPurger = expiredMessagesPurger;
        this.schemaInspector = schemaInspector;
    }

    public override async Task Initialize(PushRuntimeSettings limitations, OnMessage onMessage, OnError onError,
        CancellationToken cancellationToken = default)
    {
        await base.Initialize(limitations, onMessage, onError, cancellationToken).ConfigureAwait(false);

        // Self-identify once per process at WARN so hosts that run the package as-is can verify
        // from their logs which patch version and knob values are actually loaded.
        if (Interlocked.Exchange(ref patchDescribedOnStart, 1) == 0)
        {
            Logger.Warn(SqlServerTransportPatch.Describe());
        }

        await PurgeExpiredMessages(cancellationToken).ConfigureAwait(false);
        await PerformSchemaInspection(cancellationToken).ConfigureAwait(false);
    }

    public override async Task StopReceive(CancellationToken cancellationToken = default)
    {
        await base.StopReceive(cancellationToken).ConfigureAwait(false);

        // Final counter snapshot for the run, once per process at the first receiver shutdown
        if (Interlocked.Exchange(ref patchDescribedOnStop, 1) == 0)
        {
            Logger.Warn(SqlServerTransportPatch.Describe());
        }
    }

    static int patchDescribedOnStart;
    static int patchDescribedOnStop;

    async Task PerformSchemaInspection(CancellationToken cancellationToken) =>
        await schemaInspector.PerformInspection((SqlTableBasedQueue)inputQueue, cancellationToken).ConfigureAwait(false);

    async Task PurgeExpiredMessages(CancellationToken cancellationToken)
    {
        try
        {
            await expiredMessagesPurger.Purge((SqlTableBasedQueue)inputQueue, cancellationToken).ConfigureAwait(false);
        }
        catch (SqlException e) when (e.Number == 1205)
        {
            //Purge has been victim of a lock resolution
            Logger.Warn("Purger has been selected as a lock victim.", e);
        }
    }

    readonly IExpiredMessagesPurger expiredMessagesPurger;
    readonly SchemaInspector schemaInspector;

    static readonly ILog Logger = LogManager.GetLogger<SqlServerMessageReceiver>();
}