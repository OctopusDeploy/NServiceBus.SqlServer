namespace NServiceBus.Transport.Sql.Shared
{
    using System;
    using System.Data;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;
    using Transport;

    interface IDelayedMessageStore
    {
        Task Store(OutgoingMessage message, TimeSpan dueAfter, string destination, DbConnection connection,
            DbTransaction transaction, CancellationToken cancellationToken = default);
    }

    class DelayedMessageTable : IDelayedMessageStore
    {
        public DelayedMessageTable(ISqlConstants sqlConstants, string delayedQueueTable, string inputQueueTable)
        {
            this.sqlConstants = sqlConstants;
            this.delayedQueueTable = delayedQueueTable;
            this.inputQueueTable = inputQueueTable;
            storeCommand = string.Format(sqlConstants.StoreDelayedMessageText, delayedQueueTable);
            moveText = TransportPatchKnobs.DelayedMoverElectionEnabled
                ? sqlConstants.MoveDueDelayedMessageText
                : sqlConstants.LegacyMoveDueDelayedMessageText;
            // start without a plan-pinning hint; resolved from the actual index names on first move
            moveDueCommand = string.Format(moveText, delayedQueueTable, inputQueueTable, "");
        }

        /// <summary>
        /// Resolves the plan-pinning INDEX hint against whatever the installation actually named
        /// the Due index (transport default Index_Due; Octopus IX_NSB_&lt;Endpoint&gt;Delayed_Due).
        /// Runs once per table instance; without a matching index the statement stays unhinted.
        /// </summary>
        async Task EnsurePlanPinningHintResolved(DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            if (planHintResolved)
            {
                return;
            }

            var findIndexText = sqlConstants.FindIndexByLeadingColumnText;
            if (string.IsNullOrEmpty(findIndexText) || !TransportPatchKnobs.PlanPinningHintsEnabled)
            {
                planHintResolved = true;
                return;
            }

            try
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = string.Format(findIndexText, delayedQueueTable, "Due");
                    command.CommandType = CommandType.Text;
                    command.Transaction = transaction;

                    if (await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is string indexName)
                    {
                        var hint = ", INDEX([" + indexName.Replace("]", "]]") + "])";
                        moveDueCommand = string.Format(moveText, delayedQueueTable, inputQueueTable, hint);
                        Logger.DebugFormat("Due-delayed-message plan for {0} pinned to index [{1}].", delayedQueueTable, indexName);
                    }
                    else
                    {
                        Logger.WarnFormat("No enabled nonclustered index leading on Due found for {0}; the due-message move cannot be plan-pinned.", delayedQueueTable);
                    }
                }

                planHintResolved = true;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                // never fail the mover over hint resolution; retry on a later poll
                Logger.Warn($"Could not resolve the plan-pinning hint for {delayedQueueTable}.", ex);
            }
        }

        public event EventHandler<DateTime> OnStoreDelayedMessage;

        /// <summary>
        /// Stores the message into the delayed queue instead of input queue
        /// </summary>
        public async Task Store(OutgoingMessage message, TimeSpan dueAfter, string destination, DbConnection connection,
            DbTransaction transaction, CancellationToken cancellationToken = default)
        {
            var messageRow = StoreDelayedMessageCommand.From(message.Headers, message.Body, dueAfter, destination);
            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = storeCommand;
                command.CommandType = CommandType.Text;

                messageRow.PrepareSendCommand(command);
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            OnStoreDelayedMessage?.Invoke(null, DateTime.UtcNow.Add(dueAfter));
        }

        /// <returns>The time of the next timeout due</returns>
        public async Task<DateTime> MoveDueMessages(int batchSize, DbConnection connection, DbTransaction transaction,
            CancellationToken cancellationToken = default)
        {
            await EnsurePlanPinningHintResolved(connection, transaction, cancellationToken).ConfigureAwait(false);

            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = moveDueCommand;
                command.CommandType = CommandType.Text;

                command.AddParameter("BatchSize", DbType.Int32, batchSize);
                using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        // No timeouts waiting
                        return DateTime.MinValue;
                    }

                    // Normalizing in case of clock drift between executing machine and database instance
                    var sqlNow = reader.GetDateTime(0);
                    var sqlNextDue = reader.GetDateTime(1);

                    // Providers with mover election (SQL Server) report the outcome as a third column
                    if (reader.FieldCount > 2)
                    {
                        if (reader.GetBoolean(2))
                        {
                            Interlocked.Increment(ref TransportPatchDiagnostics.DelayedMoverWon);
                        }
                        else
                        {
                            Interlocked.Increment(ref TransportPatchDiagnostics.DelayedMoverSkipped);
                        }
                    }

                    if (sqlNextDue <= sqlNow)
                    {
                        return DateTime.UtcNow;
                    }

                    return DateTime.UtcNow.Add(sqlNextDue - sqlNow);
                }
            }
        }

        readonly ISqlConstants sqlConstants;
        readonly string delayedQueueTable;
        readonly string inputQueueTable;
        readonly string moveText;
        string storeCommand;
        volatile string moveDueCommand;
        volatile bool planHintResolved;

        static readonly Logging.ILog Logger = Logging.LogManager.GetLogger<DelayedMessageTable>();
    }
}