namespace NServiceBus.Transport.Sql.Shared
{
    using System;
    using System.Data;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;
    using Logging;
    using static System.String;

    abstract class TableBasedQueue
    {
        public string Name { get; }

        public TableBasedQueue(ISqlConstants sqlConstants, string qualifiedTableName, string queueName, bool isStreamSupported)
        {
            this.sqlConstants = sqlConstants;
            this.qualifiedTableName = qualifiedTableName;
            Name = queueName;
            // start without a plan-pinning hint; the hint is resolved from the table's actual
            // index names on first receive (installations name the indexes differently)
            receiveCommand = Format(sqlConstants.ReceiveText, this.qualifiedTableName, "");
            anchoredReceiveCommand = Format(sqlConstants.AnchoredReceiveText, this.qualifiedTableName, "");
            purgeCommand = Format(sqlConstants.PurgeText, this.qualifiedTableName);
            this.isStreamSupported = isStreamSupported;
        }

        /// <summary>
        /// Resolves the plan-pinning INDEX hint against whatever the installation actually named
        /// the RowVersion index (the transport default is Index_RowVersion; Octopus creates
        /// IX_NSB_&lt;Endpoint&gt;_Row_Version). Runs once per queue instance on the first
        /// receive; when no matching index exists the statements simply stay unhinted, which is
        /// the pre-hint behaviour.
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
                    command.CommandText = Format(findIndexText, qualifiedTableName, "RowVersion");
                    command.CommandType = CommandType.Text;
                    command.Transaction = transaction;

                    if (await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is string indexName)
                    {
                        var hint = ", INDEX([" + indexName.Replace("]", "]]") + "])";
                        receiveCommand = Format(sqlConstants.ReceiveText, qualifiedTableName, hint);
                        anchoredReceiveCommand = Format(sqlConstants.AnchoredReceiveText, qualifiedTableName, hint);
                        log.DebugFormat("Receive plan for {0} pinned to index [{1}].", qualifiedTableName, indexName);
                    }
                    else
                    {
                        log.WarnFormat("No enabled nonclustered index leading on RowVersion found for {0}; receive plans cannot be pinned and may degrade to table scans when the table statistics turn over.", qualifiedTableName);
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
                // never fail a receive over hint resolution; retry on a later receive
                log.Warn($"Could not resolve the receive plan-pinning hint for {qualifiedTableName}.", ex);
            }
        }

        public virtual async Task<int> TryPeek(DbConnection connection, DbTransaction transaction, int? timeoutInSeconds = null, CancellationToken cancellationToken = default)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandTimeout = timeoutInSeconds ?? 30;
                command.CommandType = CommandType.Text;
                command.Transaction = transaction;
                command.CommandText = peekCommand;

                var numberOfMessages = await command.ExecuteScalarAsyncOrDefault<int>(nameof(peekCommand), msg => log.Warn(msg), cancellationToken).ConfigureAwait(false);
                return numberOfMessages;
            }
        }

        public void FormatPeekCommand()
        {
            peekCommand = Format(sqlConstants.PeekText, qualifiedTableName);
        }

        public virtual async Task<MessageReadResult> TryReceive(DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken = default)
        {
            await EnsurePlanPinningHintResolved(connection, transaction, cancellationToken).ConfigureAwait(false);

            using (var command = connection.CreateCommand())
            {
                command.CommandText = receiveCommand;
                command.Transaction = transaction;
                command.CommandType = CommandType.Text;

                return await ReadMessage(command, readRowVersion: false, cancellationToken).ConfigureAwait(false);
            }
        }

        public virtual async Task<MessageReadResult> TryReceive(DbConnection connection, DbTransaction transaction, long anchor, CancellationToken cancellationToken = default)
        {
            await EnsurePlanPinningHintResolved(connection, transaction, cancellationToken).ConfigureAwait(false);

            using (var command = connection.CreateCommand())
            {
                command.CommandText = anchoredReceiveCommand;
                command.Transaction = transaction;
                command.CommandType = CommandType.Text;
                command.AddParameter("Anchor", DbType.Int64, anchor);

                return await ReadMessage(command, readRowVersion: true, cancellationToken).ConfigureAwait(false);
            }
        }

        public Task DeadLetter(MessageRow poisonMessage, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken = default)
        {
            return SendRawMessage(poisonMessage, connection, transaction, cancellationToken);
        }

        public Task Send(OutgoingMessage message, TimeSpan timeToBeReceived, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken = default)
        {
            var messageRow = MessageRow.From(message.Headers, message.Body, timeToBeReceived);

            return SendRawMessage(messageRow, connection, transaction, cancellationToken);
        }

        async Task<MessageReadResult> ReadMessage(DbCommand command, bool readRowVersion, CancellationToken cancellationToken)
        {
            var behavior = CommandBehavior.SingleRow;
            if (isStreamSupported)
            {
                behavior |= CommandBehavior.SequentialAccess;
            }

            using (var dataReader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false))
            {
                if (!await dataReader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    return MessageReadResult.NoMessage;
                }

                var readResult = await MessageRow.Read(dataReader, isStreamSupported, readRowVersion, cancellationToken).ConfigureAwait(false);

                //HINT: Reading all pending results makes sure that any query execution error,
                //      sent after the first result, are thrown by the SqlDataReader as SqlExceptions.
                //      More details in: https://github.com/DapperLib/Dapper/issues/1210
                while (await dataReader.ReadAsync(cancellationToken).ConfigureAwait(false))
                { }

                while (await dataReader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                { }

                return readResult;
            }
        }

        protected abstract Task SendRawMessage(MessageRow message, DbConnection connection, DbTransaction transaction,
            CancellationToken cancellationToken = default);


        public async Task<int> Purge(DbConnection connection, CancellationToken cancellationToken = default)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = purgeCommand;
                command.CommandType = CommandType.Text;

                return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        public override string ToString()
        {
            return qualifiedTableName;
        }

        ISqlConstants sqlConstants;
        protected string qualifiedTableName;
        string peekCommand;
        volatile string receiveCommand;
        volatile string anchoredReceiveCommand;
        volatile bool planHintResolved;
        string purgeCommand;
        bool isStreamSupported;

        static readonly ILog log = LogManager.GetLogger<TableBasedQueue>();
    }
}