namespace NServiceBus.Transport.Sql.Shared
{
    using System;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A single receive handed to a <see cref="ProcessStrategy"/> by the receive loop. The strategy
    /// supplies the connection and transaction; the attempt owns the anchored query and reports
    /// back to the loop (the receive latch, the batch backoff and stopping an empty batch).
    /// </summary>
    sealed class ReceiveAttempt(TableBasedQueue inputQueue, ReceiveState receiveState, ReceiveCountdownEvent.Signaler receiveCountdownEventSignaler, CancellationTokenSource stopBatchCancellationTokenSource)
    {
        public async Task<MessageReadResult> Receive(DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken = default)
        {
            if (receiveStarted)
            {
                throw new InvalidOperationException("A receive attempt can only receive once.");
            }

            receiveStarted = true;

            var receiveResult = await TryReceiveAnchored(connection, transaction, cancellationToken).ConfigureAwait(false);

            if (receiveResult != MessageReadResult.NoMessage)
            {
                receiveState.MarkReceived();
            }

            receiveCountdownEventSignaler.Signal();

            if (receiveResult == MessageReadResult.NoMessage)
            {
                stopBatchCancellationTokenSource.Cancel();
            }

            return receiveResult;
        }

        /// <summary>
        /// Receives seeking past the anchor (the contended head region of the queue: other
        /// instances' locked in-flight rows and remains of recently consumed rows). When nothing
        /// is found past the anchor, rescans once from the head so messages that reappeared
        /// behind it (for example rolled back on another instance) are found before the queue is
        /// declared empty. The rescan is gated: with wide processing concurrency many receives
        /// hit an empty seek at the same moment, and a single from-head probe settles whether the
        /// queue is really empty — the rest report no message without paying for the scan.
        /// </summary>
        async Task<MessageReadResult> TryReceiveAnchored(DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            var anchor = receiveState.GetAnchor();
            var receiveResult = await inputQueue.TryReceive(connection, transaction, anchor, cancellationToken).ConfigureAwait(false);

            if (!receiveResult.Successful && !receiveResult.IsPoison && anchor > 0 && receiveState.TryEnterHeadScan())
            {
                try
                {
                    receiveResult = await inputQueue.TryReceive(connection, transaction, 0, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    receiveState.ExitHeadScan();
                }
            }

            return receiveResult;
        }

        bool receiveStarted;
    }
}
