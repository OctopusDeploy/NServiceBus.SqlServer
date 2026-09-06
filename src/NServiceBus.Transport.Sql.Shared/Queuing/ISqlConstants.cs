namespace NServiceBus.Transport.Sql.Shared;

interface ISqlConstants
{
    string PurgeText { get; set; }
    string StoreDelayedMessageText { get; set; }
    string ReceiveText { get; set; }
    string AnchoredReceiveText { get; set; }

    // Finds the name of an enabled nonclustered index whose leading key is a given column
    // ({0} = qualified table name, {1} = column name). Used to resolve plan-pinning index hints
    // against whatever the installation actually named its indexes (e.g. Octopus creates the
    // queue tables itself as IX_NSB_<Endpoint>_Row_Version, not the transport default names).
    // Null/empty when the provider does not support index hints.
    string FindIndexByLeadingColumnText { get; set; }
    string MoveDueDelayedMessageText { get; set; }
    string LegacyMoveDueDelayedMessageText { get; set; }
    string PeekText { get; set; }
    string AddMessageBodyStringColumn { get; set; }
    string CreateQueueText { get; set; }
    string CreateDelayedMessageStoreText { get; set; }
    string CreateSubscriptionTableText { get; set; }
    string SubscribeText { get; set; }
    string GetSubscribersText { get; set; }
    string UnsubscribeText { get; set; }
}