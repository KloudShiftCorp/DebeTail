// ReSharper disable CheckNamespace

namespace KloudShift.DebeTail.Abstractions;

/// <summary>
/// A change event as decoded from the source database's native replication protocol,
/// before normalization into a <see cref="DebeTailEnvelope"/>. Source connectors produce
/// these; <see cref="IChangeNormalizer"/> implementations consume them.
/// </summary>
public sealed record RawChangeEvent
{
    /// <summary>
    /// Stable identifier for the connector instance that produced this event,
    /// e.g. "postgres:orders-db". Used to route the event to the correct normalizer.
    /// </summary>
    public required string ConnectorId { get; init; }

    /// <summary>Database schema (namespace), e.g. "public".</summary>
    public required string Schema { get; init; }

    /// <summary>Table name, e.g. "orders".</summary>
    public required string Table { get; init; }

    /// <summary>The type of change: Insert, Update, Delete, or Truncate.</summary>
    public required DebeTailOperation Operation { get; init; }

    /// <summary>
    /// Replication offset at which this event occurred. Used to acknowledge
    /// processed events back to the source via <see cref="ISourceConnector.AcknowledgeAsync"/>.
    /// </summary>
    public required ChangeOffset Offset { get; init; }

    /// <summary>Timestamp of the change as recorded by the database, not the wall clock.</summary>
    public required DateTimeOffset OccurredAt { get; init; }

    /// <summary>
    /// Row state before the change. Null for Insert; populated for Update/Delete
    /// only when the source is configured with full replica identity.
    /// </summary>
    public IReadOnlyDictionary<string, object?> Before { get; init; } = new Dictionary<string, object?>();

    /// <summary>Row state after the change. Null for Delete.</summary>
    public IReadOnlyDictionary<string, object?> After { get; init; } = new Dictionary<string, object?>();

    /// <summary>Column schema as reported by the replication stream for this event.</summary>
    public IReadOnlyList<ColumnSchema> Columns { get; init; } = [];

    /// <summary>
    /// Source-specific extras: transaction ID, sequence number, WAL sender PID, etc.
    /// Keys are source-defined and should not be relied upon by portable code.
    /// </summary>
    public IReadOnlyDictionary<string, object?> SourceMetadata { get; init; } = new Dictionary<string, object?>();
}
