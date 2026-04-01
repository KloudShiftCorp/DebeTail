// ReSharper disable CheckNamespace

namespace KloudShift.DebeTail.Abstractions;

/// <summary>
/// The canonical, source-agnostic representation of a database change event.
/// Produced by <see cref="IChangeNormalizer"/> from a <see cref="RawChangeEvent"/>;
/// consumed by <see cref="ISinkConnector"/> and <see cref="ISerializer"/> implementations.
/// </summary>
public sealed record DebeTailEnvelope
{
    /// <summary>Unique identifier for this event, assigned during normalization.</summary>
    public required Guid EventId { get; init; }

    /// <summary>Human-readable identifier of the source database, e.g. <c>postgres:orders-db</c>.</summary>
    public required string Source { get; init; }

    /// <summary>Database schema (namespace), e.g. <c>public</c>.</summary>
    public required string Schema { get; init; }

    /// <summary>Table name, e.g. <c>orders</c>.</summary>
    public required string Table { get; init; }

    /// <summary>Fully-qualified table reference: "{Schema}.{Table}".</summary>
    public string Relation => $"{Schema}.{Table}";

    /// <summary>The type of change: Insert, Update, Delete, or Truncate.</summary>
    public required DebeTailOperation Operation { get; init; }

    /// <summary>Timestamp of the change as recorded by the database, not the wall clock.</summary>
    public required DateTimeOffset OccurredAt { get; init; }

    /// <summary>
    /// Row state before the change. Empty for Insert; populated for Update/Delete
    /// only when the source is configured with full replica identity.
    /// </summary>
    public IReadOnlyDictionary<string, object?> Before { get; init; } = new Dictionary<string, object?>();

    /// <summary>Row state after the change. Empty for Delete.</summary>
    public IReadOnlyDictionary<string, object?> After { get; init; } = new Dictionary<string, object?>();

    /// <summary>Column schema for this event, carried from the source.</summary>
    public IReadOnlyList<ColumnSchema> Columns { get; init; } = [];

    /// <summary>
    /// Replication offset for this event. The pipeline uses this to call
    /// <see cref="ISourceConnector.AcknowledgeAsync"/> after the sink confirms the write.
    /// </summary>
    public required ChangeOffset Offset { get; init; }

    /// <summary>
    /// Source-specific metadata (transaction ID, sequence number, WAL LSN, etc.).
    /// Forwarded from <see cref="RawChangeEvent.SourceMetadata"/> for observability.
    /// </summary>
    public IReadOnlyDictionary<string, object?> Metadata { get; init; } = new Dictionary<string, object?>();
}
