// ReSharper disable CheckNamespace

namespace KloudShift.DebeTail.Abstractions;

/// <summary>
/// Reads raw change events from a database using its native replication or event mechanism.
/// <para>
/// Implementations are responsible for connection lifecycle, offset tracking, and
/// at-least-once delivery semantics. Each event must be acknowledged via
/// <see cref="AcknowledgeAsync"/> after successful downstream processing to advance
/// the committed replication position and allow the source to release WAL/binlog retention.
/// </para>
/// </summary>
public interface ISourceConnector : IAsyncDisposable
{
    /// <summary>
    /// Stable identifier for this connector instance, e.g. "postgres:orders-db".
    /// Must match the prefix used in <see cref="RawChangeEvent.ConnectorId"/>.
    /// </summary>
    string ConnectorId { get; }

    /// <summary>
    /// Opens the replication stream and yields change events.
    /// <para>
    /// The caller drives backpressure naturally via the async enumeration — the connector
    /// should not buffer events internally beyond what the source protocol requires.
    /// Cancelling <paramref name="ct"/> triggers a clean shutdown of the stream.
    /// </para>
    /// </summary>
    IAsyncEnumerable<RawChangeEvent> ReadAsync(CancellationToken ct = default);

    /// <summary>
    /// Acknowledges that all events up to and including <paramref name="offset"/> have been
    /// durably processed by the downstream sink. The connector advances its replication slot
    /// or binlog position accordingly.
    /// <para>
    /// The pipeline always calls this <em>after</em> <see cref="ISinkConnector.FlushAsync"/>
    /// confirms a durable write, never before, to preserve at-least-once delivery guarantees.
    /// </para>
    /// </summary>
    Task AcknowledgeAsync(ChangeOffset offset, CancellationToken ct = default);
}
