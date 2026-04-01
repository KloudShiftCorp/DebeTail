// ReSharper disable CheckNamespace

namespace KloudShift.DebeTail.Abstractions;

/// <summary>
/// Writes normalized change envelopes to a target system (Kafka topic, Event Hub, file, etc.).
/// <para>
/// The pipeline controls batching and flush timing. Implementations should buffer internally
/// only when the target protocol requires it (e.g. a Kafka producer). Throw on unrecoverable
/// errors; the pipeline handles retries and dead-letter routing at the orchestration level.
/// </para>
/// </summary>
public interface ISinkConnector : IAsyncDisposable
{
    /// <summary>
    /// Writes a batch of envelopes to the target system.
    /// <para>
    /// Implementations may buffer the batch internally and return before the data is
    /// durably committed — that is the job of <see cref="FlushAsync"/>. Throws on
    /// unrecoverable write errors.
    /// </para>
    /// </summary>
    Task WriteAsync(IReadOnlyList<DebeTailEnvelope> batch, CancellationToken ct = default);

    /// <summary>
    /// Flushes any internally buffered data and waits until the target system confirms
    /// a durable write.
    /// <para>
    /// The pipeline always calls <see cref="FlushAsync"/> before calling
    /// <see cref="ISourceConnector.AcknowledgeAsync"/> on the source, ensuring that
    /// offset advancement only happens after data is safely committed downstream.
    /// </para>
    /// </summary>
    Task FlushAsync(CancellationToken ct = default);
}
