// ReSharper disable CheckNamespace

namespace KloudShift.DebeTail.Abstractions;

/// <summary>
/// Converts a source-specific <see cref="RawChangeEvent"/> into a canonical
/// <see cref="DebeTailEnvelope"/>. One normalizer implementation is registered per source
/// connector type and is matched by the pipeline using <see cref="ConnectorType"/>.
/// <para>
/// Implementations must be <b>stateless and thread-safe</b> — the pipeline
/// may call <see cref="Normalize"/> concurrently from multiple threads.
/// </para>
/// </summary>
public interface IChangeNormalizer
{
    /// <summary>
    /// The connector type prefix this normalizer handles.
    /// Matched against the prefix  of <see cref="RawChangeEvent.ConnectorId"/>
    /// (e.g. "postgres" matches "postgres:orders-db").
    /// </summary>
    string ConnectorType { get; }

    /// <summary>
    /// Converts a raw, source-specific change event into the canonical envelope.
    /// Must not mutate the input or hold references to it after returning.
    /// </summary>
    DebeTailEnvelope Normalize(RawChangeEvent raw);
}
