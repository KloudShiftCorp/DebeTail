// ReSharper disable CheckNamespace

namespace KloudShift.DebeTail.Abstractions;

/// <summary>
/// Append-only, time-windowed store for stream-stream joins (Phase 2).
/// Each entry carries its own event timestamp; the store can evict entries
/// that have fallen outside the configured join window.
/// <para>
/// Not yet implemented — interface defined now so state backends can be designed
/// against this contract. Concrete implementations (in-memory, Redis, RocksDB)
/// will ship in <c>DebeTail.Enrichment</c>.
/// </para>
/// </summary>
/// <typeparam name="TKey">The join key type. Must be non-null.</typeparam>
/// <typeparam name="TValue">The buffered event type.</typeparam>
public interface IWindowStore<TKey, TValue> : IAsyncDisposable
    where TKey : notnull
{
    /// <summary>
    /// Appends <paramref name="value"/> to the window buffer for <paramref name="key"/>,
    /// tagged with <paramref name="eventTime"/> (the database event timestamp, not wall clock).
    /// </summary>
    ValueTask AppendAsync(TKey key, TValue value, DateTimeOffset eventTime, CancellationToken ct = default);

    /// <summary>
    /// Returns all values for <paramref name="key"/> whose event time falls within
    /// [<paramref name="from"/>, <paramref name="to"/>] (inclusive).
    /// </summary>
    IAsyncEnumerable<TValue> QueryWindowAsync(TKey key, DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default);

    /// <summary>
    /// Evicts all entries (across all keys) whose event time is older than <paramref name="cutoff"/>.
    /// Called periodically by the join engine to bound memory usage.
    /// </summary>
    ValueTask EvictExpiredAsync(DateTimeOffset cutoff, CancellationToken ct = default);
}
