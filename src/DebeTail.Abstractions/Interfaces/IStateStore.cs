// ReSharper disable CheckNamespace

namespace KloudShift.DebeTail.Abstractions;

/// <summary>
/// Backend-agnostic key/value store used by enrichment and join features.
/// Implementations range from in-memory (<see cref="System.Collections.Concurrent.ConcurrentDictionary{TKey,TValue}"/>)
/// to durable backends (Redis, RocksDB) shipped in commercial packages.
/// </summary>
/// <typeparam name="TKey">The lookup key type. Must be non-null.</typeparam>
/// <typeparam name="TValue">The stored value type.</typeparam>
public interface IStateStore<TKey, TValue> : IAsyncDisposable
    where TKey : notnull
{
    /// <summary>Returns the value for <paramref name="key"/>, or <c>null</c> if the key does not exist.</summary>
    ValueTask<TValue?> GetAsync(TKey key, CancellationToken ct = default);

    /// <summary>Inserts or overwrites the value for <paramref name="key"/>.</summary>
    ValueTask SetAsync(TKey key, TValue value, CancellationToken ct = default);

    /// <summary>Removes the entry for <paramref name="key"/>. No-op if the key does not exist.</summary>
    ValueTask DeleteAsync(TKey key, CancellationToken ct = default);

    /// <summary>Removes all entries from the store.</summary>
    ValueTask ClearAsync(CancellationToken ct = default);
}
