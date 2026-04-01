// ReSharper disable CheckNamespace

namespace KloudShift.DebeTail.Abstractions;

/// <summary>
/// Provides enrichment values keyed by <typeparamref name="TKey"/>.
/// Implementations may be static (dictionary), snapshot-loaded, or live-queried.
/// </summary>
/// <remarks>
/// This interface carries no pipeline references — it is consumed by
/// <see cref="IEnrichmentStep"/> implementations in <c>DebeTail.Enrichment</c>
/// and is testable via the <c>DebeTail.Testing</c> package.
/// </remarks>
/// <typeparam name="TKey">The lookup key type. Must be non-null.</typeparam>
/// <typeparam name="TValue">The enrichment value type.</typeparam>
public interface IEnrichmentSource<TKey, TValue> : IAsyncDisposable
    where TKey : notnull
{
    /// <summary>
    /// Returns the enrichment value for <paramref name="key"/>,
    /// or <c>null</c> when no entry exists.
    /// </summary>
    ValueTask<TValue?> GetAsync(TKey key, CancellationToken ct = default);

    /// <summary>
    /// Called once before the pipeline begins processing events.
    /// Implementations may eagerly load a snapshot here.
    /// </summary>
    ValueTask InitializeAsync(CancellationToken ct = default);
}
