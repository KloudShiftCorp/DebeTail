// ReSharper disable CheckNamespace

namespace KloudShift.DebeTail.Abstractions;

/// <summary>
/// Backend-agnostic store for persisting and loading replication offsets across process restarts.
/// <para>
/// Inject an <see cref="IOffsetStore"/> into a source connector to enable automatic at-least-once
/// delivery across restarts without requiring the caller to manage <c>InitialOffset</c> manually.
/// </para>
/// <para>
/// <see cref="SaveAsync"/> is called by the connector inside <c>AcknowledgeAsync</c>, which the
/// pipeline invokes only after <c>ISinkConnector.FlushAsync</c> returns successfully — so the
/// persisted position always reflects durably written events.
/// </para>
/// </summary>
public interface IOffsetStore : IAsyncDisposable
{
    /// <summary>
    /// Loads the last persisted offset for <paramref name="connectorId"/>, or <c>null</c>
    /// if no offset has been saved yet (first run).
    /// </summary>
    ValueTask<ChangeOffset?> LoadAsync(string connectorId, CancellationToken ct = default);

    /// <summary>
    /// Persists <paramref name="offset"/> for <paramref name="connectorId"/>.
    /// Called after every successfully flushed batch.
    /// </summary>
    ValueTask SaveAsync(string connectorId, ChangeOffset offset, CancellationToken ct = default);
}