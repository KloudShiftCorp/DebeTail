using System.Collections.Concurrent;
using KloudShift.DebeTail.Abstractions;

namespace KloudShift.DebeTail.Testing.OffsetStores;

/// <summary>
/// In-memory <see cref="IOffsetStore"/> for use in unit tests.
/// Offsets are not persisted across instances — create a new instance per test.
/// </summary>
public sealed class InMemoryOffsetStore : IOffsetStore
{
    private readonly ConcurrentDictionary<string, ChangeOffset> _offsets = new();

    /// <summary>All offsets currently held by this store, keyed by connector ID.</summary>
    public IReadOnlyDictionary<string, ChangeOffset> Offsets => _offsets;

    /// <inheritdoc/>
    public ValueTask<ChangeOffset?> LoadAsync(string connectorId, CancellationToken ct = default)
    {
        var result = _offsets.TryGetValue(connectorId, out var offset) ? offset : (ChangeOffset?)null;
        return ValueTask.FromResult(result);
    }

    /// <inheritdoc/>
    public ValueTask SaveAsync(string connectorId, ChangeOffset offset, CancellationToken ct = default)
    {
        _offsets[connectorId] = offset;
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}