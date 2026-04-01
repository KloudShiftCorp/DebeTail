using KloudShift.DebeTail.Abstractions;

namespace KloudShift.DebeTail.Testing.Sources;

/// <summary>
/// An <see cref="ISourceConnector"/> that replays a fixed, preloaded sequence of events.
/// Ideal for deterministic unit tests where the full event set is known up front.
/// <para>
/// <see cref="AcknowledgeAsync"/> is a no-op; acknowledged offsets are recorded in
/// <see cref="AcknowledgedOffsets"/> for test assertions.
/// </para>
/// </summary>
public sealed class InMemorySourceConnector(string connectorId, IEnumerable<RawChangeEvent> events) : ISourceConnector
{
    private readonly List<ChangeOffset> _acknowledgedOffsets = [];

    public string ConnectorId { get; } = connectorId;

    /// <summary>Offsets passed to <see cref="AcknowledgeAsync"/>, in call order.</summary>
    public IReadOnlyList<ChangeOffset> AcknowledgedOffsets => _acknowledgedOffsets;

    public async IAsyncEnumerable<RawChangeEvent> ReadAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var ev in events)
        {
            ct.ThrowIfCancellationRequested();
            yield return ev;
            await Task.Yield(); // give the consumer a chance to process each event
        }
    }

    public Task AcknowledgeAsync(ChangeOffset offset, CancellationToken ct = default)
    {
        _acknowledgedOffsets.Add(offset);
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
