using System.Collections.Concurrent;
using KloudShift.DebeTail.Abstractions;

namespace KloudShift.DebeTail.Testing.Sinks;

/// <summary>
/// An <see cref="ISinkConnector"/> that accumulates all received envelopes in memory.
/// The primary assertion vehicle for pipeline tests:
/// <code>
/// Assert.Equal(3, sink.AllReceived.Count);
/// Assert.Equal(CdcOperation.Insert, sink.AllReceived[0].Operation);
/// </code>
/// </summary>
public sealed class CapturingSinkConnector : ISinkConnector
{
    private readonly ConcurrentBag<IReadOnlyList<DebeTailEnvelope>> _batches = [];

    /// <summary>All envelopes received across all batches, in arrival order.</summary>
    public IReadOnlyList<DebeTailEnvelope> AllReceived =>
        _batches.SelectMany(b => b).ToList();

    /// <summary>Raw batches as passed to <see cref="WriteAsync"/>, in arrival order.</summary>
    public IReadOnlyList<IReadOnlyList<DebeTailEnvelope>> Batches =>
        _batches.ToList();

    public Task WriteAsync(IReadOnlyList<DebeTailEnvelope> batch, CancellationToken ct = default)
    {
        _batches.Add(batch);
        return Task.CompletedTask;
    }

    /// <summary>No-op: all writes are synchronous in memory.</summary>
    public Task FlushAsync(CancellationToken ct = default) => Task.CompletedTask;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
