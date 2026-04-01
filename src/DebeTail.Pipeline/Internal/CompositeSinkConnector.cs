using KloudShift.DebeTail.Abstractions;

namespace KloudShift.DebeTail.Pipeline.Internal;

/// <summary>
/// Fans out every batch to multiple <see cref="ISinkConnector"/> instances in order.
/// Used by <see cref="DebeTailPipelineBuilder"/> when <c>SinkTo(IEnumerable&lt;ISinkConnector&gt;)</c>
/// is called with more than one sink.
/// </summary>
internal sealed class CompositeSinkConnector(IReadOnlyList<ISinkConnector> sinks) : ISinkConnector
{
    public async Task WriteAsync(IReadOnlyList<DebeTailEnvelope> batch, CancellationToken ct = default)
    {
        foreach (var sink in sinks)
            await sink.WriteAsync(batch, ct);
    }

    public async Task FlushAsync(CancellationToken ct = default)
    {
        foreach (var sink in sinks)
            await sink.FlushAsync(ct);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var sink in sinks)
            await sink.DisposeAsync();
    }
}
