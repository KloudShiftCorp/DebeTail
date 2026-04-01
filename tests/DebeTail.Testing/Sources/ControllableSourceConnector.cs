using System.Runtime.CompilerServices;
using System.Threading.Channels;
using KloudShift.DebeTail.Abstractions;

namespace KloudShift.DebeTail.Testing.Sources;

/// <summary>
/// An <see cref="ISourceConnector"/> that allows test code to push events imperatively
/// via <see cref="EmitAsync"/>. Ideal for testing retry logic, backpressure, and
/// time-sensitive pipeline behavior where the event timing must be controlled precisely.
/// </summary>
public sealed class ControllableSourceConnector(string connectorId) : ISourceConnector
{
    private readonly Channel<RawChangeEvent> _channel = Channel.CreateUnbounded<RawChangeEvent>();
    private readonly List<ChangeOffset> _acknowledgedOffsets = [];

    public string ConnectorId { get; } = connectorId;

    /// <summary>Offsets passed to <see cref="AcknowledgeAsync"/>, in call order.</summary>
    public IReadOnlyList<ChangeOffset> AcknowledgedOffsets => _acknowledgedOffsets;

    /// <summary>
    /// Pushes an event into the stream. The reading pipeline will receive it on the
    /// next iteration of its consumption loop.
    /// </summary>
    public ValueTask EmitAsync(RawChangeEvent ev, CancellationToken ct = default)
        => _channel.Writer.WriteAsync(ev, ct);

    /// <summary>
    /// Signals that no further events will be emitted. The reading pipeline's
    /// <see cref="ReadAsync"/> enumerable will complete after all queued events are consumed.
    /// </summary>
    public void Complete() => _channel.Writer.Complete();

    public async IAsyncEnumerable<RawChangeEvent> ReadAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var ev in _channel.Reader.ReadAllAsync(ct))
        {
            yield return ev;
        }
    }

    public Task AcknowledgeAsync(ChangeOffset offset, CancellationToken ct = default)
    {
        _acknowledgedOffsets.Add(offset);
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _channel.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }
}
