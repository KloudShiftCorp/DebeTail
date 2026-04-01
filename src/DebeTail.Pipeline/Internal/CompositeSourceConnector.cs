using System.Runtime.CompilerServices;
using System.Threading.Channels;
using KloudShift.DebeTail.Abstractions;

namespace KloudShift.DebeTail.Pipeline.Internal;

/// <summary>
/// Merges multiple <see cref="ISourceConnector"/> instances into a single event stream.
/// Each source runs in its own background task feeding a shared channel.
/// Offsets are prefixed with the originating connector ID so that
/// <see cref="AcknowledgeAsync"/> can route acks back to the correct child.
/// </summary>
/// <remarks>
/// <para>
/// Offset encoding: every <see cref="RawChangeEvent.Offset"/> emitted by this connector
/// takes the form <c>"&lt;connectorId&gt;|&lt;rawOffset&gt;"</c>.
/// The pipe character (<c>|</c>) must not appear in any child connector's
/// <see cref="ISourceConnector.ConnectorId"/>.
/// </para>
/// </remarks>
internal sealed class CompositeSourceConnector(IReadOnlyList<ISourceConnector> sources) : ISourceConnector
{
    private readonly IReadOnlyDictionary<string, ISourceConnector> _sourceById =
        sources.ToDictionary(s => s.ConnectorId);

    public string ConnectorId { get; } =
        $"composite:[{string.Join(",", sources.Select(s => s.ConnectorId))}]";

    public async IAsyncEnumerable<RawChangeEvent> ReadAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var channel = Channel.CreateUnbounded<RawChangeEvent>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

        var workers = sources.Select(source => Task.Run(async () =>
        {
            try
            {
                await foreach (var raw in source.ReadAsync(ct))
                {
                    var stamped = raw with
                    {
                        Offset = new ChangeOffset($"{source.ConnectorId}|{raw.Offset.Value}")
                    };
                    await channel.Writer.WriteAsync(stamped, ct);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // Expected on shutdown — suppress so channel completes cleanly
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
                throw;
            }
        }, ct)).ToArray();

        // Complete the channel (with any aggregated exception) once all workers finish
        _ = Task.WhenAll(workers).ContinueWith(
            t => channel.Writer.TryComplete(t.Exception?.InnerException),
            TaskScheduler.Default);

        await foreach (var ev in channel.Reader.ReadAllAsync(ct))
            yield return ev;

        // Surface non-cancellation exceptions from workers
        await Task.WhenAll(workers);
    }

    public Task AcknowledgeAsync(ChangeOffset offset, CancellationToken ct = default)
    {
        if (offset == ChangeOffset.None)
            return Task.CompletedTask;

        var pipeIndex = offset.Value.IndexOf('|');
        if (pipeIndex < 0)
            return Task.CompletedTask;

        var connectorId = offset.Value[..pipeIndex];
        var rawOffset = offset.Value[(pipeIndex + 1)..];

        return _sourceById.TryGetValue(connectorId, out var source)
            ? source.AcknowledgeAsync(new ChangeOffset(rawOffset), ct)
            : Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var source in sources)
            await source.DisposeAsync();
    }
}
