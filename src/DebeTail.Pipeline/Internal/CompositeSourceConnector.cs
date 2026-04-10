using System.Runtime.CompilerServices;
using System.Threading.Channels;
using KloudShift.DebeTail.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

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
/// <see cref="ISourceConnector.ConnectorId"/> — it is validated at construction time.
/// </para>
/// </remarks>
internal sealed class CompositeSourceConnector(
    IReadOnlyList<ISourceConnector> sources,
    ILogger<CompositeSourceConnector>? logger = null) : ISourceConnector
{
    private readonly ILogger<CompositeSourceConnector> _logger =
        logger ?? NullLogger<CompositeSourceConnector>.Instance;

    private readonly IReadOnlyDictionary<string, ISourceConnector> _sourceById =
        sources.ToDictionary(s =>
        {
            // The pipe character is the separator in composite offset encoding ("<id>|<offset>").
            // A ConnectorId that contains '|' would corrupt offset parsing and silently drop all acks.
            if (s.ConnectorId.Contains('|'))
                throw new ArgumentException(
                    $"Connector ID '{s.ConnectorId}' contains a pipe character ('|'), which is reserved " +
                    "for composite offset encoding. Rename the connector to remove it.",
                    nameof(sources));

            return s.ConnectorId;
        });

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
        {
            // Offset is not in the composite format "<connectorId>|<rawOffset>".
            // Should never happen in normal operation — the offset was not produced by this connector.
            _logger.LogWarning(
                "CompositeSourceConnector received an offset with no pipe separator, ack skipped: {Offset}",
                offset.Value);
            return Task.CompletedTask;
        }

        var connectorId = offset.Value[..pipeIndex];
        var rawOffset = offset.Value[(pipeIndex + 1)..];

        if (!_sourceById.TryGetValue(connectorId, out var source))
        {
            // The connector ID encoded in the offset does not match any registered child.
            // This indicates a corrupted or stale offset.
            _logger.LogWarning(
                "CompositeSourceConnector could not route ack — unknown connector ID '{ConnectorId}' in offset '{Offset}'",
                connectorId, offset.Value);
            return Task.CompletedTask;
        }

        return source.AcknowledgeAsync(new ChangeOffset(rawOffset), ct);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var source in sources)
            await source.DisposeAsync();
    }
}