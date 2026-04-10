using System.Text;
using System.Text.Json;
using KloudShift.DebeTail.Abstractions;
using Microsoft.Extensions.Logging;

namespace KloudShift.DebeTail.Sinks.Console;

/// <summary>
/// An <see cref="ISinkConnector"/> that writes change envelopes to stdout via
/// <see cref="ILogger"/>. Suitable for local development and containerized deployments
/// where stdout is the log aggregation channel.
/// <para>
/// When an <see cref="ISerializer"/> is registered in the DI container it is injected
/// automatically and used to format each envelope (e.g., CloudEvents JSON). When no
/// serializer is registered the sink falls back to plain <c>System.Text.Json</c>.
/// </para>
/// </summary>
public sealed class ConsoleSinkConnector(
    ILogger<ConsoleSinkConnector> logger,
    ISerializer? serializer = null) : ISinkConnector
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };

    /// <inheritdoc/>
    public Task WriteAsync(IReadOnlyList<DebeTailEnvelope> batch, CancellationToken ct = default)
    {
        foreach (var envelope in batch)
        {
            ct.ThrowIfCancellationRequested();

            var text = serializer is not null
                ? Encoding.UTF8.GetString(serializer.Serialize(envelope).Span)
                : JsonSerializer.Serialize(envelope, JsonOptions);

            logger.LogInformation("[{Operation}] {Relation} {Text}",
                envelope.Operation, envelope.Relation, text);
        }
        
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task FlushAsync(CancellationToken ct = default) => Task.CompletedTask;

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
