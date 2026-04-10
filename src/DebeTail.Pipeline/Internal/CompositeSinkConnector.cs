using System.Runtime.ExceptionServices;
using KloudShift.DebeTail.Abstractions;

namespace KloudShift.DebeTail.Pipeline.Internal;

/// <summary>
/// Fans out every batch to multiple <see cref="ISinkConnector"/> instances in order.
/// Used by <see cref="DebeTailPipelineBuilder"/> when <c>SinkTo(IEnumerable&lt;ISinkConnector&gt;)</c>
/// is called with more than one sink.
/// </summary>
/// <remarks>
/// All sinks are always attempted even if an earlier one fails, so a transient error in
/// one sink does not prevent others from receiving the batch. Because the pipeline retries
/// the whole batch on any failure, each sink operates under at-least-once semantics —
/// consumers must be idempotent on <see cref="DebeTailEnvelope.EventId"/>.
/// </remarks>
internal sealed class CompositeSinkConnector(IReadOnlyList<ISinkConnector> sinks) : ISinkConnector
{
    public async Task WriteAsync(IReadOnlyList<DebeTailEnvelope> batch, CancellationToken ct = default)
    {
        List<Exception>? errors = null;

        foreach (var sink in sinks)
        {
            try
            {
                await sink.WriteAsync(batch, ct);
            }
            catch (Exception ex)
            {
                (errors ??= []).Add(ex);
            }
        }

        ThrowIfErrors(errors, "One or more sinks failed during WriteAsync.");
    }

    public async Task FlushAsync(CancellationToken ct = default)
    {
        List<Exception>? errors = null;

        foreach (var sink in sinks)
        {
            try
            {
                await sink.FlushAsync(ct);
            }
            catch (Exception ex)
            {
                (errors ??= []).Add(ex);
            }
        }

        ThrowIfErrors(errors, "One or more sinks failed during FlushAsync.");
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var sink in sinks)
            await sink.DisposeAsync();
    }

    private static void ThrowIfErrors(List<Exception>? errors, string message)
    {
        if (errors is null)
            return;

        if (errors.Count == 1)
            ExceptionDispatchInfo.Throw(errors[0]); // preserves original stack trace

        throw new AggregateException(message, errors);
    }
}