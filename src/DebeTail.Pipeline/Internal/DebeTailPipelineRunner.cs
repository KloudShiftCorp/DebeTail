using KloudShift.DebeTail.Abstractions;
using Microsoft.Extensions.Logging;

namespace KloudShift.DebeTail.Pipeline.Internal;

/// <summary>
/// Drives the CDC event loop: reads from the source, normalizes, filters, transforms,
/// batches, and writes to the sink. Manages offset acknowledgement and retries.
/// </summary>
internal sealed class DebeTailPipelineRunner : IDebeTailPipeline
{
    private readonly ISourceConnector _source;
    private readonly HashSet<string>? _watchedTables; // null = watch all
    private readonly IReadOnlyList<Func<DebeTailEnvelope, bool>> _filters;
    private readonly IReadOnlyList<Func<DebeTailEnvelope, DebeTailEnvelope>> _transforms;
    private readonly IReadOnlyList<IEnrichmentStep> _enrichmentSteps;
    private readonly ISinkConnector _sink;
    private readonly ISinkConnector? _deadLetterSink;
    private readonly DebeTailPipelineOptions _options;
    private readonly ILogger<DebeTailPipelineRunner> _logger;

    private CancellationTokenSource? _cts;
    private Task? _loopTask;

    // Completed when the first event arrives from the source (source is live and streaming),
    // faulted if the loop dies before receiving any event (bad connection string, missing
    // replication slot, auth failure, etc.), or completed when the stream ends cleanly with
    // no events. Callers can optionally await WhenReady after StartAsync for a connectivity signal.
    private readonly TaskCompletionSource _readyTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Completes when the pipeline has received its first event from the source, confirming
    /// the source connection is live. Faults with the original exception if the source fails
    /// to connect before producing any event. Await this after <see cref="StartAsync"/> when
    /// you need an explicit readiness guarantee — it is optional and safe to ignore.
    /// </summary>
    public Task WhenReady => _readyTcs.Task;

    internal DebeTailPipelineRunner(
        ISourceConnector source,
        HashSet<string>? watchedTables,
        IReadOnlyList<Func<DebeTailEnvelope, bool>> filters,
        IReadOnlyList<Func<DebeTailEnvelope, DebeTailEnvelope>> transforms,
        IReadOnlyList<IEnrichmentStep> enrichmentSteps,
        ISinkConnector sink,
        ISinkConnector? deadLetterSink,
        DebeTailPipelineOptions options,
        ILogger<DebeTailPipelineRunner> logger)
    {
        _source = source;
        _watchedTables = watchedTables;
        _filters = filters;
        _transforms = transforms;
        _enrichmentSteps = enrichmentSteps;
        _sink = sink;
        _deadLetterSink = deadLetterSink;
        _options = options;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken ct = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        foreach (var step in _enrichmentSteps)
            await step.InitializeAsync(_cts.Token);

        _loopTask = RunLoopAsync(_cts.Token);

        _logger.LogInformation("Pipeline starting (source: {SourceId})", _source.ConnectorId);

        // Log unhandled faults so pipeline crashes are not silently swallowed.
        _ = _loopTask.ContinueWith(
            t => _logger.LogError(t.Exception?.InnerException, "Pipeline loop terminated unexpectedly"),
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted,
            TaskScheduler.Default);
    }

    public async Task StopAsync(CancellationToken ct = default)
    {
        _logger.LogInformation("Pipeline stopping");

        if (_cts is not null)
            await _cts.CancelAsync();

        if (_loopTask is not null)
        {
            try
            {
                await _loopTask.WaitAsync(ct);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    private async Task RunLoopAsync(CancellationToken ct)
    {
        var batch = new List<DebeTailEnvelope>(_options.MaxBatchSize);
        var batchStarted = DateTimeOffset.UtcNow;
        var lastOffset = ChangeOffset.None;

        try
        {
            await foreach (var raw in _source.ReadAsync(ct))
            {
                // Signal readiness on the first event — the source is connected and streaming.
                _readyTcs.TrySetResult();

                _logger.LogTrace("Event received: [{ConnectorId}] {Schema}.{Table} {Operation}",
                    raw.ConnectorId, raw.Schema, raw.Table, raw.Operation);

                // Table filter: drop events not in the watch list
                if (_watchedTables is not null && !_watchedTables.Contains($"{raw.Schema}.{raw.Table}"))
                {
                    _logger.LogDebug("Event dropped (table not watched): {Schema}.{Table}", raw.Schema, raw.Table);
                    continue;
                }

                // Normalize: RawChangeEvent -> DebeTailEnvelope
                var envelope = Normalize(raw);

                // User-defined predicate filters
                if (!PassesFilters(envelope))
                {
                    _logger.LogDebug("Event dropped by predicate filter: [{Source}] {Schema}.{Table} {Operation}",
                        envelope.Source, envelope.Schema, envelope.Table, envelope.Operation);
                    continue;
                }

                // User-defined transforms (stateless, in registration order)
                envelope = ApplyTransforms(envelope);

                // Enrichment steps (async, in registration order)
                if (_enrichmentSteps.Count > 0)
                    envelope = await ApplyEnrichmentAsync(envelope, ct);

                batch.Add(envelope);
                lastOffset = raw.Offset;

                // Flush when batch is full or has aged past the max batch window
                var batchFull = batch.Count >= _options.MaxBatchSize;
                var batchAged = DateTimeOffset.UtcNow - batchStarted >= _options.MaxBatchAge;

                if (batchFull || batchAged)
                {
                    await FlushBatchAsync(batch, lastOffset, ct);
                    batch.Clear();
                    batchStarted = DateTimeOffset.UtcNow;
                }
            }

            // Flush any remaining events when the source stream completes
            if (batch.Count > 0)
                await FlushBatchAsync(batch, lastOffset, ct);

            _logger.LogInformation("Pipeline loop completed");
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            // Fault WhenReady so callers awaiting it get the real startup exception
            // (e.g. wrong connection string, missing replication slot, auth failure).
            _readyTcs.TrySetException(ex);
            throw;
        }
        finally
        {
            // Ensure WhenReady always completes — covers a clean empty-stream exit
            // and the cancellation path where neither branch above has signalled it yet.
            _readyTcs.TrySetResult();
        }
    }

    private async ValueTask<DebeTailEnvelope> ApplyEnrichmentAsync(DebeTailEnvelope envelope, CancellationToken ct)
    {
        foreach (var step in _enrichmentSteps)
            envelope = await step.EnrichAsync(envelope, ct);

        _logger.LogDebug("Enrichment applied ({Count} step(s)): [{Source}] {Schema}.{Table} {Operation}",
            _enrichmentSteps.Count, envelope.Source, envelope.Schema, envelope.Table, envelope.Operation);

        return envelope;
    }

    private static DebeTailEnvelope Normalize(RawChangeEvent raw) => new()
    {
        EventId = Guid.NewGuid(),
        Source = raw.ConnectorId,
        Schema = raw.Schema,
        Table = raw.Table,
        Operation = raw.Operation,
        OccurredAt = raw.OccurredAt,
        Before = raw.Before,
        After = raw.After,
        Columns = raw.Columns,
        Offset = raw.Offset,
        Metadata = raw.SourceMetadata,
    };

    private bool PassesFilters(DebeTailEnvelope envelope)
    {
        foreach (var filter in _filters)
        {
            if (!filter(envelope))
                return false;
        }

        return true;
    }

    private DebeTailEnvelope ApplyTransforms(DebeTailEnvelope envelope)
    {
        foreach (var transform in _transforms)
            envelope = transform(envelope);

        if (_transforms.Count > 0)
            _logger.LogDebug("Transforms applied ({Count}): [{Source}] {Schema}.{Table} {Operation}",
                _transforms.Count, envelope.Source, envelope.Schema, envelope.Table, envelope.Operation);

        return envelope;
    }

    private async Task FlushBatchAsync(List<DebeTailEnvelope> batch, ChangeOffset lastOffset, CancellationToken ct)
    {
        for (var attempt = 0; attempt <= _options.MaxRetries; attempt++)
        {
            try
            {
                await _sink.WriteAsync(batch, ct);
                await _sink.FlushAsync(ct);

                // Ack offset only after the sink confirms a durable write
                await _source.AcknowledgeAsync(lastOffset, ct);

                _logger.LogInformation("Batch flushed: {Count} events, offset {Offset}", batch.Count, lastOffset.Value);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (attempt < _options.MaxRetries)
            {
                _logger.LogWarning(ex, "Sink write failed on attempt {Attempt}/{MaxRetries}, retrying in {DelayMs}ms",
                    attempt + 1, _options.MaxRetries, (int)_options.RetryDelay.TotalMilliseconds);
                await Task.Delay(_options.RetryDelay, ct);
            }
            catch
            {
                // All retries exhausted — route to dead-letter sink if configured
                if (_deadLetterSink is not null)
                {
                    _logger.LogWarning("All retries exhausted, routing {Count} events to dead-letter sink",
                        batch.Count);

                    await _deadLetterSink.WriteAsync(batch, ct);
                    await _deadLetterSink.FlushAsync(ct);
                }
                else
                {
                    throw;
                }
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_cts is not null)
            await _cts.CancelAsync();

        if (_loopTask is not null)
        {
            try
            {
                await _loopTask;
            }
            catch (OperationCanceledException)
            {
            }
        }

        foreach (var step in _enrichmentSteps)
            await step.DisposeAsync();

        _cts?.Dispose();
    }
}
