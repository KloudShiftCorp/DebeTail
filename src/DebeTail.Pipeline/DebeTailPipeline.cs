using KloudShift.DebeTail.Abstractions;
using KloudShift.DebeTail.Pipeline.Internal;
using Microsoft.Extensions.Logging;

namespace KloudShift.DebeTail.Pipeline;

/// <summary>
/// Entry point for the fluent pipeline builder.
/// </summary>
/// <example>
/// <code>
/// var pipeline = DebeTailPipeline
///     .From(postgresConnector)
///     .Watch("public.orders", "public.customers")
///     .Filter(e => e.Operation != CdcOperation.Delete)
///     .Transform(e => e with { Source = "my-app" })
///     .SinkTo(kafkaSink)
///     .Build();
///
/// await pipeline.StartAsync(cancellationToken);
/// </code>
/// </example>
public static class DebeTailPipeline
{
    /// <summary>
    /// Begins a new pipeline from the given source connector.
    /// </summary>
    public static IDebeTailPipelineSourceStage From(ISourceConnector source)
    {
        ArgumentNullException.ThrowIfNull(source);

        return new DebeTailPipelineBuilder(source);
    }

    /// <summary>
    /// Begins a new pipeline that merges events from multiple source connectors.
    /// When a single source is supplied the composite wrapper is skipped entirely.
    /// </summary>
    public static IDebeTailPipelineSourceStage From(params ISourceConnector[] sources)
    {
        ArgumentNullException.ThrowIfNull(sources);
        if (sources.Length == 0)
            throw new ArgumentException("At least one source connector must be provided.", nameof(sources));

        return sources.Length == 1
            ? new DebeTailPipelineBuilder(sources[0])
            : new DebeTailPipelineBuilder(new CompositeSourceConnector(sources));
    }

    /// <summary>
    /// Begins a new pipeline that merges events from multiple source connectors.
    /// Suitable for DI scenarios where sources are resolved as <c>IEnumerable&lt;ISourceConnector&gt;</c>.
    /// </summary>
    public static IDebeTailPipelineSourceStage From(IEnumerable<ISourceConnector> sources)
    {
        var list = sources?.ToList() ?? throw new ArgumentNullException(nameof(sources));
        if (list.Count == 0)
            throw new ArgumentException("At least one source connector must be provided.", nameof(sources));

        return list.Count == 1
            ? new DebeTailPipelineBuilder(list[0])
            : new DebeTailPipelineBuilder(new CompositeSourceConnector(list));
    }
}

/// <summary>First stage: choose which tables to watch.</summary>
public interface IDebeTailPipelineSourceStage
{
    /// <summary>
    /// Restricts the pipeline to events from the specified fully-qualified tables
    /// (e.g. "public.orders", "dbo.customers"). Events from other tables are dropped
    /// before normalization.
    /// </summary>
    IDebeTailPipelineFilterStage Watch(params string[] fullyQualifiedTables);

    /// <summary>Passes all tables through to the pipeline without filtering by name.</summary>
    IDebeTailPipelineFilterStage WatchAll();
}

/// <summary>Middle stage: add filters and transforms, then route to a sink.</summary>
public interface IDebeTailPipelineFilterStage
{
    /// <summary>
    /// Adds a predicate that drops envelopes for which <paramref name="predicate"/>
    /// returns <c>false</c>. Multiple filters are ANDed together.
    /// </summary>
    IDebeTailPipelineFilterStage Filter(Func<DebeTailEnvelope, bool> predicate);

    /// <summary>
    /// Adds a synchronous, stateless transformation applied to every envelope
    /// after normalization. Multiple transforms are applied in registration order.
    /// </summary>
    IDebeTailPipelineFilterStage Transform(Func<DebeTailEnvelope, DebeTailEnvelope> transform);

    /// <summary>
    /// Registers an enrichment step that is applied asynchronously to every envelope
    /// after all filters and synchronous transforms. Multiple steps are applied in
    /// registration order.
    /// <para>
    /// Enrichment step implementations are provided by the <c>DebeTail.Enrichment</c>
    /// commercial package (e.g. <c>LookupEnrichmentStep</c>).
    /// </para>
    /// </summary>
    IDebeTailPipelineFilterStage UseEnrichment(IEnrichmentStep step);

    /// <summary>Registers the primary sink connector.</summary>
    IDebeTailPipelineBuildStage SinkTo(ISinkConnector sink);

    /// <summary>
    /// Registers a primary sink and a dead-letter sink. Batches that fail after
    /// all retries are routed to <paramref name="deadLetter"/> instead of being dropped.
    /// </summary>
    IDebeTailPipelineBuildStage SinkTo(ISinkConnector sink, ISinkConnector deadLetter);

    // todo: should support multiple sinks with a single dead-letter sink?
    /// <summary>
    /// Registers multiple primary sinks. Every batch is written to each sink in order.
    /// If any sink throws, the pipeline retries the whole batch (at-least-once delivery).
    /// </summary>
    IDebeTailPipelineBuildStage SinkTo(IEnumerable<ISinkConnector> sinks);
}

/// <summary>Final stage: build the runnable pipeline.</summary>
public interface IDebeTailPipelineBuildStage
{
    /// <summary>
    /// Applies optional tuning. If not called, <see cref="DebeTailPipelineOptions"/> defaults are used.
    /// </summary>
    IDebeTailPipelineBuildStage WithOptions(Action<DebeTailPipelineOptions> configure);

    /// <summary>
    /// Configures the logger factory used for pipeline diagnostics.
    /// If not called, logging is suppressed. Prefer this in DI-based applications.
    /// </summary>
    IDebeTailPipelineBuildStage WithLoggerFactory(ILoggerFactory loggerFactory);

    /// <summary>Constructs the <see cref="IDebeTailPipeline"/> instance.</summary>
    IDebeTailPipeline Build();
}

/// <summary>A running CDC pipeline that can be started, stopped, and disposed.</summary>
public interface IDebeTailPipeline : IAsyncDisposable
{
    /// <summary>Starts the pipeline. Returns when the pipeline is connected and streaming.</summary>
    Task StartAsync(CancellationToken ct = default);

    /// <summary>Gracefully drains in-flight events and stops the pipeline.</summary>
    Task StopAsync(CancellationToken ct = default);
}
