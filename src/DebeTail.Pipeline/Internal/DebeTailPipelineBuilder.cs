using KloudShift.DebeTail.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace KloudShift.DebeTail.Pipeline.Internal;

/// <summary>
/// Internal implementation of all staged builder interfaces.
/// Accumulates configuration and produces a <see cref="DebeTailPipelineRunner"/> via <see cref="Build"/>.
/// </summary>
internal sealed class DebeTailPipelineBuilder :
    IDebeTailPipelineSourceStage,
    IDebeTailPipelineFilterStage,
    IDebeTailPipelineBuildStage
{
    private readonly ISourceConnector _source;
    private readonly HashSet<string> _watchedTables = new(StringComparer.OrdinalIgnoreCase);
    private bool _watchAll;
    private readonly List<Func<DebeTailEnvelope, bool>> _filters = [];
    private readonly List<Func<DebeTailEnvelope, DebeTailEnvelope>> _transforms = [];
    private readonly List<IEnrichmentStep> _enrichmentSteps = [];
    private ISinkConnector? _sink;
    private ISinkConnector? _deadLetterSink;
    private readonly DebeTailPipelineOptions _options = new();
    private ILoggerFactory _loggerFactory = NullLoggerFactory.Instance;

    internal DebeTailPipelineBuilder(ISourceConnector source) => _source = source;

    public IDebeTailPipelineFilterStage Watch(params string[] fullyQualifiedTables)
    {
        foreach (var table in fullyQualifiedTables)
            _watchedTables.Add(table);
        return this;
    }

    public IDebeTailPipelineFilterStage WatchAll()
    {
        _watchAll = true;
        return this;
    }

    public IDebeTailPipelineFilterStage Filter(Func<DebeTailEnvelope, bool> predicate)
    {
        _filters.Add(predicate);
        return this;
    }

    public IDebeTailPipelineFilterStage Transform(Func<DebeTailEnvelope, DebeTailEnvelope> transform)
    {
        _transforms.Add(transform);
        return this;
    }

    public IDebeTailPipelineFilterStage UseEnrichment(IEnrichmentStep step)
    {
        ArgumentNullException.ThrowIfNull(step);
        _enrichmentSteps.Add(step);
        return this;
    }

    public IDebeTailPipelineBuildStage SinkTo(ISinkConnector sink)
    {
        _sink = sink;
        return this;
    }

    public IDebeTailPipelineBuildStage SinkTo(ISinkConnector sink, ISinkConnector deadLetter)
    {
        _sink = sink;
        _deadLetterSink = deadLetter;
        return this;
    }

    public IDebeTailPipelineBuildStage SinkTo(IEnumerable<ISinkConnector> sinks)
    {
        _sink = new CompositeSinkConnector(sinks.ToList());
        return this;
    }

    public IDebeTailPipelineBuildStage WithOptions(Action<DebeTailPipelineOptions> configure)
    {
        configure(_options);
        return this;
    }

    public IDebeTailPipelineBuildStage WithLoggerFactory(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
        return this;
    }

    public IDebeTailPipeline Build() => BuildCore(_loggerFactory);

    private DebeTailPipelineRunner BuildCore(ILoggerFactory loggerFactory)
    {
        if (_sink is null)
            throw new InvalidOperationException("A sink must be registered via SinkTo() before calling Build().");

        if (_options.MaxBatchSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(_options.MaxBatchSize), _options.MaxBatchSize, "MaxBatchSize must be greater than zero.");

        if (_options.MaxRetries < 0)
            throw new ArgumentOutOfRangeException(nameof(_options.MaxRetries), _options.MaxRetries, "MaxRetries must be zero or greater.");

        if (_options.MaxBatchAge <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(_options.MaxBatchAge), _options.MaxBatchAge, "MaxBatchAge must be a positive duration.");

        if (_options.RetryDelay < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(_options.RetryDelay), _options.RetryDelay, "RetryDelay must be zero or greater.");

        return new DebeTailPipelineRunner(
            source: _source,
            watchedTables: _watchAll ? null : _watchedTables,
            filters: [.. _filters],
            transforms: [.. _transforms],
            enrichmentSteps: [.. _enrichmentSteps],
            sink: _sink,
            deadLetterSink: _deadLetterSink,
            options: _options,
            logger: loggerFactory.CreateLogger<DebeTailPipelineRunner>());
    }
}
