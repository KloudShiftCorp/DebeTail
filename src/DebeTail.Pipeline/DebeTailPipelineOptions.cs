namespace KloudShift.DebeTail.Pipeline;

/// <summary>
/// Tuning parameters for a <see cref="DebeTailPipeline"/> instance.
/// </summary>
public sealed class DebeTailPipelineOptions
{
    /// <summary>
    /// Maximum number of envelopes to accumulate before flushing a batch to the sink.
    /// Default: 100.
    /// </summary>
    public int MaxBatchSize { get; set; } = 100;

    /// <summary>
    /// Maximum time to wait before flushing an incomplete batch to the sink.
    /// Ensures low-volume tables are not stalled waiting for <see cref="MaxBatchSize"/>.
    /// Default: 500 ms.
    /// </summary>
    public TimeSpan MaxBatchAge { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// Maximum number of times the pipeline retries a failed sink write before
    /// routing the batch to the dead-letter sink (if configured). Default: 3.
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// Delay between sink retry attempts. Default: 1 second.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);
}
