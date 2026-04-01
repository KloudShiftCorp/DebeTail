// ReSharper disable CheckNamespace

namespace KloudShift.DebeTail.Abstractions;

/// <summary>
/// A single enrichment step executed by the pipeline for every event after
/// filtering and synchronous transforms have been applied.
/// </summary>
/// <remarks>
/// <para>
/// <c>DebeTail.Pipeline</c> (free) calls <see cref="EnrichAsync"/> — it has no
/// knowledge of how enrichment is performed. <c>DebeTail.Enrichment</c> (BSL)
/// provides implementations such as <c>LookupEnrichmentStep</c>.
/// </para>
/// <para>
/// Multiple steps may be registered on a single pipeline via
/// <c>IDebeTailPipelineFilterStage.UseEnrichment()</c>; they are applied in
/// registration order.
/// </para>
/// </remarks>
public interface IEnrichmentStep : IAsyncDisposable
{
    /// <summary>
    /// Enriches <paramref name="envelope"/> and returns the (possibly modified) result.
    /// Must not return <c>null</c>; return the original envelope unchanged when no
    /// enrichment applies.
    /// </summary>
    ValueTask<DebeTailEnvelope> EnrichAsync(DebeTailEnvelope envelope, CancellationToken ct = default);

    /// <summary>
    /// Called once before the pipeline loop starts. Implementations may load
    /// snapshots or open connections here.
    /// </summary>
    ValueTask InitializeAsync(CancellationToken ct = default);
}
