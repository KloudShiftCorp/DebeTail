// ReSharper disable CheckNamespace

namespace KloudShift.DebeTail.Abstractions;

/// <summary>
/// Thrown by an enrichment step when a lookup key is not found in the enrichment
/// source and <see cref="EnrichmentMissingKeyBehavior.Throw"/> is configured.
/// </summary>
public sealed class EnrichmentKeyNotFoundException(string message) : Exception(message);
