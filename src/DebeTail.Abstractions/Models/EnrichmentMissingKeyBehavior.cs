// ReSharper disable CheckNamespace

namespace KloudShift.DebeTail.Abstractions;

/// <summary>
/// Determines what happens when an enrichment lookup finds no value for the
/// extracted key.
/// </summary>
public enum EnrichmentMissingKeyBehavior
{
    /// <summary>Pass the event through unchanged when the lookup key is not found.</summary>
    PassThrough,

    /// <summary>
    /// Throw an <see cref="EnrichmentKeyNotFoundException"/> when the key is not found.
    /// Use this when a missing enrichment value is always a data error.
    /// </summary>
    Throw,
}
