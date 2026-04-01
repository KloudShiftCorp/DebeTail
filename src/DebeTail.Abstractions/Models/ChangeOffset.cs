// ReSharper disable CheckNamespace

namespace KloudShift.DebeTail.Abstractions;

/// <summary>
/// An opaque, source-specific offset marker used for at-least-once delivery.
/// For Postgres this wraps an LSN; for MySQL a binlog file + position; etc.
/// </summary>
public sealed record ChangeOffset(string Value)
{
    /// <summary>Sentinel representing an unset or unknown offset.</summary>
    public static readonly ChangeOffset None = new(string.Empty);

    /// <inheritdoc/>
    public override string ToString() => Value;
}
