// ReSharper disable CheckNamespace

namespace KloudShift.DebeTail.Abstractions;

/// <summary>
/// Describes a single column in the source table as reported by the replication stream.
/// </summary>
public sealed record ColumnSchema
{
    /// <summary>Column name as defined in the database.</summary>
    public required string Name { get; init; }

    /// <summary>
    /// Native database type string (e.g. "integer", "varchar", "timestamp with time zone").
    /// Format is source-specific; schema-aware serializers (Avro, Protobuf) use this to
    /// select the correct wire type.
    /// </summary>
    public required string NativeType { get; init; }

    /// <summary>
    /// Optional constraint flags reported by the source (e.g. "NOT NULL", "PRIMARY KEY").
    /// </summary>
    public IReadOnlyList<string> Constraints { get; init; } = [];
}
