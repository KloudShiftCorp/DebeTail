// ReSharper disable CheckNamespace

namespace KloudShift.DebeTail.Abstractions;

/// <summary>The type of change captured from the source database.</summary>
public enum DebeTailOperation
{
    /// <summary>A new row was inserted.</summary>
    Insert,
    /// <summary>An existing row was updated.</summary>
    Update,
    /// <summary>An existing row was deleted.</summary>
    Delete,
    /// <summary>All rows in a table were removed via TRUNCATE.</summary>
    Truncate,
}
