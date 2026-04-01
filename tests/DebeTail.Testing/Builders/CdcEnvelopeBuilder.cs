using KloudShift.DebeTail.Abstractions;

namespace KloudShift.DebeTail.Testing.Builders;

/// <summary>
/// Fluent builder for constructing <see cref="DebeTailEnvelope"/> instances in test fixtures.
/// <code>
/// var envelope = new CdcEnvelopeBuilder()
///     .ForTable("public.orders")
///     .AsInsert()
///     .WithAfter("id", 1)
///     .WithAfter("total", 99.99m)
///     .Build();
/// </code>
/// </summary>
public sealed class CdcEnvelopeBuilder
{
    private Guid _eventId = Guid.NewGuid();
    private string _source = "test:testdb";
    private string _schema = "public";
    private string _table = "test_table";
    private DebeTailOperation _operation = DebeTailOperation.Insert;
    private DateTimeOffset _occurredAt = DateTimeOffset.UtcNow;
    private Dictionary<string, object?> _before = [];
    private Dictionary<string, object?> _after = [];
    private List<ColumnSchema> _columns = [];
    private ChangeOffset _offset = new("0/1");
    private Dictionary<string, object?> _metadata = [];

    public CdcEnvelopeBuilder WithEventId(Guid id) { _eventId = id; return this; }
    public CdcEnvelopeBuilder WithSource(string source) { _source = source; return this; }

    /// <summary>Sets schema and table from a "schema.table" qualified name.</summary>
    public CdcEnvelopeBuilder ForTable(string qualifiedTable)
    {
        var parts = qualifiedTable.Split('.', 2);
        _schema = parts.Length == 2 ? parts[0] : "public";
        _table = parts.Length == 2 ? parts[1] : qualifiedTable;
        return this;
    }

    public CdcEnvelopeBuilder ForSchema(string schema) { _schema = schema; return this; }
    public CdcEnvelopeBuilder ForTableName(string table) { _table = table; return this; }
    public CdcEnvelopeBuilder AsInsert() { _operation = DebeTailOperation.Insert; return this; }
    public CdcEnvelopeBuilder AsUpdate() { _operation = DebeTailOperation.Update; return this; }
    public CdcEnvelopeBuilder AsDelete() { _operation = DebeTailOperation.Delete; return this; }
    public CdcEnvelopeBuilder AsTruncate() { _operation = DebeTailOperation.Truncate; return this; }
    public CdcEnvelopeBuilder OccurredAt(DateTimeOffset ts) { _occurredAt = ts; return this; }
    public CdcEnvelopeBuilder WithOffset(string offset) { _offset = new ChangeOffset(offset); return this; }

    public CdcEnvelopeBuilder WithAfter(string column, object? value)
    {
        _after[column] = value;
        return this;
    }

    public CdcEnvelopeBuilder WithBefore(string column, object? value)
    {
        _before[column] = value;
        return this;
    }

    public CdcEnvelopeBuilder WithMetadata(string key, object? value)
    {
        _metadata[key] = value;
        return this;
    }

    public DebeTailEnvelope Build() => new()
    {
        EventId = _eventId,
        Source = _source,
        Schema = _schema,
        Table = _table,
        Operation = _operation,
        OccurredAt = _occurredAt,
        Before = _before,
        After = _after,
        Columns = _columns,
        Offset = _offset,
        Metadata = _metadata,
    };
}
