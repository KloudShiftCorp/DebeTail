using KloudShift.DebeTail.Abstractions;

namespace KloudShift.DebeTail.Testing.Builders;

/// <summary>
/// Fluent builder for constructing <see cref="RawChangeEvent"/> instances in test fixtures
/// without specifying every optional field by hand.
/// </summary>
public sealed class RawChangeEventBuilder
{
    private string _connectorId = "test:testdb";
    private string _schema = "public";
    private string _table = "test_table";
    private DebeTailOperation _operation = DebeTailOperation.Insert;
    private ChangeOffset _offset = new("0/1");
    private DateTimeOffset _occurredAt = DateTimeOffset.UtcNow;
    private Dictionary<string, object?> _before = [];
    private Dictionary<string, object?> _after = [];
    private List<ColumnSchema> _columns = [];
    private Dictionary<string, object?> _sourceMetadata = [];

    public RawChangeEventBuilder WithConnectorId(string id) { _connectorId = id; return this; }
    public RawChangeEventBuilder ForSchema(string schema) { _schema = schema; return this; }
    public RawChangeEventBuilder ForTable(string table) { _table = table; return this; }
    public RawChangeEventBuilder AsInsert() { _operation = DebeTailOperation.Insert; return this; }
    public RawChangeEventBuilder AsUpdate() { _operation = DebeTailOperation.Update; return this; }
    public RawChangeEventBuilder AsDelete() { _operation = DebeTailOperation.Delete; return this; }
    public RawChangeEventBuilder AsTruncate() { _operation = DebeTailOperation.Truncate; return this; }
    public RawChangeEventBuilder WithOffset(string offset) { _offset = new ChangeOffset(offset); return this; }
    public RawChangeEventBuilder OccurredAt(DateTimeOffset ts) { _occurredAt = ts; return this; }

    public RawChangeEventBuilder WithAfter(string column, object? value)
    {
        _after[column] = value;
        return this;
    }

    public RawChangeEventBuilder WithBefore(string column, object? value)
    {
        _before[column] = value;
        return this;
    }

    public RawChangeEventBuilder WithColumn(string name, string nativeType, params string[] constraints)
    {
        _columns.Add(new ColumnSchema { Name = name, NativeType = nativeType, Constraints = constraints });
        return this;
    }

    public RawChangeEvent Build() => new()
    {
        ConnectorId = _connectorId,
        Schema = _schema,
        Table = _table,
        Operation = _operation,
        Offset = _offset,
        OccurredAt = _occurredAt,
        Before = _before,
        After = _after,
        Columns = _columns,
        SourceMetadata = _sourceMetadata,
    };
}
