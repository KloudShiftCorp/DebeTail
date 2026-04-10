using KloudShift.DebeTail.Abstractions;
using Npgsql;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;

namespace KloudShift.DebeTail.Sources.Postgres;

public sealed partial class PostgresSourceConnector
{
    private static async Task<(Dictionary<string, object?> values, List<ColumnSchema> columns)>
        ExtractRowAsync(ReplicationTuple tuple, RelationMessage relation, CancellationToken ct)
    {
        var values = new Dictionary<string, object?>();
        var columns = new List<ColumnSchema>();
        var i = 0;

        await foreach (var item in tuple)
        {
            ct.ThrowIfCancellationRequested();

            var colDef = relation.Columns[i++];
            var name = colDef.ColumnName;
            var nativeType = item.GetPostgresType().FullName;

            // item.Get() can only be called once per column; capture the value immediately.
            var rawValue = await item.Get();
            var value = rawValue is DBNull ? null : rawValue;

            values[name] = value;
            columns.Add(new ColumnSchema
            {
                Name = name,
                NativeType = nativeType ?? string.Empty,
            });
        }

        return (values, columns);
    }

    private RawChangeEvent BuildEvent(
        RelationMessage relation,
        DateTime serverClock,
        NpgsqlLogSequenceNumber walEnd,
        DebeTailOperation operation,
        Dictionary<string, object?>? before,
        Dictionary<string, object?>? after,
        List<ColumnSchema> columns)
    {
        return new RawChangeEvent
        {
            ConnectorId = ConnectorId,
            Schema = relation.Namespace,
            Table = relation.RelationName,
            Operation = operation,
            Offset = new ChangeOffset(walEnd.ToString()),
            OccurredAt = new DateTimeOffset(serverClock, TimeSpan.Zero),
            Before = before ?? new Dictionary<string, object?>(),
            After = after ?? new Dictionary<string, object?>(),
            Columns = columns,
            SourceMetadata = new Dictionary<string, object?>
            {
                ["wal_end"] = walEnd.ToString(),
            },
        };
    }

    // Doubles any embedded double-quotes and wraps the result in double-quotes,
    // producing a safely-quoted PostgreSQL identifier.
    private static string QuoteIdentifier(string identifier) =>
        '"' + identifier.Replace("\"", "\"\"") + '"';

    // Handles "schema.table" by quoting each part separately.
    private static string QuoteTableName(string name)
    {
        var dot = name.IndexOf('.');
        return dot < 0
            ? QuoteIdentifier(name)
            : $"{QuoteIdentifier(name[..dot])}.{QuoteIdentifier(name[(dot + 1)..])}";
    }

    private static string ExtractHostFromConnectionString(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        return builder.Host ?? "unknown";
    }
}
