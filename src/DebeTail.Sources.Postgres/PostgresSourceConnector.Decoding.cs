using KloudShift.DebeTail.Abstractions;
using Npgsql;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;

namespace KloudShift.DebeTail.Sources.Postgres;

public sealed partial class PostgresSourceConnector
{
    private async Task<RawChangeEvent?> DecodeMessageAsync(PgOutputReplicationMessage message, CancellationToken ct)
    {
        return message switch
        {
            InsertMessage insert => await DecodeInsertAsync(insert, ct),
            DefaultUpdateMessage defaultUpdate => await DecodeDefaultUpdateAsync(defaultUpdate, ct),
            FullUpdateMessage fullUpdate => await DecodeFullUpdateAsync(fullUpdate, ct),
            IndexUpdateMessage indexUpdate => await DecodeIndexUpdateAsync(indexUpdate, ct),
            FullDeleteMessage fullDelete => await DecodeFullDeleteAsync(fullDelete, ct),
            KeyDeleteMessage keyDelete => await DecodeKeyDeleteAsync(keyDelete, ct),
            
            _ => null, // Begin, Commit, Relation, Type messages — no change event to emit
        };
    }

    private async Task<RawChangeEvent> DecodeInsertAsync(InsertMessage msg, CancellationToken ct)
    {
        var (after, columns) = await ExtractRowAsync(msg.NewRow, msg.Relation, ct);

        return BuildEvent(
            msg.Relation,
            msg.ServerClock,
            msg.WalEnd, 
            DebeTailOperation.Insert, before: null, after, columns);
    }

    private async Task<RawChangeEvent> DecodeDefaultUpdateAsync(DefaultUpdateMessage msg, CancellationToken ct)
    {
        // DefaultUpdate: only NewRow is available (default replica identity — primary key only for OLD)
        var (after, columns) = await ExtractRowAsync(msg.NewRow, msg.Relation, ct);
        
        return BuildEvent(
            msg.Relation, 
            msg.ServerClock, 
            msg.WalEnd, 
            DebeTailOperation.Update, before: null, after, columns);
    }

    private async Task<RawChangeEvent> DecodeFullUpdateAsync(FullUpdateMessage msg, CancellationToken ct)
    {
        var (before, columns) = await ExtractRowAsync(msg.OldRow, msg.Relation, ct);
        var (after, _) = await ExtractRowAsync(msg.NewRow, msg.Relation, ct);
        
        return BuildEvent(
            msg.Relation, 
            msg.ServerClock, 
            msg.WalEnd, 
            DebeTailOperation.Update, before, after, columns);
    }

    private async Task<RawChangeEvent> DecodeIndexUpdateAsync(IndexUpdateMessage msg, CancellationToken ct)
    {
        // IndexUpdate: OldRow contains only the indexed key columns; NewRow is the full new row.
        var (after, columns) = await ExtractRowAsync(msg.NewRow, msg.Relation, ct);
        
        return BuildEvent(
            msg.Relation, 
            msg.ServerClock, 
            msg.WalEnd, 
            DebeTailOperation.Update, before: null, after, columns);
    }

    private async Task<RawChangeEvent> DecodeFullDeleteAsync(FullDeleteMessage msg, CancellationToken ct)
    {
        var (before, columns) = await ExtractRowAsync(msg.OldRow, msg.Relation, ct);
        
        return BuildEvent(
            msg.Relation, 
            msg.ServerClock, 
            msg.WalEnd, 
            DebeTailOperation.Delete, before, after: null, columns);
    }

    private async Task<RawChangeEvent> DecodeKeyDeleteAsync(KeyDeleteMessage msg, CancellationToken ct)
    {
        // KeyDelete: the default replica identity sends only the primary-key columns in Key.
        // This is the common case for tables without REPLICA IDENTITY FULL.
        var (keyColumns, columns) = await ExtractRowAsync(msg.Key, msg.Relation, ct);
        
        return BuildEvent(
            msg.Relation, 
            msg.ServerClock, 
            msg.WalEnd, 
            DebeTailOperation.Delete, before: keyColumns, after: null, columns);
    }

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
            // Postgres WAL timestamps are always UTC, but Npgsql deserializes them with
            // DateTimeKind.Unspecified. SpecifyKind makes the intent explicit and ensures
            // serializers and comparisons treat the value correctly as UTC.
            OccurredAt = new DateTimeOffset(DateTime.SpecifyKind(serverClock, DateTimeKind.Utc)),
            Before = before ?? new Dictionary<string, object?>(),
            After = after ?? new Dictionary<string, object?>(),
            Columns = columns,
            SourceMetadata = new Dictionary<string, object?>
            {
                ["wal_end"] = walEnd.ToString(),
            },
        };
    }
}
