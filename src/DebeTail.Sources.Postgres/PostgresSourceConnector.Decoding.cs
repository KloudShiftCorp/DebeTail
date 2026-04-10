using KloudShift.DebeTail.Abstractions;
using Npgsql;
using Npgsql.Replication.PgOutput.Messages;

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
        return BuildEvent(msg.Relation, msg.ServerClock, msg.WalEnd, DebeTailOperation.Insert, before: null, after,
            columns);
    }

    private async Task<RawChangeEvent> DecodeDefaultUpdateAsync(DefaultUpdateMessage msg, CancellationToken ct)
    {
        // DefaultUpdate: only NewRow is available (default replica identity — primary key only for OLD)
        var (after, columns) = await ExtractRowAsync(msg.NewRow, msg.Relation, ct);
        return BuildEvent(msg.Relation, msg.ServerClock, msg.WalEnd, DebeTailOperation.Update, before: null, after,
            columns);
    }

    private async Task<RawChangeEvent> DecodeFullUpdateAsync(FullUpdateMessage msg, CancellationToken ct)
    {
        var (before, columns) = await ExtractRowAsync(msg.OldRow, msg.Relation, ct);
        var (after, _) = await ExtractRowAsync(msg.NewRow, msg.Relation, ct);
        return BuildEvent(msg.Relation, msg.ServerClock, msg.WalEnd, DebeTailOperation.Update, before, after, columns);
    }

    private async Task<RawChangeEvent> DecodeIndexUpdateAsync(IndexUpdateMessage msg, CancellationToken ct)
    {
        // IndexUpdate: OldRow contains only the indexed key columns; NewRow is the full new row.
        var (after, columns) = await ExtractRowAsync(msg.NewRow, msg.Relation, ct);
        return BuildEvent(msg.Relation, msg.ServerClock, msg.WalEnd, DebeTailOperation.Update, before: null, after,
            columns);
    }

    private async Task<RawChangeEvent> DecodeFullDeleteAsync(FullDeleteMessage msg, CancellationToken ct)
    {
        var (before, columns) = await ExtractRowAsync(msg.OldRow, msg.Relation, ct);
        return BuildEvent(msg.Relation, msg.ServerClock, msg.WalEnd, DebeTailOperation.Delete, before, after: null,
            columns);
    }

    private async Task<RawChangeEvent> DecodeKeyDeleteAsync(KeyDeleteMessage msg, CancellationToken ct)
    {
        // KeyDelete: the default replica identity sends only the primary-key columns in Key.
        // This is the common case for tables without REPLICA IDENTITY FULL.
        var (keyColumns, columns) = await ExtractRowAsync(msg.Key, msg.Relation, ct);
        return BuildEvent(msg.Relation, msg.ServerClock, msg.WalEnd, DebeTailOperation.Delete, before: keyColumns,
            after: null, columns);
    }
}
