using KloudShift.DebeTail.Abstractions;
using Microsoft.Extensions.Logging;
using MySqlCdc;
using MySqlCdc.Events;

namespace KloudShift.DebeTail.Sources.MariaDb;

public sealed partial class MariaDbSourceConnector
{
    private async Task<RawChangeEvent?> DecodeEventAsync(
        EventHeader header, IBinlogEvent binlogEvent, BinlogClient client, CancellationToken ct)
    {
        return binlogEvent switch
        {
            WriteRowsEvent write => await DecodeWriteRowsAsync(header, write, client, ct),
            UpdateRowsEvent update => await DecodeUpdateRowsAsync(header, update, client, ct),
            DeleteRowsEvent delete => await DecodeDeleteRowsAsync(header, delete, client, ct),
            _ => null,
        };
    }

    private async Task<RawChangeEvent?> DecodeWriteRowsAsync(
        EventHeader header, WriteRowsEvent ev, BinlogClient client, CancellationToken ct)
    {
        if (!_tableMap.TryGetValue(ev.TableId, out var tableMap))
        {
            _logger.LogWarning("No TableMapEvent cached for table id {TableId}", ev.TableId);
            return null;
        }

        if (!ShouldCapture(tableMap))
            return null;

        var columnNames = await GetColumnNamesAsync(tableMap, ct);
        var offset = BuildOffset(client);
        var rows = ev.Rows;

        if (rows.Count == 0)
            return null;

        // Emit only the first row per event. Multi-row inserts are uncommon but
        // can occur with bulk INSERTs. TODO: buffer and emit all rows.
        var after = BuildRowDict(rows[0].Cells, columnNames);
        var columns = BuildColumnSchemas(columnNames, tableMap);

        return BuildEvent(tableMap, header, offset, DebeTailOperation.Insert,
            before: null, after, columns);
    }

    private async Task<RawChangeEvent?> DecodeUpdateRowsAsync(
        EventHeader header, UpdateRowsEvent ev, BinlogClient client, CancellationToken ct)
    {
        if (!_tableMap.TryGetValue(ev.TableId, out var tableMap))
        {
            _logger.LogWarning("No TableMapEvent cached for table id {TableId}", ev.TableId);
            return null;
        }

        if (!ShouldCapture(tableMap))
            return null;

        var columnNames = await GetColumnNamesAsync(tableMap, ct);
        var offset = BuildOffset(client);
        var rows = ev.Rows;

        if (rows.Count == 0)
            return null;

        var before = BuildRowDict(rows[0].BeforeUpdate.Cells, columnNames);
        var after = BuildRowDict(rows[0].AfterUpdate.Cells, columnNames);
        var columns = BuildColumnSchemas(columnNames, tableMap);

        return BuildEvent(tableMap, header, offset, DebeTailOperation.Update, before, after, columns);
    }

    private async Task<RawChangeEvent?> DecodeDeleteRowsAsync(
        EventHeader header, DeleteRowsEvent ev, BinlogClient client, CancellationToken ct)
    {
        if (!_tableMap.TryGetValue(ev.TableId, out var tableMap))
        {
            _logger.LogWarning("No TableMapEvent cached for table id {TableId}", ev.TableId);
            return null;
        }

        if (!ShouldCapture(tableMap))
            return null;

        var columnNames = await GetColumnNamesAsync(tableMap, ct);
        var offset = BuildOffset(client);
        var rows = ev.Rows;

        if (rows.Count == 0)
            return null;

        var before = BuildRowDict(rows[0].Cells, columnNames);
        var columns = BuildColumnSchemas(columnNames, tableMap);

        return BuildEvent(tableMap, header, offset, DebeTailOperation.Delete, before, after: null, columns);
    }

    private bool ShouldCapture(TableMapEvent tableMap)
    {
        if (_watchedTables.Count == 0)
            return true;

        var qualified = $"{tableMap.DatabaseName}.{tableMap.TableName}";

        return _watchedTables.Contains(qualified)
               || _watchedTables.Contains(tableMap.TableName);
    }

    private async Task<IReadOnlyList<string>> GetColumnNamesAsync(
        TableMapEvent tableMap, CancellationToken ct)
    {
        var fromMetadata = tableMap.TableMetadata?.ColumnNames;
        if (fromMetadata is { Count: > 0 })
            return fromMetadata;

        return await _schemaCache.GetColumnNamesAsync(
            tableMap.DatabaseName, tableMap.TableName, ct);
    }

    private static Dictionary<string, object?> BuildRowDict(
        IReadOnlyList<object?> cells, IReadOnlyList<string> columnNames)
    {
        var dict = new Dictionary<string, object?>(cells.Count);
        for (var i = 0; i < cells.Count && i < columnNames.Count; i++)
            dict[columnNames[i]] = cells[i];

        return dict;
    }

    private static List<ColumnSchema> BuildColumnSchemas(
        IReadOnlyList<string> columnNames, TableMapEvent tableMap)
    {
        var schemas = new List<ColumnSchema>(columnNames.Count);
        for (var i = 0; i < columnNames.Count; i++)
        {
            var nativeType = i < tableMap.ColumnTypes.Length
                ? MapColumnType(tableMap.ColumnTypes[i])
                : "unknown";

            schemas.Add(new ColumnSchema { Name = columnNames[i], NativeType = nativeType });
        }

        return schemas;
    }

    private RawChangeEvent BuildEvent(
        TableMapEvent tableMap,
        EventHeader header,
        ChangeOffset offset,
        DebeTailOperation operation,
        Dictionary<string, object?>? before,
        Dictionary<string, object?>? after,
        List<ColumnSchema> columns)
    {
        var offsetParts = offset.Value.Split('|', 2);

        return new RawChangeEvent
        {
            ConnectorId = ConnectorId,
            Schema = tableMap.DatabaseName,
            Table = tableMap.TableName,
            Operation = operation,
            Offset = offset,
            OccurredAt = DateTimeOffset.FromUnixTimeSeconds(header.Timestamp),
            Before = before ?? [],
            After = after ?? [],
            Columns = columns,
            SourceMetadata = new Dictionary<string, object?>
            {
                ["binlog_filename"] = offsetParts.ElementAtOrDefault(0),
                ["binlog_position"] = offsetParts.ElementAtOrDefault(1),
            },
        };
    }

    private static ChangeOffset BuildOffset(BinlogClient client) =>
        new($"{client.State.Filename}|{client.State.Position}");
}
