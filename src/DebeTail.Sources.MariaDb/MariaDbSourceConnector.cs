using System.Runtime.CompilerServices;
using System.Threading.Channels;
using KloudShift.DebeTail.Abstractions;
using KloudShift.DebeTail.Sources.MariaDb.Configuration;
using KloudShift.DebeTail.Sources.MariaDb.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MySqlCdc;
using MySqlCdc.Events;
using MySqlConnector;

namespace KloudShift.DebeTail.Sources.MariaDb;

/// <summary>
/// Reads change events from a MySQL or MariaDB binary log using the MySqlCdc library.
/// Implements <see cref="ISourceConnector"/> for use with the DebeTail pipeline.
/// <para>
/// Column names are sourced from <c>TableMapEvent.TableMetadata.ColumnNames</c> when the
/// MySQL server has <c>binlog_row_metadata=FULL</c> set (MySQL 8.0+ only; not supported by
/// MariaDB). Otherwise, they are fetched from <c>INFORMATION_SCHEMA.COLUMNS</c> via a regular
/// connection on the first encounter of each table. MariaDB always uses this fallback.
/// </para>
/// <para>
/// <see cref="AcknowledgeAsync"/> is a no-op — the binlog is pull-based, and position
/// tracking is the responsibility of the caller. Store the last acknowledged
/// <see cref="ChangeOffset"/> and pass it back via <see cref="MariaDbConnectorOptions.InitialOffset"/>
/// on restart to resume from the correct position.
/// </para>
/// </summary>
public sealed class MariaDbSourceConnector : ISourceConnector
{
    private readonly MariaDbConnectorOptions _options;
    private readonly ILogger<MariaDbSourceConnector> _logger;
    private readonly HashSet<string> _watchedTables;
    private readonly TableSchemaCache _schemaCache;
    private readonly MySqlConnectionStringBuilder _parsedCs;

    // tableId → TableMapEvent, rebuilt when the connector starts.
    // Accessed only from within the single ReplicateAsync callback, so no locking needed.
    private readonly Dictionary<long, TableMapEvent> _tableMap = [];

    /// <summary>Initializes the connector and derives <see cref="ConnectorId"/> from the connection string.</summary>
    public MariaDbSourceConnector(ILogger<MariaDbSourceConnector> logger, IOptions<MariaDbConnectorOptions> options)
    {
        _logger = logger;
        _options = options.Value;

        _parsedCs = new MySqlConnectionStringBuilder(_options.ConnectionString);
        _schemaCache = new TableSchemaCache(_options.ConnectionString);
        _watchedTables = _options.TableNames.ToHashSet(StringComparer.OrdinalIgnoreCase);

        ConnectorId = $"mariadb:{_parsedCs.Server}:{_parsedCs.Port}";
    }

    /// <inheritdoc/>
    public string ConnectorId { get; }

    /// <summary>
    /// Opens the MySQL/MariaDB binlog stream and yields change events as they arrive.
    /// Set <see cref="MariaDbConnectorOptions.InitialOffset"/> to resume from a saved position.
    /// </summary>
    public async IAsyncEnumerable<RawChangeEvent> ReadAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        var binlogOptions = await ResolveBinlogOptionsAsync(ct);

        var client = new BinlogClient(opts =>
        {
            opts.Hostname = _parsedCs.Server;
            opts.Port = (int)_parsedCs.Port;
            opts.Username = _parsedCs.UserID;
            opts.Password = _parsedCs.Password;
            opts.Database = _parsedCs.Database;
            opts.ServerId = _options.ServerId;
            opts.Blocking = true;
            opts.Binlog = binlogOptions;
        });

        _logger.LogInformation("Opening MySQL/MariaDB binlog stream from {Host}:{Port}",
            _parsedCs.Server, _parsedCs.Port);

        // Bridge the callback-based ReplicateAsync into an IAsyncEnumerable via a channel.
        var channel = Channel.CreateUnbounded<RawChangeEvent>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });

        var replicateTask = Task.Run((Func<Task>)Worker);

        await foreach (var item in channel.Reader.ReadAllAsync(ct))
            yield return item;

        // Propagate any replication error that isn't a simple cancellation.
        await replicateTask;
        yield break;

        async Task Worker()
        {
            try
            {
                await client.ReplicateAsync(async binlogEvent =>
                {
                    if (binlogEvent is TableMapEvent tableMap)
                    {
                        _tableMap[tableMap.TableId] = tableMap;
                        return;
                    }

                    var raw = await DecodeEventAsync(binlogEvent.Header, binlogEvent, client, ct);
                    if (raw is not null) await channel.Writer.WriteAsync(raw, ct);
                }, ct);
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown — suppress so the channel completes cleanly.
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "MySqlCdc replication error");
                channel.Writer.TryComplete(ex);

                return;
            }

            channel.Writer.TryComplete();
        }
    }

    /// <summary>
    /// No-op — MySQL/MariaDB binlog position tracking is external.
    /// Persist the <paramref name="offset"/> value and pass it back via
    /// <see cref="MariaDbConnectorOptions.InitialOffset"/> on the next startup.
    /// </summary>
    /// <inheritdoc/>
    public Task AcknowledgeAsync(ChangeOffset offset, CancellationToken ct = default) => Task.CompletedTask;

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

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
                ["binlog_filename"] = offset.Value.Split('|').ElementAtOrDefault(0),
                ["binlog_position"] = offset.Value.Split('|').ElementAtOrDefault(1),
            },
        };
    }

    private static ChangeOffset BuildOffset(BinlogClient client) =>
        new($"{client.State.Filename}|{client.State.Position}");

    /// <summary>
    /// Resolves the starting binlog position. When <see cref="MariaDbConnectorOptions.InitialOffset"/>
    /// is set, parses and uses it directly. Otherwise queries the server for the current binlog
    /// position using <c>SHOW BINARY LOG STATUS</c> (MySQL 8.2+) or <c>SHOW MASTER STATUS</c>
    /// (MariaDB / MySQL &lt; 8.2), falling back between the two automatically.
    /// </summary>
    private async Task<BinlogOptions> ResolveBinlogOptionsAsync(CancellationToken ct)
    {
        if (!string.IsNullOrWhiteSpace(_options.InitialOffset))
        {
            var parts = _options.InitialOffset.Split('|', 2);

            if (parts.Length == 2 && long.TryParse(parts[1], out var savedPosition))
                return BinlogOptions.FromPosition(parts[0], savedPosition);
        }

        // MySQL 8.2+ removed SHOW MASTER STATUS in favour of SHOW BINARY LOG STATUS.
        // MariaDB and older MySQL still use SHOW MASTER STATUS.
        // Try the new statement first; on a parse error fall back to the legacy one.
        await using var conn = new MySqlConnection(_options.ConnectionString);
        await conn.OpenAsync(ct);

        string? filename = null;
        long position = 0;

        foreach (var sql in (string[])["show binary log status", "show master status"])
        {
            try
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (await reader.ReadAsync(ct))
                {
                    filename = reader.GetString("File");
                    position = reader.GetInt64("Position");
                }

                break;
            }
            catch (MySqlException ex) when (ex.ErrorCode == MySqlErrorCode.ParseError)
            {
                // Statement not supported on this server version — try the next one.
                _logger.LogInformation("'{Sql}' not supported, trying fallback", sql);
            }
        }

        if (filename is null)
        {
            _logger.LogError(
                "Could not determine the current binlog position. Ensure binary logging is enabled on the server.");

            throw new InvalidOperationException(
                "Could not determine the current binlog position. " +
                "Ensure binary logging is enabled on the server.");
        }


        _logger.LogInformation(
            "Starting MySQL/MariaDB binlog stream from current position: {Filename}|{Position}",
            filename, position);

        return BinlogOptions.FromPosition(filename, position);
    }

    /// <summary>
    /// Maps a MySQL column type byte to a human-readable type name.
    /// Values correspond to the MySQL internal type constants (enum_field_types).
    /// </summary>
    private static string MapColumnType(byte typeCode) => typeCode switch
    {
        1 => "tinyint",
        2 => "smallint",
        3 => "int",
        4 => "float",
        5 => "double",
        7 => "timestamp",
        8 => "bigint",
        9 => "mediumint",
        10 => "date",
        11 => "time",
        12 => "datetime",
        13 => "year",
        15 => "varchar",
        16 => "bit",
        245 => "json",
        246 => "decimal",
        247 => "enum",
        248 => "set",
        249 => "tinyblob",
        250 => "mediumblob",
        251 => "longblob",
        252 => "blob",
        253 => "varchar",
        254 => "char",
        255 => "geometry",
        _ => $"unknown({typeCode})",
    };
}
