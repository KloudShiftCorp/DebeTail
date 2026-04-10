using System.Runtime.CompilerServices;
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
/// <b>At-least-once delivery across restarts</b> requires an <see cref="IOffsetStore"/>. When
/// one is injected, <see cref="AcknowledgeAsync"/> persists the offset after each successfully
/// flushed batch, and <see cref="ReadAsync"/> loads it on startup so the stream resumes from the
/// correct position automatically. Without an offset store the connector falls back to
/// <see cref="MariaDbConnectorOptions.InitialOffset"/>; if that is also empty it starts from the
/// current end of the binlog (new changes only).
/// </para>
/// </summary>
public sealed partial class MariaDbSourceConnector : ISourceConnector
{
    private readonly MariaDbConnectorOptions _options;
    private readonly ILogger<MariaDbSourceConnector> _logger;
    private readonly HashSet<string> _watchedTables;
    private readonly TableSchemaCache _schemaCache;
    private readonly MySqlConnectionStringBuilder _parsedCs;
    private readonly IOffsetStore? _offsetStore;

    // tableId → TableMapEvent, rebuilt when the connector starts.
    // Accessed only from within the single Replicate loop, so no locking needed.
    private readonly Dictionary<long, TableMapEvent> _tableMap = [];

    /// <summary>Initializes the connector and derives <see cref="ConnectorId"/> from the connection string.</summary>
    /// <param name="logger">Logger instance.</param>
    /// <param name="options">Connector options.</param>
    /// <param name="offsetStore">
    /// Optional offset store. When provided, the last acknowledged binlog position is persisted
    /// automatically and restored on the next startup, enabling at-least-once delivery across
    /// process restarts without manual <see cref="MariaDbConnectorOptions.InitialOffset"/> management.
    /// </param>
    public MariaDbSourceConnector(
        ILogger<MariaDbSourceConnector> logger,
        IOptions<MariaDbConnectorOptions> options,
        IOffsetStore? offsetStore = null)
    {
        _logger = logger;
        _options = options.Value;
        _offsetStore = offsetStore;

        _parsedCs = new MySqlConnectionStringBuilder(_options.ConnectionString);
        _schemaCache = new TableSchemaCache(_options.ConnectionString);
        _watchedTables = _options.TableNames.ToHashSet(StringComparer.OrdinalIgnoreCase);

        ConnectorId = $"mariadb:{_parsedCs.Server}:{_parsedCs.Port}";
    }

    /// <inheritdoc/>
    public string ConnectorId { get; }

    /// <summary>
    /// Opens the MySQL/MariaDB binlog stream and yields change events as they arrive.
    /// Resumes from the position stored in <see cref="IOffsetStore"/> (if configured), then from
    /// <see cref="MariaDbConnectorOptions.InitialOffset"/>, then from the current binlog end.
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

        await foreach (var (header, binlogEvent) in client.Replicate(ct))
        {
            if (binlogEvent is TableMapEvent tableMap)
            {
                _tableMap[tableMap.TableId] = tableMap;
                continue;
            }

            foreach (var raw in await DecodeEventAsync(header, binlogEvent, client, ct))
                yield return raw;
        }
    }

    /// <summary>
    /// Persists <paramref name="offset"/> to the configured <see cref="IOffsetStore"/> so the
    /// connector can resume from this position on the next startup.
    /// If no offset store is configured, this method is a no-op — see class remark.
    /// </summary>
    /// <inheritdoc/>
    public async Task AcknowledgeAsync(ChangeOffset offset, CancellationToken ct = default)
    {
        if (_offsetStore is null || offset == ChangeOffset.None)
            return;

        await _offsetStore.SaveAsync(ConnectorId, offset, ct);
        _logger.LogDebug("Persisted binlog offset: {Offset}", offset.Value);
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
