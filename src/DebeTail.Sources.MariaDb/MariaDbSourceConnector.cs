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
/// <see cref="AcknowledgeAsync"/> is a no-op — the binlog is pull-based, and position
/// tracking is the responsibility of the caller. Store the last acknowledged
/// <see cref="ChangeOffset"/> and pass it back via <see cref="MariaDbConnectorOptions.InitialOffset"/>
/// on restart to resume from the correct position.
/// </para>
/// </summary>
public sealed partial class MariaDbSourceConnector : ISourceConnector
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

        await foreach (var (header, binlogEvent) in client.Replicate(ct))
        {
            if (binlogEvent is TableMapEvent tableMap)
            {
                _tableMap[tableMap.TableId] = tableMap;
                continue;
            }

            var raw = await DecodeEventAsync(header, binlogEvent, client, ct);
            if (raw is not null) yield return raw;
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
}
