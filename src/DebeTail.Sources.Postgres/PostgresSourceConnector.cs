using KloudShift.DebeTail.Abstractions;
using KloudShift.DebeTail.Sources.Postgres.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;

namespace KloudShift.DebeTail.Sources.Postgres;

/// <summary>
/// Reads change events from a Postgres WAL stream using logical replication (pgoutput protocol).
/// Implements <see cref="ISourceConnector"/> for use with the DebeTail pipeline.
/// </summary>
public sealed class PostgresSourceConnector : ISourceConnector
{
    private readonly PostgresConnectorOptions _options;
    private readonly ILogger<PostgresSourceConnector> _logger;
    private readonly NpgsqlDataSource _dataSource;

    // Shared with AcknowledgeAsync so it can advance the WAL position after sink confirms durability.
    private volatile LogicalReplicationConnection? _replicationConnection;

    /// <summary>Initializes the connector and derives <see cref="ConnectorId"/> from the connection string.</summary>
    public PostgresSourceConnector(
        ILogger<PostgresSourceConnector> logger,
        IOptions<PostgresConnectorOptions> options)
    {
        _logger = logger;
        _options = options.Value;
        _dataSource = NpgsqlDataSource.Create(_options.ConnectionString);
        
        var host = ExtractHostFromConnectionString(_options.ConnectionString);
        ConnectorId = $"postgres:{host}:{_options.SlotName}";
    }

    /// <inheritdoc/>
    public string ConnectorId { get; }

    /// <summary>
    /// Opens the Postgres replication stream and yields change events as they arrive.
    /// Cancelling <paramref name="ct"/> triggers a clean shutdown; the slot and publication
    /// are removed on exit if <see cref="PostgresConnectorOptions.AutoConfigureReplication"/> is set.
    /// </summary>
    public async IAsyncEnumerable<RawChangeEvent> ReadAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        await EnableReplicationAsync(ct);

        _logger.LogInformation("Opening replication connection to Postgres");
        var connection = new LogicalReplicationConnection(_options.ConnectionString);
        _replicationConnection = connection;

        try
        {
            await connection.Open(ct);

            var slot = new PgOutputReplicationSlot(_options.SlotName);
            var replicationOptions = new PgOutputReplicationOptions(
                _options.PublicationName, PgOutputProtocolVersion.V1);

            await foreach (var message in connection.StartReplication(slot, replicationOptions, ct))
            {
                _logger.LogTrace("{ServerClock}: {MessageType}", message.ServerClock, message.GetType().Name);

                var raw = await DecodeMessageAsync(message, ct);
                if (raw is not null)
                    yield return raw;
            }
        }
        finally
        {
            _replicationConnection = null;
            await connection.DisposeAsync();

            // Use CancellationToken.None for cleanup — the provided ct is already canceled
            // at this point (triggered by the pipeline shutting down), so we need an independent
            // token to successfully send the DROP commands to Postgres.
            await DisableReplicationAsync(CancellationToken.None);
        }
    }

    /// <summary>
    /// Advances the Postgres WAL position, signalling that data up to <paramref name="offset"/>
    /// has been durably written by the downstream sink. Only called by the pipeline after a
    /// successful <see cref="ISinkConnector.FlushAsync"/>.
    /// </summary>
    public Task AcknowledgeAsync(ChangeOffset offset, CancellationToken ct = default)
    {
        if (offset == ChangeOffset.None)
            return Task.CompletedTask;

        var connection = _replicationConnection;
        if (connection is null)
            return Task.CompletedTask;

        if (NpgsqlLogSequenceNumber.TryParse(offset.Value, out var lsn))
            connection.SetReplicationStatus(lsn);
        else
            _logger.LogWarning("Could not parse WAL offset for acknowledgement: {Offset}", offset.Value);

        return Task.CompletedTask;
    }

    #region Replication slot & publication management

    private async Task EnableReplicationAsync(CancellationToken ct)
    {
        if (!_options.AutoConfigureReplication)
        {
            _logger.LogInformation("Skipping auto-configuration of replication (AutoConfigureReplication = false)");
            return;
        }

        await CreateReplicationSlotAsync(ct);
        await CreatePublicationAsync(ct);
    }

    private async Task CreateReplicationSlotAsync(CancellationToken ct)
    {
        try
        {
            await using var cmd = _dataSource.CreateCommand(
                $"select pg_create_logical_replication_slot('{_options.SlotName}', 'pgoutput', false)");
            await cmd.ExecuteReaderAsync(ct);
            
            _logger.LogInformation("Created replication slot: {SlotName}", _options.SlotName);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.DuplicateObject)
        {
            // Slot already exists — this is expected on restart; not an error.
            _logger.LogInformation("Replication slot already exists: {SlotName}", _options.SlotName);
        }
    }

    private async Task CreatePublicationAsync(CancellationToken ct)
    {
        try
        {
            var sql = _options.TableNames.Count > 0
                ? $"create publication {_options.PublicationName} for table {string.Join(", ", _options.TableNames)}"
                : $"create publication {_options.PublicationName} for all tables";

            await using var cmd = _dataSource.CreateCommand(sql);
            await cmd.ExecuteReaderAsync(ct);

            _logger.LogInformation("Created publication: {PublicationName}", _options.PublicationName);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.DuplicateObject)
        {
            _logger.LogInformation("Publication already exists: {PublicationName}", _options.PublicationName);
        }
    }

    private async Task DisableReplicationAsync(CancellationToken ct)
    {
        if (!_options.AutoConfigureReplication)
            return;

        try
        {
            await using var cmd = _dataSource.CreateCommand(
                $"select pg_drop_replication_slot('{_options.SlotName}')");
            await cmd.ExecuteReaderAsync(ct);
            _logger.LogInformation("Dropped replication slot: {SlotName}", _options.SlotName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to drop replication slot: {SlotName}", _options.SlotName);
        }

        try
        {
            await using var cmd = _dataSource.CreateCommand(
                $"drop publication {_options.PublicationName}");
            await cmd.ExecuteReaderAsync(ct);
            _logger.LogInformation("Dropped publication: {PublicationName}", _options.PublicationName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to drop publication: {PublicationName}", _options.PublicationName);
        }
    }

    #endregion

    #region Message decoding

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
        return BuildEvent(msg.Relation, msg.ServerClock, msg.WalEnd, DebeTailOperation.Insert, before: null, after, columns);
    }

    private async Task<RawChangeEvent> DecodeDefaultUpdateAsync(DefaultUpdateMessage msg, CancellationToken ct)
    {
        // DefaultUpdate: only NewRow is available (default replica identity — primary key only for OLD)
        var (after, columns) = await ExtractRowAsync(msg.NewRow, msg.Relation, ct);
        return BuildEvent(msg.Relation, msg.ServerClock, msg.WalEnd, DebeTailOperation.Update, before: null, after, columns);
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
        return BuildEvent(msg.Relation, msg.ServerClock, msg.WalEnd, DebeTailOperation.Update, before: null, after, columns);
    }

    private async Task<RawChangeEvent> DecodeFullDeleteAsync(FullDeleteMessage msg, CancellationToken ct)
    {
        var (before, columns) = await ExtractRowAsync(msg.OldRow, msg.Relation, ct);
        return BuildEvent(msg.Relation, msg.ServerClock, msg.WalEnd, DebeTailOperation.Delete, before, after: null, columns);
    }

    private async Task<RawChangeEvent> DecodeKeyDeleteAsync(KeyDeleteMessage msg, CancellationToken ct)
    {
        // KeyDelete: the default replica identity sends only the primary-key columns in Key.
        // This is the common case for tables without REPLICA IDENTITY FULL.
        var (keyColumns, columns) = await ExtractRowAsync(msg.Key, msg.Relation, ct);
        return BuildEvent(msg.Relation, msg.ServerClock, msg.WalEnd, DebeTailOperation.Delete, before: keyColumns, after: null, columns);
    }
    
    private static string ExtractHostFromConnectionString(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        return builder.Host ?? "unknown";
    }

    #endregion

    #region Row extraction helpers

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

    #endregion

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_replicationConnection is { } conn)
        {
            _replicationConnection = null;
            await conn.DisposeAsync();
        }
        await _dataSource.DisposeAsync();
    }
}
