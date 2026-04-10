using KloudShift.DebeTail.Abstractions;
using KloudShift.DebeTail.Sources.Postgres.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using NpgsqlTypes;

namespace KloudShift.DebeTail.Sources.Postgres;

/// <summary>
/// Reads change events from a Postgres WAL stream using logical replication (pgoutput protocol).
/// Implements <see cref="ISourceConnector"/> for use with the DebeTail pipeline.
/// </summary>
public sealed partial class PostgresSourceConnector : ISourceConnector
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
    /// Advances the Postgres WAL position, signaling that data up to <paramref name="offset"/>
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
