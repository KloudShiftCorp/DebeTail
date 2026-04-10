using Microsoft.Extensions.Logging;
using Npgsql;

namespace KloudShift.DebeTail.Sources.Postgres;

public sealed partial class PostgresSourceConnector
{
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
                "select pg_create_logical_replication_slot($1, 'pgoutput', false)");
            cmd.Parameters.AddWithValue(_options.SlotName);
            await cmd.ExecuteNonQueryAsync(ct);

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
            var pubName = QuoteIdentifier(_options.PublicationName);
            var sql = _options.TableNames.Count > 0
                ? $"create publication {pubName} for table {string.Join(", ", _options.TableNames.Select(QuoteTableName))}"
                : $"create publication {pubName} for all tables";

            await using var cmd = _dataSource.CreateCommand(sql);
            await cmd.ExecuteNonQueryAsync(ct);

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
            await using var cmd = _dataSource.CreateCommand("select pg_drop_replication_slot($1)");
            cmd.Parameters.AddWithValue(_options.SlotName);
            await cmd.ExecuteNonQueryAsync(ct);
            _logger.LogInformation("Dropped replication slot: {SlotName}", _options.SlotName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to drop replication slot: {SlotName}", _options.SlotName);
        }

        try
        {
            await using var cmd = _dataSource.CreateCommand(
                $"drop publication {QuoteIdentifier(_options.PublicationName)}");
            await cmd.ExecuteNonQueryAsync(ct);
            _logger.LogInformation("Dropped publication: {PublicationName}", _options.PublicationName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to drop publication: {PublicationName}", _options.PublicationName);
        }
    }
}
