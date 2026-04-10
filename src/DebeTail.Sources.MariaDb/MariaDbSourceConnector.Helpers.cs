using KloudShift.DebeTail.Abstractions;
using KloudShift.DebeTail.Sources.MariaDb.Configuration;
using Microsoft.Extensions.Logging;
using MySqlCdc;
using MySqlConnector;

namespace KloudShift.DebeTail.Sources.MariaDb;

public sealed partial class MariaDbSourceConnector
{
        /// <summary>
    /// Resolves the starting binlog position using the following priority order:
    /// <list type="number">
    ///   <item>Persisted offset from <see cref="IOffsetStore"/> (if configured and non-empty).</item>
    ///   <item><see cref="MariaDbConnectorOptions.InitialOffset"/> from options (if set).</item>
    ///   <item>Current binlog position queried from the server via <c>SHOW BINARY LOG STATUS</c>
    ///   (MySQL 8.2+) or <c>SHOW MASTER STATUS</c> (MariaDB / MySQL &lt; 8.2).</item>
    /// </list>
    /// </summary>
    private async Task<BinlogOptions> ResolveBinlogOptionsAsync(CancellationToken ct)
    {
        // 1. Offset store — takes priority so restarts always resume from the last acked position.
        if (_offsetStore is not null)
        {
            var stored = await _offsetStore.LoadAsync(ConnectorId, ct);
            if (stored is not null)
            {
                var parts = stored.Value.Split('|', 2);
                if (parts.Length == 2 && long.TryParse(parts[1], out var storedPosition))
                {
                    _logger.LogInformation(
                        "Resuming MySQL/MariaDB binlog from stored offset: {Offset}", stored.Value);
                    return BinlogOptions.FromPosition(parts[0], storedPosition);
                }

                _logger.LogWarning(
                    "Stored offset '{Offset}' could not be parsed; falling back to InitialOffset", stored.Value);
            }
        }

        // 2. InitialOffset from options.
        if (!string.IsNullOrWhiteSpace(_options.InitialOffset))
        {
            var parts = _options.InitialOffset.Split('|', 2);

            if (parts.Length == 2 && long.TryParse(parts[1], out var savedPosition))
                return BinlogOptions.FromPosition(parts[0], savedPosition);

            _logger.LogWarning(
                "InitialOffset '{InitialOffset}' could not be parsed; starting from current binlog position",
                _options.InitialOffset);
        }

        // MySQL 8.2+ removed SHOW MASTER STATUS in favor of SHOW BINARY LOG STATUS.
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
                // Statement isn't supported on this server version — try the next one.
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
