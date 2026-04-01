using MySqlConnector;

namespace KloudShift.DebeTail.Sources.MariaDb.Internal;

/// <summary>
/// Caches table column names fetched from INFORMATION_SCHEMA.
/// Used as a fallback when the server does not embed column names in binlog events.
/// MySQL 8.0+ supports <c>binlog_row_metadata=FULL</c> to embed column names in
/// <c>TableMapEvent.TableMetadata.ColumnNames</c>; MariaDB always uses this fallback.
/// </summary>
internal sealed class TableSchemaCache(string connectionString)
{
    // keyed by "schema.table"
    private readonly Dictionary<string, IReadOnlyList<string>> _cache = [];

    /// <summary>
    /// Returns the ordered list of column names for the given table, fetching from
    /// INFORMATION_SCHEMA on the first call and caching thereafter.
    /// </summary>
    public async Task<IReadOnlyList<string>> GetColumnNamesAsync(
        string schema, string table, CancellationToken ct)
    {
        var key = $"{schema}.{table}";
        if (_cache.TryGetValue(key, out var cached))
            return cached;

        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(ct);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
            ORDER BY ORDINAL_POSITION
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        cmd.Parameters.AddWithValue("@table", table);

        var names = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            names.Add(reader.GetString(0));

        _cache[key] = names;
        return names;
    }

    /// <summary>Removes a cached entry, forcing a re-fetch on the next access.</summary>
    public void Invalidate(string schema, string table) =>
        _cache.Remove($"{schema}.{table}");
}
