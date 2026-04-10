using Npgsql;

namespace KloudShift.DebeTail.Sources.Postgres;

public sealed partial class PostgresSourceConnector
{
    // Doubles any embedded double-quotes and wraps the result in double-quotes,
    // producing a safely quoted Postgres identifier.
    private static string QuoteIdentifier(string identifier) => '"' + identifier.Replace("\"", "\"\"") + '"';

    // Handles "schema.table" by quoting each part separately.
    private static string QuoteTableName(string name)
    {
        var dot = name.IndexOf('.');
        return dot < 0
            ? QuoteIdentifier(name)
            : $"{QuoteIdentifier(name[..dot])}.{QuoteIdentifier(name[(dot + 1)..])}";
    }

    private static string ExtractHostFromConnectionString(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        return builder.Host ?? "unknown";
    }
}
