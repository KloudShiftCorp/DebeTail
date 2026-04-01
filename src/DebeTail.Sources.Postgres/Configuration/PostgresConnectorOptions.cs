namespace KloudShift.DebeTail.Sources.Postgres.Configuration;

/// <summary>Configuration options for the Postgres logical replication source connector.</summary>
public sealed class PostgresConnectorOptions
{
    /// <summary>Npgsql-format connection string for the source database.</summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Name of the logical replication slot to consume.</summary>
    public string SlotName { get; set; } = string.Empty;

    /// <summary>Name of the publication that lists the tables to replicate.</summary>
    public string PublicationName { get; set; } = string.Empty;

    /// <summary>Tables to watch in "schema.table" format. An empty list watches all tables in the publication.</summary>
    public List<string> TableNames { get; set; } = [];

    /// <summary>
    /// When true, the connector automatically creates the replication slot and publication
    /// on startup, and drops them on shutdown. Useful for development; in production you
    /// may prefer to manage these manually.
    /// </summary>
    public bool AutoConfigureReplication { get; set; }
}
