namespace KloudShift.DebeTail.Sources.MariaDb.Configuration;

/// <summary>Configuration options for the MySQL/MariaDB binlog source connector.</summary>
public sealed class MariaDbConnectorOptions
{
    /// <summary>
    /// MySqlConnector-format connection string.
    /// Example: "Server=localhost;Port=3306;Database=mydb;User Id=repl_user;Password=secret;"
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Unique server ID for this replica. Must not conflict with any other replica
    /// or the primary server's own server_id.
    /// </summary>
    public int ServerId { get; set; } = 65535;

    /// <summary>
    /// Tables to capture in "schema.table" format. An empty list captures all tables.
    /// </summary>
    public List<string> TableNames { get; set; } = [];

    /// <summary>
    /// Starting binlog position in "{filename}|{position}" format. When empty the connector
    /// starts from the end of the current binlog (captures only new changes).
    /// Populate this with the last acknowledged <see cref="KloudShift.DebeTail.Abstractions.ChangeOffset"/>
    /// to resume after a restart.
    /// </summary>
    public string InitialOffset { get; set; } = string.Empty;
}
