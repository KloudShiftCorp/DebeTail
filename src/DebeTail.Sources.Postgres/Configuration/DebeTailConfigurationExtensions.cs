using KloudShift.DebeTail.Abstractions;
using KloudShift.DebeTail.Pipeline.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KloudShift.DebeTail.Sources.Postgres.Configuration;

/// <summary>Extension methods on <see cref="KloudShift.DebeTail.Pipeline.Configuration.DebeTailConfiguration"/> for registering the Postgres source connector.</summary>
public static class DebeTailConfigurationExtensions
{
    /// <summary>
    /// Registers a Postgres logical replication source connector.
    /// </summary>
    public static DebeTailConfiguration UsePostgres(
        this DebeTailConfiguration config,
        Action<PostgresConnectorOptions> configure)
    {
        config.Services.Configure(configure);
        
        // Single instance, but two resolution paths
        config.Services.AddSingleton<PostgresSourceConnector>();
        config.Services.AddSingleton<ISourceConnector>(sp =>
            sp.GetRequiredService<PostgresSourceConnector>());

        return config;
    }
}
