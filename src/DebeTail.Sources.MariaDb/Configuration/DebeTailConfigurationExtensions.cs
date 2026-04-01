using KloudShift.DebeTail.Abstractions;
using KloudShift.DebeTail.Pipeline.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KloudShift.DebeTail.Sources.MariaDb.Configuration;

/// <summary>Extension methods on <see cref="KloudShift.DebeTail.Pipeline.Configuration.DebeTailConfiguration"/> for registering the MySQL/MariaDB source connector.</summary>
public static class DebeTailConfigurationExtensions
{
    /// <summary>
    /// Registers a MySQL or MariaDB binlog source connector.
    /// </summary>
    public static DebeTailConfiguration UseMariaDb(
        this DebeTailConfiguration config,
        Action<MariaDbConnectorOptions> configure)
    {
        config.Services.Configure(configure);
        
        // Single instance, but two resolution paths
        config.Services.AddSingleton<MariaDbSourceConnector>();
        config.Services.AddSingleton<ISourceConnector>(sp =>
            sp.GetRequiredService<MariaDbSourceConnector>());

        return config;
    }
}
