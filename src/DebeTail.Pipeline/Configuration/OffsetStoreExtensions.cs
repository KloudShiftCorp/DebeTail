using KloudShift.DebeTail.Abstractions;
using KloudShift.DebeTail.Pipeline.OffsetStores;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace KloudShift.DebeTail.Pipeline.Configuration;

/// <summary>Extension methods on <see cref="DebeTailConfiguration"/> for registering offset stores.</summary>
public static class OffsetStoreExtensions
{
    /// <summary>
    /// Registers a <see cref="FileOffsetStore"/> as the <see cref="IOffsetStore"/> implementation.
    /// Connectors that support offset persistence (MySQL/MariaDB, SQL Server) will automatically
    /// use it to resume from the last acknowledged position on restart.
    /// </summary>
    /// <param name="config">The DebeTail configuration builder.</param>
    /// <param name="directory">
    /// Directory in which offset files are stored. Defaults to <c>"offsets"</c> relative to the
    /// current working directory. Created automatically on the first save.
    /// </param>
    public static DebeTailConfiguration UseFileOffsetStore(
        this DebeTailConfiguration config,
        string directory = "offsets")
    {
        config.Services.AddSingleton<IOffsetStore>(sp =>
            new FileOffsetStore(directory, sp.GetService<ILogger<FileOffsetStore>>()));
        return config;
    }
}