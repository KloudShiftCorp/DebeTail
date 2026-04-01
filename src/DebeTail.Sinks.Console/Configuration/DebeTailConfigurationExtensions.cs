using KloudShift.DebeTail.Abstractions;
using KloudShift.DebeTail.Pipeline.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KloudShift.DebeTail.Sinks.Console.Configuration;

/// <summary>Extension methods on <see cref="KloudShift.DebeTail.Pipeline.Configuration.DebeTailConfiguration"/> for registering the console sink.</summary>
public static class DebeTailConfigurationExtensions
{
    /// <summary>
    /// Registers the console sink. Convenience wrapper around
    /// <see cref="ServiceCollectionExtensions.AddConsoleSink"/> for use inside
    /// <c>AddDebeTail(c =&gt; c.UseConsoleSink())</c>.
    /// </summary>
    public static DebeTailConfiguration UseConsoleSink(this DebeTailConfiguration config)
    {
        config.Services.AddConsoleSink();
        return config;
    }
}
