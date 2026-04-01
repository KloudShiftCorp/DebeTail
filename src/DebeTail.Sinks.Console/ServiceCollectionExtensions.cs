using KloudShift.DebeTail.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace KloudShift.DebeTail.Sinks.Console;

/// <summary>Extension methods on <see cref="Microsoft.Extensions.DependencyInjection.IServiceCollection"/> for registering the console sink.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="ConsoleSinkConnector"/> as the <see cref="ISinkConnector"/>
    /// singleton that writes change envelopes to stdout via <see cref="Microsoft.Extensions.Logging.ILogger"/>.
    /// </summary>
    public static IServiceCollection AddConsoleSink(this IServiceCollection services)
    {
        services.AddSingleton<ConsoleSinkConnector>();
        services.AddSingleton<ISinkConnector, ConsoleSinkConnector>(sp =>
            sp.GetRequiredService<ConsoleSinkConnector>());
        return services;
    }
}
