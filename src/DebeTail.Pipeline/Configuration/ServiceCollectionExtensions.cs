using Microsoft.Extensions.DependencyInjection;

namespace KloudShift.DebeTail.Pipeline.Configuration;

/// <summary>Extension methods on <see cref="IServiceCollection"/> for registering DebeTail services.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers DebeTail services and invokes <paramref name="configure"/> to add
    /// source connectors, sink connectors, and serializers via fluent extension methods.
    /// </summary>
    public static IServiceCollection AddDebeTail(
        this IServiceCollection services,
        Action<DebeTailConfiguration> configure)
    {
        configure(new DebeTailConfiguration(services));
        return services;
    }
}
