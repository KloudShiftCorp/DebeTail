using Microsoft.Extensions.DependencyInjection;

namespace KloudShift.DebeTail.Pipeline.Configuration;

/// <summary>
/// Builder object passed to the <c>AddDebeTail</c> extension method.
/// Source and sink packages hang their own extension methods off this type.
/// </summary>
public sealed class DebeTailConfiguration(IServiceCollection services)
{
    /// <summary>The DI service collection. Source and sink registrations are added here.</summary>
    public IServiceCollection Services { get; } = services
        ?? throw new ArgumentNullException(nameof(services));
}
