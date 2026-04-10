using KloudShift.DebeTail.Abstractions;
using KloudShift.DebeTail.Pipeline;
using KloudShift.DebeTail.Pipeline.Configuration;
using KloudShift.DebeTail.Sinks.Console;
using KloudShift.DebeTail.Sinks.Console.Configuration;
using KloudShift.DebeTail.Sources.MariaDb;
using KloudShift.DebeTail.Sources.MariaDb.Configuration;
using KloudShift.DebeTail.Sources.Postgres.Configuration;

namespace DebeTail.Tests.Manual;

public class Program
{
    public static void Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        builder.Services.AddDebeTail(c =>
        {
            c.UsePostgres(o =>
            {
                o.ConnectionString = builder.Configuration.GetConnectionString("Postgres") ??
                                     throw new InvalidOperationException("Postgres connection string not found");
                o.SlotName = "slot1";
                o.PublicationName = "pub1";
                o.AutoConfigureReplication = true;
            });

            // c.UseMariaDb(o =>
            // {
            //     o.ConnectionString = builder.Configuration.GetConnectionString("MariaDb")
            //         ?? throw new InvalidOperationException("MariaDb connection string not found");
            // });
        });

        builder.Services.AddConsoleSink();

        // Build the pipeline using resolved services
        builder.Services.AddHostedService(sp =>
        {
            // Resolve all registered sources and sinks
            var sources = sp.GetRequiredService<IEnumerable<ISourceConnector>>();
            var sinks = sp.GetRequiredService<IEnumerable<ISinkConnector>>();

            var pipeline = DebeTailPipeline
                .From(sources)
                .WatchAll()
                .Transform(e => e with
                {
                    Source = "demo-transformation"
                })
                .Filter(e => e.Operation != DebeTailOperation.Truncate)
                .SinkTo(sinks)
                .WithOptions(o => { o.MaxBatchSize = 100; })
                .WithLoggerFactory(sp.GetRequiredService<ILoggerFactory>())
                .Build();

            return new PipelineHostedService(pipeline);
        });

        var app = builder.Build();

        app.Run();
    }
}

/// <summary>
/// Wraps an <see cref="IDebeTailPipeline"/> in an <see cref="IHostedService"/> so the host
/// manages its lifetime alongside application shutdown.
/// </summary>
file sealed class PipelineHostedService(IDebeTailPipeline pipeline) : IHostedService
{
    public Task StartAsync(CancellationToken ct) => pipeline.StartAsync(ct);

    public Task StopAsync(CancellationToken ct) => pipeline.StopAsync(ct);
}
