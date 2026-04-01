# DebeTail

A .NET-native CDC (Change Data Capture) library. The closest analogy is [Debezium](https://debezium.io/), but idiomatic
C# and targeting enterprise .NET consumers.

Reads database write events (INSERT, UPDATE, DELETE) via native replication mechanisms, normalizes them into a
`DebeTailEnvelope`, and routes them to message brokers or storage sinks.

---

## Requirements

- .NET 10.0 SDK
- PostgreSQL 13+ with logical replication enabled (for the Postgres connector)
- MySQL 5.7+ / 8.0+ or MariaDB 10.x+ with `binlog_format=ROW` and a unique `server_id` (for the MySQL/MariaDB connector)
- SQL Server 2016+ with CDC enabled (for the SQL Server connector)

---

## Packages

### Free / OSS (MIT)

| Package                      | Purpose                                                                                                            |
|------------------------------|--------------------------------------------------------------------------------------------------------------------|
| `DebeTail.Abstractions`     | Interfaces and models. Zero external dependencies — the contract every package references.                         |
| `DebeTail.Pipeline`         | Pipeline runtime: fluent `DebeTailPipeline` builder, async event loop, batching, offset tracking, DI registration. |
| `DebeTail.Sources.Postgres` | Postgres WAL logical replication source via Npgsql.                                                                |
| `DebeTail.Sources.MariaDb`  | MySQL and MariaDB binlog source via MySqlCdc.                                                                      |
| `DebeTail.Sinks.Console`    | Stdout sink for local dev and containerised deployments.                                                           |
| `DebeTail.Testing`          | Framework-agnostic test doubles and builders for pipeline tests.                                                   |

### Commercial (BSL)

| Package                                     | Purpose                                       |
|---------------------------------------------|-----------------------------------------------|
| `DebeTail.Sources.SqlServer`                | SQL Server CDC source via Microsoft.Data.SqlClient.           |
| `DebeTail.Sinks.AzureBlob`                  | Azure Blob Storage sink.                                      |
| `DebeTail.Serializers.CloudEvents`          | CloudEvents v1.0 structured JSON serializer.                  |
| `DebeTail.Sinks.Kafka`                      | Apache Kafka sink.                                            |
| `DebeTail.Sinks.EventHub` _(planned)_       | Azure Event Hubs sink.                                        |
| `DebeTail.Sinks.RabbitMq`                   | RabbitMQ sink.                                                |
| `DebeTail.Sinks.S3` _(planned)_             | AWS S3 sink.                                                  |
| `DebeTail.Serializers.Avro` _(planned)_     | Avro serializer with Schema Registry support.                 |
| `DebeTail.Serializers.Protobuf` _(planned)_ | Protobuf serializer.                                          |
| `DebeTail.Enrichment`                       | Snapshot and lookup-based enrichment for CDC pipelines.       |
| `DebeTail.Telemetry` _(planned)_            | OpenTelemetry metrics and tracing.                            |

---

## Quick start

### 1. With the hosted service / DI pattern

```csharp
using KloudShift.DebeTail.Pipeline.Configuration;
using KloudShift.DebeTail.Sources.Postgres.Configuration;
using KloudShift.DebeTail.Sinks.Console.Configuration;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddLogging(o => o.AddSimpleConsole());
builder.Services.AddDebeTail(c =>
{
    c.UsePostgres(o =>
    {
        o.ConnectionString = "Host=localhost;Database=mydb;Username=postgres;Password=...";
        o.SlotName = "my_slot";
        o.PublicationName = "my_pub";
        o.TableNames = ["public.orders", "public.customers"];
        o.AutoConfigureReplication = true; // creates slot & publication automatically
    });

    c.UseConsoleSink();
});

// Wire up the pipeline as a hosted service
builder.Services.AddHostedService(sp =>
{
    var source = sp.GetRequiredService<ISourceConnector>();
    var sink   = sp.GetRequiredService<ISinkConnector>();

    var pipeline = DebeTailPipeline
        .From(source)
        .Watch("public.orders", "public.customers")
        .Filter(e => e.Operation != DebeTailOperation.Delete)
        .Transform(e => e with { Source = "my-app" })
        .SinkTo(sink)
        .Build();

    return new PipelineHostedService(pipeline);
});

await builder.Build().RunAsync();
```

### MySQL / MariaDB (DI pattern)

```csharp
builder.Services.AddDebeTail(c =>
{
    c.UseMariaDb(o =>
    {
        o.ConnectionString = "Server=localhost;Port=3306;Database=mydb;User Id=repl_user;Password=...;";
        o.ServerId        = 100;
        o.TableNames      = ["mydb.orders", "mydb.customers"];
        o.InitialOffset   = ""; // leave empty to start from end; set to resume
    });

    c.UseConsoleSink();
});
```

### 2. Standalone (no DI)

```csharp
await using var source = new PostgresSourceConnector(logger, options);
await using var sink   = new ConsoleSinkConnector(logger);

var pipeline = DebeTailPipeline
    .From(source)
    .WatchAll()
    .SinkTo(sink)
    .WithOptions(o => { o.MaxBatchSize = 50; o.MaxBatchAge = TimeSpan.FromSeconds(1); })
    .Build();

await pipeline.StartAsync(cancellationToken);
```

---

## Pipeline architecture

```
[ Source layer ]
  Postgres | SQL Server 2016+ | MySQL/MariaDB | Custom (ISourceConnector)
  
       ⬇️ IAsyncEnumerable<RawChangeEvent>
       
[ Pipeline core ]
  Watch ➡️ Normalize ➡️ Filter ➡️ Transform ➡️ Batch
  
       ⬇️ IReadOnlyList<DebeTailEnvelope>
       
[ Sink layer ]
  Console | Kafka | Event Hub | RabbitMQ | ...
```

**At-least-once delivery:** the pipeline calls `ISinkConnector.FlushAsync()` to confirm a durable write, then calls
`ISourceConnector.AcknowledgeAsync(offset)` to advance the WAL/binlog position. Offset advancement never precedes sink
confirmation.

---

## Delivery guarantee

DebeTail provides **at-least-once delivery**: every change event is written to the sink at least once. Events may be
delivered more than once after a crash or a retry. Consumers are responsible for deduplication (see below).

### The ordering contract

For every batch, `DebeTailPipelineRunner` enforces this exact sequence:

```
WriteAsync(batch):          send events to the sink (may buffer internally)
FlushAsync():               durability point: sink persists all buffered data
AcknowledgeAsync(offset):   source advances its WAL/binlog position
```

`AcknowledgeAsync` is **never** called unless `FlushAsync` returns successfully. This means the source offset only
advances after the sink has confirmed a durable write.

### Crash recovery

If the pipeline crashes or is killed between `FlushAsync` and `AcknowledgeAsync`, the source offset has not advanced. On
the next startup:

- **Postgres**: The replication slot still holds the original WAL position; the database replays all unacknowledged
  events automatically.
- **MySQL / MariaDB**: `AcknowledgeAsync` is a no-op (the binlog is pull-based). You must persist the last
  `ChangeOffset.Value` yourself and pass it back as `InitialOffset` on restart.
  See [Resuming after restart](#resuming-after-restart).

### Batching and offset tracking

The pipeline accumulates events into a batch (up to `MaxBatchSize` events or `MaxBatchAge` elapsed, whichever comes
first). It tracks the offset of the last event added to the batch. When `AcknowledgeAsync` is called with that single
offset, the source interprets it as "all events up to and including this position have been durably written."

Filtered-out events (table watch list, user-defined predicates) do not advance `lastOffset` — only events that actually
enter the batch do.

### Retry and dead-letter behavior

When `WriteAsync` or `FlushAsync` throws, the pipeline retries the **entire batch** up to `MaxRetries` times with
`RetryDelay` between attempts. `AcknowledgeAsync` is never called for a failing batch.

| Option       | Default | Description                               |
|--------------|---------|-------------------------------------------|
| `MaxRetries` | `3`     | Number of retry attempts before giving up |
| `RetryDelay` | `1s`    | Delay between attempts                    |

Once all retries are exhausted:

- If a **dead-letter sink** is configured, the batch is written there and the offset is **not** acknowledged (events
  will replay on the primary sink on restart).
- Otherwise, the exception propagates and the pipeline stops.

```csharp
// Configure retries
var pipeline = DebeTailPipeline
    .From(source)
    .WatchAll()
    .WithOptions(o =>
    {
        o.MaxRetries  = 5;
        o.RetryDelay  = TimeSpan.FromSeconds(2);
    })
    .SinkTo(primarySink, deadLetterSink)   // second arg = dead-letter sink
    .Build();
```

### Deduplication

Because retries and crash-recovery replays can re-deliver the same event, consumers must treat delivery as idempotent.
Use `DebeTailEnvelope.EventId` (a `Guid` assigned once during normalization and stable across retries) as the
idempotency key.

---

## Key interfaces (`DebeTail.Abstractions`)

### `ISourceConnector`

```csharp
public interface ISourceConnector : IAsyncDisposable
{
    string ConnectorId { get; }
    IAsyncEnumerable<RawChangeEvent> ReadAsync(CancellationToken ct = default);
    Task AcknowledgeAsync(ChangeOffset offset, CancellationToken ct = default);
}
```

### `ISinkConnector`

```csharp
public interface ISinkConnector : IAsyncDisposable
{
    Task WriteAsync(IReadOnlyList<DebeTailEnvelope> batch, CancellationToken ct = default);
    Task FlushAsync(CancellationToken ct = default);
}
```

### `IChangeNormalizer`

```csharp
public interface IChangeNormalizer
{
    string ConnectorType { get; }
    DebeTailEnvelope Normalize(RawChangeEvent raw);
}
```

### `ISerializer`

```csharp
public interface ISerializer
{
    string ContentType { get; }
    ReadOnlyMemory<byte> Serialize(DebeTailEnvelope envelope);
    DebeTailEnvelope Deserialize(ReadOnlyMemory<byte> data);
}
```

`ISerializer` is a **sink-level concern**. The pipeline itself is wire-format agnostic and always operates on
`DebeTailEnvelope` objects. Serializers are injected into sink connectors, which call `Serialize` inside `WriteAsync`
before transmitting bytes to the broker or storage system:

```
Pipeline (format-agnostic)
  └─ ISinkConnector.WriteAsync(IReadOnlyList<DebeTailEnvelope>)
        └─ KafkaSink (injects ISerializer)
              └─ serializer.Serialize(envelope) → byte[]
                    └─ producer.Produce(topic, bytes)
```

The `ContentType` property (e.g. `"application/cloudevents+json"`, `"application/avro"`) is forwarded as the message
content-type header so downstream consumers can select the correct deserializer.

#### CloudEvents serializer

`DebeTail.Serializers.CloudEvents` (commercial) encodes each `DebeTailEnvelope` as
a [CloudEvents v1.0](https://cloudevents.io/) structured JSON document. The standard CE attributes (`specversion`, `id`,
`source`, `type`, `time`) are at the root; the row images live under `data`:

```json
{
  "specversion": "1.0",
  "id": "a1b2c3d4-...",
  "source": "postgres:orders-db/public.orders",
  "type": "io.debetail.change.insert",
  "datacontenttype": "application/json",
  "time": "2024-06-01T12:00:00+00:00",
  "subject": "public.orders",
  "debetailschema": "public",
  "debetailtable": "orders",
  "debetailoperation": "insert",
  "debetailoffset": "0/1A2B3C4",
  "data": {
    "before": {},
    "after": {
      "id": 1,
      "amount": 99.99
    },
    "columns": [
      {
        "name": "id",
        "nativeType": "integer"
      },
      {
        "name": "amount",
        "nativeType": "numeric"
      }
    ],
    "metadata": {
      "wal_lsn": "0/1A2B3C4"
    }
  }
}
```

To test CloudEvents output locally, register the serializer alongside the Console sink — the Console sink automatically
picks it up via DI:

```csharp
builder.Services.AddDebeTail(c =>
{
    c.UsePostgres(o => { ... });
    c.UseConsoleSink();
});
// Register the serializer — ConsoleSinkConnector will use it automatically
builder.Services.AddCloudEventsSerializer();
```

---

## `DebeTailEnvelope`

The canonical, source-agnostic representation of a change event produced by the pipeline:

```csharp
public sealed record DebeTailEnvelope
{
    // unique per event
    public Guid EventId { get; init; }       
    
    // connector ID, e.g. "postgres:my-slot"
    public string Source { get; init; }     
    
    // e.g. "public"
    public string Schema { get; init; }
    
    // e.g. "orders"
    public string Table { get; init; }
    
    // "{Schema}.{Table}"
    public string Relation { get; }
    
    // Insert | Update | Delete | Truncate
    public DebeTailOperation Operation { get; init; }   
    
    // from DB, not wall clock
    public DateTimeOffset OccurredAt { get; init; }
    
    public IReadOnlyDictionary<string, object?> Before { get; init; }
    
    public IReadOnlyDictionary<string, object?> After { get; init; }
    
    public IReadOnlyList<ColumnSchema> Columns { get; init; }
    
    // opaque LSN / binlog position
    public ChangeOffset Offset { get; init; }  
    
    public IReadOnlyDictionary<string, object?> Metadata { get; init; }
}
```

**Before / After behavior by operation:**

| Operation                         | `Before`                 | `After`      |
|-----------------------------------|--------------------------|--------------|
| Insert                            | empty                    | full row     |
| Update (default replica identity) | empty                    | full new row |
| Update (full replica identity)    | full old row             | full new row |
| Delete (default replica identity) | primary key columns only | empty        |
| Delete (full replica identity)    | full old row             | empty        |

---

## Postgres connector

### Replica identity

Postgres tables default to `REPLICA IDENTITY DEFAULT`, which means WAL records contain only primary-key columns for the
old row on UPDATE and DELETE. To get the full old row image:

```sql
ALTER TABLE public.orders REPLICA IDENTITY FULL;
```

### Auto-configuration

When `AutoConfigureReplication = true`, the connector creates the replication slot and publication on startup and drops
them on shutdown. Useful for development. In production, manage these manually:

```sql
-- Create slot
SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput', false);

-- Create publication
CREATE PUBLICATION my_pub FOR TABLE public.orders, public.customers;
```

### Supported Postgres versions

13, 14, 15, 16, 17 (uses pgoutput protocol V1).

---

## MySQL / MariaDB connector

### Server prerequisites

```sql
-- binlog_format must be ROW (not STATEMENT or MIXED)
SET GLOBAL binlog_format = 'ROW';

-- MySQL 8.0+ only: include column names in binlog events.
-- Not supported on MariaDB — it always uses the INFORMATION_SCHEMA fallback.
-- Without this on MySQL, column names are fetched from INFORMATION_SCHEMA on first use.
SET GLOBAL binlog_row_metadata = 'FULL';

-- Grant the replication user the required privileges
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl_user'@'%';
```

### MariaDB notes

MariaDB is fully supported. The only difference from MySQL is that `binlog_row_metadata=FULL` is not available, so
column names are always fetched from `INFORMATION_SCHEMA.COLUMNS` on the first event from each table. No special
configuration is required beyond `binlog_format=ROW`.

### Resuming after restart

`AcknowledgeAsync` is a no-op — the binlog is pull-based and there is no server-side WAL position to advance. To resume
from a saved position, persist the last `ChangeOffset.Value` (format: `"{binlog_filename}|{position}"`) and pass it back
as `InitialOffset` on restart:

```csharp
// Persist on acknowledge
await File.WriteAllTextAsync("offset.txt", envelope.Offset.Value);

// Restore on startup
o.InitialOffset = await File.ReadAllTextAsync("offset.txt");
```

Leaving `InitialOffset` empty starts from the end of the current binlog (captures only changes that arrive after the
connector starts).

### Column names

Column names are embedded in binlog events only when `binlog_row_metadata=FULL` is set on the server (MySQL 8.0+ only).
When it is not set, or when using MariaDB, the connector falls back to querying `INFORMATION_SCHEMA.COLUMNS` on the
first event from each table and caches the result for the lifetime of the connector.

---

## SQL Server connector

### CDC vs Change Event Streaming (CES)

SQL Server has two distinct change-capture mechanisms:

|                  | **CDC (Change Data Capture)**              | **CES (Change Event Streaming)**           |
|------------------|--------------------------------------------|--------------------------------------------|
| Available since  | SQL Server 2016 (2012 Enterprise)          | SQL Server 2025 (preview)                  |
| Mechanism        | Poll `cdc.fn_cdc_get_all_changes_*` tables | SQL Server pushes to Azure Event Hubs      |
| Azure dependency | None                                       | Required                                   |
| Coexist with CDC | —                                          | No — mutually exclusive                    |
| Pull / push      | **Pull** (DebeTail model)                  | Push (consumer needs EventProcessorClient) |

DebeTail uses **CDC**. CES is a push-based producer that writes to Azure Event Hubs; it is architecturally incompatible
with DebeTail's pull-based `ISourceConnector` model and is currently in preview.

### Server prerequisites

```sql
-- 1. Enable CDC on the database (requires db_owner or sysadmin)
EXEC sys.sp_cdc_enable_db;

-- 2. Enable CDC on each table you want to track
EXEC sys.sp_cdc_enable_table
     @source_schema = N'dbo',
     @source_name = N'orders',
     @role_name = NULL, -- set a role to restrict read access, or NULL for no gate
     @supports_net_changes = 0;

-- 3. Grant the connecting user SELECT on CDC tables (if not db_owner)
GRANT SELECT ON SCHEMA::cdc TO [your_user];
```

### DI registration

```csharp
builder.Services.AddDebeTail(c =>
{
    c.UseSqlServer(o =>
    {
        o.ConnectionString = "Server=localhost;Database=mydb;User Id=sa;Password=...;TrustServerCertificate=true";
        o.TableNames  = ["dbo.orders", "dbo.customers"];
        o.InitialOffset = ""; // leave empty to start from now; set hex LSN to resume
    });

    c.UseConsoleSink();
});
```

### Resuming after restart

`AcknowledgeAsync` is a no-op — CDC position tracking is external. Persist the last `ChangeOffset.Value` (a 20-character
lowercase hex LSN, e.g. `"00000031000027540001"`) and pass it back as `InitialOffset` on restart:

```csharp
// Persist on acknowledge
await File.WriteAllTextAsync("offset.txt", envelope.Offset.Value);

// Restore on startup
o.InitialOffset = await File.ReadAllTextAsync("offset.txt");
```

Leaving `InitialOffset` empty starts from the current maximum LSN — only changes that arrive after the connector starts
are captured.

### Auto-configuration

When `AutoConfigureCdc = true`, the connector runs `sp_cdc_enable_db` and `sp_cdc_enable_table` at startup for each
table in `TableNames`. The connecting user must be a member of `db_owner`. This is useful for development; in production
manage CDC setup separately.

### At-least-once delivery

At-least-once delivery holds **within a single run** automatically: the source's internal `lastLsn` only advances after
all events in a polling window have been yielded to the pipeline. A mid-run crash leaves `lastLsn` at its previous
value, so on restart the events replay.

**Across restarts**, at-least-once requires you to persist the offset externally. `AcknowledgeAsync` is a no-op — there
is no server-side position to advance. You must store the last `ChangeOffset.Value` from a successfully flushed batch
and restore it via `InitialOffset`:

```csharp
// In your sink or application code — persist after each successful batch
await File.WriteAllTextAsync("offset.txt", envelope.Offset.Value);

// On startup — restore the saved position
o.InitialOffset = await File.ReadAllTextAsync("offset.txt");
```

If `InitialOffset` is empty the connector logs a warning and starts from the current max LSN, silently skipping any
changes that accumulated while the pipeline was down.

### CDC retention window

SQL Server's CDC capture job periodically cleans up change table rows. By default it runs every **5 minutes** and
retains data for **3 days**. If the pipeline is down longer than the retention window, events are deleted from the CDC
tables and cannot be replayed — even with a correct `InitialOffset`.

Configure a longer retention window when you need more recovery headroom:

```sql
-- Example: extend retention to 7 days (value is in minutes)
exec sys.sp_cdc_change_job
     @job_type = N'cleanup',
     @retention = 10080; -- 7 days × 24 h × 60 min
```

This is different from Postgres, where the replication slot holds WAL data indefinitely until `AcknowledgeAsync`advances
the slot position — the database itself prevents data loss. SQL Server has no equivalent server-side consumer tracking.

### Supported SQL Server versions

SQL Server 2016, 2017, 2019, 2022, 2025 (uses `cdc.fn_cdc_get_all_changes_*`).

---

## Testing

`DebeTail.Testing` provides framework-agnostic test doubles that work with xUnit, NUnit, MSTest, or no framework:

```csharp
// Deterministic unit test
var source = new InMemorySourceConnector("postgres:test",
[
    new RawChangeEventBuilder().ForTable("public.orders").AsInsert().WithAfter("id", 1).Build(),
    new RawChangeEventBuilder().ForTable("public.orders").AsUpdate().WithAfter("id", 1).Build(),
]);

var sink = new CapturingSinkConnector();

var pipeline = DebeTailPipeline.From(source).WatchAll().SinkTo(sink).Build();
await pipeline.StartAsync();

Assert.Equal(2, sink.AllReceived.Count);
Assert.Equal(DebeTailOperation.Insert, sink.AllReceived[0].Operation);
```

```csharp
// Imperative event pushing
var source = new ControllableSourceConnector("postgres:test");
var sink   = new CapturingSinkConnector();

var pipeline = DebeTailPipeline.From(source).WatchAll().SinkTo(sink).Build();
await pipeline.StartAsync();

await source.EmitAsync(new RawChangeEventBuilder().ForTable("public.orders").AsInsert().Build());
source.Complete();
```

---

## Planned source connectors

| Source           | Mechanism                            | Status              |
|------------------|--------------------------------------|---------------------|
| Postgres         | WAL logical replication (pgoutput)   | ✅ Implemented       |
| MySQL / MariaDB  | Binlog (MySqlCdc)                    | ✅ Implemented       |
| SQL Server 2016+ | CDC (`cdc.fn_cdc_get_all_changes_*`) | ✅ Implemented       |
| MongoDB          | Change streams                       | Under consideration |

---

## Build & test

```bash
# Build free tier
dotnet build DebeTail-Free/DebeTail.slnx

# Test free tier (Postgres integration tests require a live instance)
dotnet test DebeTail-Free/DebeTail.slnx

# Run single test
dotnet test DebeTail-Free/tests/DebeTail.Testing/ --filter "FullyQualifiedName~TestName"

# Restore free tier
dotnet restore DebeTail-Free/DebeTail.slnx

# Pack free tier into local feed (required before building commercial)
dotnet pack DebeTail-Free/DebeTail.slnx -o artifacts

# Build commercial tier
dotnet build DebeTail-Commercial/DebeTail-Commercial.slnx

# Test commercial tier
dotnet test DebeTail-Commercial/DebeTail-Commercial.slnx
```

---

## Licensing

- **Free / MIT:** `DebeTail.Abstractions`, `DebeTail.Pipeline`, `DebeTail.Sources.Postgres`,
  `DebeTail.Sources.MariaDb`, `DebeTail.Sinks.Console`, `DebeTail.Testing`
- **Commercial / BSL:** `DebeTail.Sources.SqlServer`, production sinks (Kafka, Event Hub, RabbitMQ, Blob, S3),
  serializers (Avro, Protobuf, CloudEvents), enrichment engine, schema registry integration, OTel metrics

License model under consideration: BSL (Business Source License) — source-available, commercial use requires a license.
Enterprises can audit source.
