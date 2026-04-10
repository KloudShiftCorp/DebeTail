using System.Text.Json;
using KloudShift.DebeTail.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace KloudShift.DebeTail.Pipeline.OffsetStores;

/// <summary>
/// File-based <see cref="IOffsetStore"/> that writes one JSON file per connector to a local directory.
/// <para>
/// Suitable for single-process deployments. Each save writes to a <c>.tmp</c> file first and then
/// renames it over the target, minimising the risk of a corrupt offset file on an unexpected shutdown.
/// </para>
/// <para>
/// For distributed or multi-instance deployments consider a shared store such as Redis
/// (available as a commercial package).
/// </para>
/// </summary>
public sealed class FileOffsetStore : IOffsetStore
{
    private static readonly char[] InvalidChars = Path.GetInvalidFileNameChars();

    private readonly string _directory;
    private readonly ILogger<FileOffsetStore> _logger;

    /// <param name="directory">
    /// Directory in which offset files are stored. Created on first save if it does not exist.
    /// Relative paths are resolved against the current working directory.
    /// </param>
    /// <param name="logger">Optional logger. Defaults to a no-op logger if not provided.</param>
    public FileOffsetStore(string directory, ILogger<FileOffsetStore>? logger = null)
    {
        _directory = Path.GetFullPath(directory);
        _logger = logger ?? NullLogger<FileOffsetStore>.Instance;
    }

    /// <inheritdoc/>
    public async ValueTask<ChangeOffset?> LoadAsync(string connectorId, CancellationToken ct = default)
    {
        var path = FilePath(connectorId);

        if (!File.Exists(path))
        {
            _logger.LogDebug("No offset file found for connector '{ConnectorId}' at {Path} — starting fresh",
                connectorId, path);
            return null;
        }

        var json = await File.ReadAllTextAsync(path, ct);
        var entry = JsonSerializer.Deserialize<OffsetEntry>(json);

        if (entry?.Value is not { Length: > 0 })
        {
            _logger.LogWarning(
                "Offset file for connector '{ConnectorId}' at {Path} exists but contains no valid offset value",
                connectorId, path);
            return null;
        }

        _logger.LogInformation(
            "Loaded offset for connector '{ConnectorId}': {Offset} (saved at {SavedAt:O})",
            connectorId, entry.Value, entry.SavedAt);

        return new ChangeOffset(entry.Value);
    }

    /// <inheritdoc/>
    public async ValueTask SaveAsync(string connectorId, ChangeOffset offset, CancellationToken ct = default)
    {
        Directory.CreateDirectory(_directory);

        var path = FilePath(connectorId);
        var tmp = path + ".tmp";

        var entry = new OffsetEntry(offset.Value, DateTimeOffset.UtcNow);
        var json = JsonSerializer.Serialize(entry);

        await File.WriteAllTextAsync(tmp, json, ct);

        // Rename over target — atomic on the same filesystem (POSIX), near-atomic on Windows.
        File.Move(tmp, path, overwrite: true);

        _logger.LogDebug("Saved offset for connector '{ConnectorId}': {Offset} → {Path}",
            connectorId, offset.Value, path);
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private string FilePath(string connectorId)
    {
        // Replace file-system-invalid characters (e.g. ':' in "mariadb:host:3306") with underscores.
        var safe = string.Concat(connectorId.Select(c => Array.IndexOf(InvalidChars, c) >= 0 ? '_' : c));
        return Path.Combine(_directory, $"{safe}.json");
    }

    private sealed record OffsetEntry(string Value, DateTimeOffset SavedAt);
}