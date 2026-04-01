// ReSharper disable CheckNamespace

namespace KloudShift.DebeTail.Abstractions;

/// <summary>
/// Converts a <see cref="DebeTailEnvelope"/> to and from a byte representation for wire transmission.
/// <para>
/// Implementations ship as independent NuGet packages:
/// JSON is free (<c>DebeTail.Serializers.Json</c>); Avro, Protobuf, and CloudEvents
/// are commercial. New formats can be added without changing the pipeline.
/// </para>
/// </summary>
public interface ISerializer
{
    /// <summary>
    /// MIME content type of the serialized bytes, e.g. "application/json",
    /// "application/avro", "application/cloudevents+json".
    /// Downstream consumers use this to select the correct deserializer.
    /// </summary>
    string ContentType { get; }

    /// <summary>Serializes the envelope to bytes.</summary>
    ReadOnlyMemory<byte> Serialize(DebeTailEnvelope envelope);

    /// <summary>Deserializes bytes back into an envelope.</summary>
    DebeTailEnvelope Deserialize(ReadOnlyMemory<byte> data);
}
