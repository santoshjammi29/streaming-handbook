# Module 03: Confluent Schema Registry & Binary Avro Serialization

## 1. Wire Format Wire Framing
Messages registered with Schema Registry prepend a 5-byte header to the payload:
- **Byte 0**: Magic Byte (`0x00`).
- **Bytes 1-4**: 4-byte Schema ID registered in Schema Registry.
- **Bytes 5+**: Raw Avro/Protobuf binary serialized payload.
