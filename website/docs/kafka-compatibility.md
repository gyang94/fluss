---
sidebar_label: Kafka Compatibility
title: Kafka Compatibility
sidebar_position: 5.5
---

# Kafka Compatibility

Fluss Kafka compatibility maps Kafka topics and records to native Fluss tables. Tables are
created through Fluss DDL or the Flink Fluss catalog, and explicitly enabled for Kafka access.

:::warning
Kafka protocol compatibility is still in development. The current implementation provides
ApiVersions and the DDL mapping contract described here. Kafka Metadata, Produce, and consumer
APIs are not yet available; enabling a table does not enable these APIs.
:::

## Supported APIs

| API or capability | Current status |
|---|---|
| ApiVersions | Implemented. |
| Native CREATE/ALTER and Kafka table mapping validation | Implemented. |
| Kafka Metadata and Produce | Planned for Basic Produce; not yet available. |
| Fetch, consumer groups, and offset commits | Not available. |
| Idempotent and transactional producers | Not available in Basic Produce. |
| CreateTopics and DeleteTopics | Not supported; manage tables through Fluss. |

The mapping and conversion rules below define the table contract for the Kafka write path.
They do not make an unsupported Kafka API available.

## Supported tables

Kafka mapping currently accepts non-partitioned [Log Tables](table-design/table-types/log-table.md) using
the Arrow log format, without a primary key. The initial Kafka write path is designed to
encode Arrow batches and append them to log tables. Supporting other log formats requires
matching encoders; primary key and partitioned tables also require additional write semantics.

Each table must have a compatible schema and `table.kafka.enabled=true`. System columns are
optional: declare only the Kafka information that the table needs to store. Enabling Kafka
does not add columns or persist default conversion properties. CREATE and ALTER validate the
resulting mapping before persisting it, including schema changes on enabled tables. All declared
columns and nested value fields must be nullable. This includes ROW fields, ARRAY elements,
MAP values, and optional system columns. MAP keys retain Fluss's mandatory non-null constraint;
they are the only exception. Validation rejects NOT NULL fields rather than changing their types.

## Cluster configuration

Set `kafka.enabled: true` in the cluster's `server.yaml`. On each TabletServer, add a Kafka
listener alongside its native Fluss listener. For a local TabletServer:

```yaml title="server.yaml"
kafka.enabled: true
kafka.listener.names: KAFKA
kafka.default.database: fluss
bind.listeners: FLUSS://localhost:9123,KAFKA://localhost:9092
```

Kafka listeners belong on TabletServers. For a distributed deployment, configure reachable
addresses with `bind.listeners` and `advertised.listeners`; see the
[server configuration reference](maintenance/configuration.md#common). All TabletServers
and the coordinator must use the same `kafka.default.database`.

## Topic-to-table mapping

Kafka topics map to Fluss tables using `<database>.<table>`. Topic names without a database prefix
map to the same-named table in the database configured by `kafka.default.database` (default: `fluss`).
Kafka clients can access a topic only after its corresponding table has been created in Fluss.

For example, `sales.orders` maps to the `orders` table in `sales`, while `orders` maps to
`fluss.orders` with the default configuration. Reverse mapping always uses the fully qualified
name. Changing the default database redirects unqualified names; use fully qualified topic
names when clients need a stable mapping across such changes.

The native `fluss` database is created when the cluster has no databases. When Kafka is enabled,
the coordinator also ensures that the configured Kafka default database exists, including when
it is `fluss` and other databases already exist. Existing databases are preserved. A database
deleted at runtime is not recreated by a Kafka request; initialization runs when the coordinator
becomes active.
Other databases and all tables must be created explicitly; Kafka does not automatically create
tables, and Kafka CreateTopics/DeleteTopics are not supported.

### Partition-to-bucket mapping

A Kafka partition maps to the same-numbered Fluss bucket. The topic's partition count equals
the table's bucket count. Qualified and unqualified names for the same table share its buckets
and log offsets; same-named tables in different databases remain independent. The write path
uses the bucket's leader and replicas rather than a separate Kafka storage layer.

## Create or enable a table

The examples below run in an existing [Flink Fluss catalog](engine-flink/ddl.md#create-catalog).
Create a table for JSON values, optionally storing the Kafka key and headers:

```sql title="Flink SQL"
CREATE DATABASE IF NOT EXISTS sales;

CREATE TABLE sales.orders (
    order_id BIGINT,
    amount DECIMAL(18, 2),
    _kafka_key BYTES,
    _kafka_headers MAP<STRING, BYTES>
) WITH (
    'table.kafka.enabled' = 'true'
);
```

This defines the mapping for `sales.orders`. JSON fields such as `order_id` and `amount` map to
the business columns, while the Kafka record's key and headers map to their system columns.
The omitted conversion type and layout are read as `parse_json` and `flat` without adding
properties to table metadata.

For an existing table with a compatible JSON schema, enable Kafka:

```sql title="Flink SQL"
ALTER TABLE sales.orders SET (
    'table.kafka.enabled' = 'true'
);
```

Set `table.kafka.enabled` to `false` to disable Kafka access. Disabled tables are not subject
to Kafka-specific schema validation.

## Producer configuration and writes

:::warning
The following example describes the intended Basic Produce usage once Metadata and Produce
are implemented. It cannot write through the current ApiVersions-only endpoint.
:::

Use the Kafka listener as the bootstrap address and disable idempotence. For the `sales.orders`
table above, a Java producer can send UTF-8 JSON values and string keys as follows:

```java
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

Properties properties = new Properties();
properties.put("bootstrap.servers", "localhost:9092");
properties.put("enable.idempotence", "false");
properties.put("acks", "all");

try (KafkaProducer<String, String> producer =
        new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer())) {
    ProducerRecord<String, String> record = new ProducerRecord<>(
            "sales.orders", "order-1", "{\"order_id\":1,\"amount\":12.50}");
    record.headers().add("source", "app".getBytes(StandardCharsets.UTF_8));
    producer.send(record).get();
}
```

The value populates `order_id` and `amount`; the UTF-8 key bytes populate `_kafka_key`, and the
header populates `_kafka_headers`. Binary values should use `ByteArraySerializer`; string values
can use `StringSerializer`. Do not configure `transactional.id`. Basic Produce does not provide
Kafka idempotence or transactional guarantees, so retries can produce duplicate rows.

## Conversion properties

These are table properties set through CREATE or ALTER TABLE, rather than `server.yaml` settings.

| Option | Type | Default | Description |
|---|---|---|---|
| table.kafka.enabled | Boolean | false | Explicitly enable the table's Kafka mapping. |
| table.kafka.convert.value.type | Enum | `parse_json` | `binary`, `string`, or `parse_json`. |
| table.kafka.convert.value.layout | Enum | `flat` | JSON layout: `flat` maps top-level business columns; `nested` targets `_kafka_value ROW<...>`. Ignored for binary/string, whether defaulted or explicitly configured. |
| table.kafka.convert.key.type | Enum | (None) | Optional `binary` conversion; explicitly configuring it requires `_kafka_key BYTES`. |

Conversion defaults apply only at read time. CREATE and ALTER preserve omitted properties.
Declaring `_kafka_key BYTES` selects binary key mapping even when the key conversion option is
omitted. No key column or conversion property is required when the key is not needed.

Custom properties are preserved but do not configure the Kafka mapping. Storage properties and
their values are checked by the server against the options supported by that cluster version.

### Value conversion

| Conversion | Required value schema | Mapping |
|---|---|---|
| `binary` | `_kafka_value BYTES` | Store the record value as bytes. |
| `string` | `_kafka_value STRING` | Interpret the record value as a UTF-8 string. |
| `parse_json`, `flat` | Business columns at the table's top level | Map JSON object members to business columns by name. |
| `parse_json`, `nested` | `_kafka_value ROW<...>` | Map JSON object members to fields inside the value ROW. |

Binary, string, and nested JSON map their value to `_kafka_value`; other top-level columns must
be optional system columns. Flat JSON uses top-level business columns and does not allow a
`_kafka_value` column. Both JSON layouts require an object and preserve declared nested ROW,
ARRAY, and MAP structures.

System names are reserved at the physical table's top level. Inside a nested `_kafka_value` ROW,
they are ordinary business field names: `_kafka_value ROW<_kafka_key STRING>` can coexist with
a top-level `_kafka_key BYTES` column. The inner field comes from the JSON value, and the outer
column comes from the Kafka key. The same rule applies to deeper business structures.

JSON schemas accept all currently defined Fluss types, subject to the nullable-field rule above
and two additional restrictions: MAP keys
must be STRING, including nested maps, and nesting depth must not exceed 64. Each top-level
business field starts at depth zero; ROW fields, ARRAY elements, and MAP values each add one.
The JSON root and the physical `_kafka_value` ROW container add no level. Scalar value constraints
apply when decoding individual records.

## System columns

The four optional columns below may be omitted independently. Unmapped key, headers, or metadata
are not stored in row fields. `_kafka_value` is the value target required by the conversions above.

| Column | Type | Mapping |
|---|---|---|
| `_kafka_key` | `BYTES` | Kafka record key as bytes. |
| `_kafka_headers` | `MAP<STRING, BYTES>` | Kafka headers; the last value wins for duplicate header names. |
| `_kafka_metadata` | `ROW<timestamp TIMESTAMP_LTZ(3), timestamp_type INT, partition INT>` | Input record timestamp, batch timestamp type, and requested Kafka partition. |
| `_kafka_rescue` | Nullable `STRING` | Compact JSON containing unknown fields, values that cannot be converted, and flat-layout system-name conflicts. |

The metadata ROW must use the field names, order, and types shown above. Timestamp type is `-1`
when absent, `0` for CreateTime, or `1` for LogAppendTime. An absent timestamp maps to null.
The metadata ROW and its three fields must be nullable. Log offsets remain native log metadata and are not
fields of this ROW.

A declared rescue column needs no additional property. The conversion contract sends an
unconvertible field's original JSON value to rescue and writes null to that field. For example,
`{"order_id":"invalid","amount":12.50}` writes null to `order_id`, keeps `amount`, and stores
`{"order_id":"invalid"}` in `_kafka_rescue` when that column is declared. Nested errors preserve
their path and structure; valid sibling fields remain usable. If an entire field has the wrong
structure, rescue contains that field's original value and the field is null.

Without rescue, unknown fields, conversion failures, and flat-layout system-name conflicts
fail conversion. Invalid JSON and a non-object JSON root always fail; rescue requires a parsed
object and does not hide protocol or storage failures. These write-time behaviors require the
planned JSON decoder; this implementation enforces their DDL prerequisites.
Binary/string conversion and JSON records with no fields to rescue write null to this column.

## Troubleshooting

| Symptom | What to check |
|---|---|
| Producer cannot discover a topic or send records | Check the API status above first. Metadata and Produce are not yet implemented in this version. |
| Connection fails | Use a TabletServer Kafka listener, enable `kafka.enabled`, and ensure advertised addresses are reachable from the client. |
| A topic does not map to the intended table | Create and enable the Fluss table first. Check the `<database>.<table>` spelling or `kafka.default.database` for an unqualified name. |
| CREATE or ALTER rejects the Kafka mapping | Use a non-partitioned Arrow log table without a primary key, nullable fields, and the required value column for the chosen conversion. |
| JSON schema validation fails | Check nested MAP key types and the nesting limit. MAP values and ARRAY elements must be nullable. |
| A future Basic Produce client requests unsupported producer APIs | Disable idempotence and transactions. Consumer APIs require separate support. |
