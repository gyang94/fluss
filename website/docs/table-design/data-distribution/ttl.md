---
title: TTL
sidebar_position: 3
---

# TTL

Fluss provides separate controls for primary-key row expiration, remote log retention, local log retention, and rolling an
expired active segment. Keeping these controls separate lets you retain data in remote storage
while reclaiming local disk earlier.

## Row TTL for Primary Key Tables

Primary key tables can configure row-level TTL with `'table.kv.ttl' = '<duration>'`. The duration must be at least 1 millisecond. The option has no default value; if it is not configured, row-level TTL is disabled.

```sql title="Flink SQL"
CREATE TABLE pk_table
(
    id BIGINT,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket.num' = '4',
    'table.kv.ttl' = '7 d'
);
```

Row TTL is best-effort cleanup. A row becomes eligible for cleanup after the configured duration, but expired rows may still be visible until RocksDB compaction removes them. Fluss stores the TTL timestamp in the primary-key table value and uses a compaction filter to remove expired rows during RocksDB compaction.

Auto partitioning is the recommended expiration mechanism when data can be partitioned by time, because expiring whole partitions preserves changelog completeness. Row TTL is intended for cases that require per-row cleanup and can accept weaker changelog semantics.

Row TTL cleanup does not emit delete records. Downstream consumers of `$changelog` or `$binlog` will not receive delete changes when rows expire. After compaction removes an expired row, the next write for the same primary key is treated as an insert and does not emit an `UPDATE_BEFORE` for the expired value.

By default, row TTL uses processing time. To use event time, configure `table.kv.ttl.time-column` when creating the table:

```sql title="Flink SQL"
CREATE TABLE pk_table_with_event_time
(
    id BIGINT,
    event_time BIGINT,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket.num' = '4',
    'table.kv.ttl' = '7 d',
    'table.kv.ttl.time-column' = 'event_time'
);
```

The event-time column must be `BIGINT` epoch milliseconds, `TIMESTAMP`, or `TIMESTAMP_LTZ`. `TIMESTAMP` values are interpreted in the TabletServer's system time zone, so all TabletServers must use the same system time zone. Rows with null event-time values do not expire through row TTL.

Row TTL must be configured when creating the primary key table. Changing or disabling `table.kv.ttl`, or changing `table.kv.ttl.time-column`, with `ALTER TABLE ... SET` or `ALTER TABLE ... RESET` is not supported in this version.

Before creating a row-TTL primary key table, upgrade every server to Fluss 1.0+ so that all servers understand the tagged value layout used for row TTL. After a row-TTL table exists, downgrading to a version < 1.0 that does not understand that layout is not supported.

See [Flink Connector Options](/engine-flink/options.md#storage-options) for the complete row TTL option definitions.

## Table log TTL

Set `'table.log.ttl' = '<duration>'` on a table to control how long Fluss retains its log data. The
default is 7 days. Setting it to `0ms` disables TTL-based deletion.

For log tables, this option controls the retention of the table data, including data copied to
remote storage. For primary key tables, it controls changelog retention and does not expire the
primary key table data itself. To expire primary key table data, use
[auto partitioning](partitioning.md#auto-partitioning) or [row TTL](#row-ttl-for-primary-key-tables).

## Local log TTL

When [remote log storage](../../maintenance/tiered-storage/remote-storage.md) is enabled,
`table.log.local-ttl` controls how long local log segments are retained. It has no independent
default and inherits `table.log.ttl` when it is not configured. Setting it to `0ms` disables
TTL-based local cleanup. When both TTLs are positive, `table.log.local-ttl` must be less than or
equal to `table.log.ttl`.

A local segment is eligible for TTL cleanup only after it has been rolled and copied to remote
storage. Therefore, `table.log.local-ttl` can reclaim local disk without shortening the remote log
retention controlled by `table.log.ttl`. When remote log storage is disabled,
`table.log.local-ttl` does not apply and `table.log.ttl` controls local log cleanup.

## Active-segment rolling

The active segment is still receiving records and cannot be uploaded or deleted. On a low-traffic
table, it might remain active after the effective local cleanup TTL expires. The server option
`log.retention.roll-active-segment.enabled` controls whether Fluss rolls such a segment and is
disabled by default. The effective local cleanup TTL is `table.log.local-ttl` when remote log
storage is enabled, falling back to `table.log.ttl` when the local option is not configured. When
remote log storage is disabled, it is always `table.log.ttl`.

When the option is enabled, Fluss can roll a non-empty active segment after the effective local
cleanup TTL expires, provided that the high watermark has reached the log end offset. The resulting
inactive segment can then be uploaded to remote storage and subsequently removed from local
storage.

The complete lifecycle is:

1. Records are appended to the active local segment.
2. After the effective local cleanup TTL expires and all records are committed, active-segment
   rolling makes the segment inactive.
3. The inactive segment is copied to remote storage.
4. The copied local segment becomes eligible for TTL cleanup. It may also be removed earlier by the
   `table.log.tiered.local-segments` count-based policy.
5. The remote copy remains available until it expires according to `table.log.ttl`.

See [server configuration](../../maintenance/configuration.md#log) for the server option and
[updating configs](../../maintenance/operations/updating-configs.md#updating-cluster-configs) for
changing it dynamically. Clusters upgrading from 0.9 should also follow the
[1.0 upgrade notes](../../maintenance/operations/upgrade-notes-1.0.md#active-segment-retention-rollout).
