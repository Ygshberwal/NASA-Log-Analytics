CREATE KEYSPACE IF NOT EXISTS log_analysis
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- A small, single-topic demo: Kafka partition and offset identify each record.
CREATE TABLE IF NOT EXISTS log_analysis.events_by_partition (
    partition int,
    offset bigint,
    host text,
    event_time timestamp,
    ingested_at timestamp,
    method text,
    path text,
    status int,
    response_bytes bigint,
    raw_log text,
    PRIMARY KEY (partition, offset)
);
