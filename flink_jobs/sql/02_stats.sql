-- Per-wiki activity, in one-minute event-time windows.
--
-- Event time, not arrival time: if ingestion stalls and catches up, the edits
-- must still be counted in the minute they happened. The watermark decides
-- when a window is complete; anything later than the allowed lateness is late
-- data, not a silent miscount.

CREATE TABLE clean_edits (
    wiki         STRING,
    server_name  STRING,
    `domain`     STRING,
    `type`       STRING,
    `namespace`  INT,
    title        STRING,
    `user`       STRING,
    is_bot       BOOLEAN,
    is_minor     BOOLEAN,
    byte_delta   INT,
    new_length   INT,
    `comment`    STRING,
    revision_id  BIGINT,
    event_time   TIMESTAMP_LTZ(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '${max_out_of_orderness}' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '${topic_clean}',
    'properties.bootstrap.servers' = '${bootstrap}',
    'properties.group.id' = 'wes-stats',
    -- Only committed records: the upstream job writes transactionally.
    'properties.isolation.level' = 'read_committed',
    -- The upstream job writes `wiki` into the record KEY, not the value, so
    -- the key has to be read back explicitly. Without this, wiki is silently
    -- NULL for every row and every aggregation collapses into one group.
    'key.format' = 'raw',
    'key.fields' = 'wiki',
    'value.fields-include' = 'EXCEPT_KEY',
    'scan.startup.mode' = 'earliest-offset',
    ${scan_bounded}
    'format' = 'json'
);

CREATE TABLE wiki_stats_1m (
    window_start   TIMESTAMP(3),
    window_end     TIMESTAMP(3),
    wiki           STRING,
    edits          BIGINT,
    bot_edits      BIGINT,
    human_edits    BIGINT,
    editors        BIGINT,
    pages_touched  BIGINT,
    new_pages      BIGINT,
    bytes_added    BIGINT,
    bytes_removed  BIGINT,
    net_bytes      BIGINT,
    bot_share_pct  DOUBLE,
    -- A window is written once and then corrected if late data arrives, so
    -- the sink is an upsert keyed by the window and wiki.
    PRIMARY KEY (window_start, wiki) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = '${topic_stats}',
    'properties.bootstrap.servers' = '${bootstrap}',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO wiki_stats_1m
SELECT
    window_start,
    window_end,
    wiki,
    COUNT(*)                                             AS edits,
    COUNT(*) FILTER (WHERE is_bot)                       AS bot_edits,
    COUNT(*) FILTER (WHERE NOT is_bot)                   AS human_edits,
    COUNT(DISTINCT `user`)                               AS editors,
    COUNT(DISTINCT title)                                AS pages_touched,
    COUNT(*) FILTER (WHERE `type` = 'new')               AS new_pages,
    COALESCE(SUM(byte_delta) FILTER (WHERE byte_delta > 0), 0) AS bytes_added,
    COALESCE(SUM(byte_delta) FILTER (WHERE byte_delta < 0), 0) AS bytes_removed,
    COALESCE(SUM(byte_delta), 0)                         AS net_bytes,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_bot) / COUNT(*), 2) AS bot_share_pct
FROM TABLE(
    TUMBLE(TABLE clean_edits, DESCRIPTOR(event_time), INTERVAL '${window_minutes}' MINUTE)
)
GROUP BY window_start, window_end, wiki;
