-- Raw edits -> conformed stream + dead-letter queue.
--
-- The source topic holds the payload exactly as Wikimedia sent it. This job
-- does the three things that must happen before anything else reads it:
-- assigns event time, validates, and splits good from bad. Nothing is
-- dropped - a record that fails validation goes to the DLQ carrying the
-- reason, so a change upstream is visible rather than silent.

CREATE TABLE raw_edits (
    meta          ROW<`domain` STRING, dt STRING, `id` STRING, `stream` STRING, uri STRING>,
    `id`          BIGINT,
    `type`        STRING,
    `namespace`   INT,
    title         STRING,
    `comment`     STRING,
    `timestamp`   BIGINT,
    `user`        STRING,
    bot           BOOLEAN,
    minor         BOOLEAN,
    patrolled     BOOLEAN,
    `length`      ROW<`old` INT, `new` INT>,
    revision      ROW<`old` BIGINT, `new` BIGINT>,
    server_name   STRING,
    wiki          STRING,

    -- Event time comes from the edit itself, never from arrival. A backlog
    -- replayed after an outage must land in the windows it belongs to.
    event_time AS TO_TIMESTAMP_LTZ(`timestamp`, 0),
    -- Bounded out-of-orderness: the stream is fanned in from several
    -- datacentres, so records arrive slightly out of order.
    WATERMARK FOR event_time AS event_time - INTERVAL '${max_out_of_orderness}' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '${topic_raw}',
    'properties.bootstrap.servers' = '${bootstrap}',
    'properties.group.id' = 'wes-clean',
    'scan.startup.mode' = 'earliest-offset',
    ${scan_bounded}
    'format' = 'json',
    -- Survive a single malformed payload instead of failing the job; such
    -- records surface as nulls and are caught by the validation below.
    'json.ignore-parse-errors' = 'true'
);

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
    event_time   TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'kafka',
    'topic' = '${topic_clean}',
    'properties.bootstrap.servers' = '${bootstrap}',
    -- Keyed by wiki so every edit for a wiki keeps its order on one partition.
    'key.format' = 'raw',
    'key.fields' = 'wiki',
    'value.format' = 'json',
    'value.fields-include' = 'EXCEPT_KEY',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = 'wes-clean-',
    'properties.transaction.timeout.ms' = '900000'
);

CREATE TABLE dlq_edits (
    reject_reason STRING,
    wiki          STRING,
    title         STRING,
    `user`        STRING,
    `type`        STRING,
    raw_timestamp BIGINT,
    rejected_at   TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'kafka',
    'topic' = '${topic_dlq}',
    'properties.bootstrap.servers' = '${bootstrap}',
    'format' = 'json',
    'sink.delivery-guarantee' = 'at-least-once'
);

-- One source read feeding both sinks. Two separate jobs would consume the
-- topic twice and could disagree about which records were rejected.
EXECUTE STATEMENT SET
BEGIN

INSERT INTO clean_edits
SELECT
    wiki,
    server_name,
    meta.`domain`,
    `type`,
    `namespace`,
    title,
    `user`,
    COALESCE(bot, FALSE)   AS is_bot,
    COALESCE(minor, FALSE) AS is_minor,
    -- A new page has no previous length; treat its whole size as the delta.
    COALESCE(`length`.`new`, 0) - COALESCE(`length`.`old`, 0) AS byte_delta,
    `length`.`new` AS new_length,
    `comment`,
    revision.`new` AS revision_id,
    event_time
FROM raw_edits
WHERE wiki IS NOT NULL
  AND title IS NOT NULL
  AND `user` IS NOT NULL
  AND `timestamp` IS NOT NULL
  AND `type` IN ('edit', 'new', 'categorize', 'log');

INSERT INTO dlq_edits
SELECT
    CASE
        WHEN wiki IS NULL        THEN 'missing_wiki'
        WHEN title IS NULL       THEN 'missing_title'
        WHEN `user` IS NULL      THEN 'missing_user'
        WHEN `timestamp` IS NULL THEN 'missing_timestamp'
        ELSE 'unknown_type'
    END AS reject_reason,
    wiki,
    title,
    `user`,
    `type`,
    `timestamp`,
    CURRENT_TIMESTAMP
FROM raw_edits
WHERE wiki IS NULL
   OR title IS NULL
   OR `user` IS NULL
   OR `timestamp` IS NULL
   OR `type` NOT IN ('edit', 'new', 'categorize', 'log');

END;
