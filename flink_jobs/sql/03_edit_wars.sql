-- Detect edit wars with pattern matching over the stream.
--
-- The signature of a revert war is not "many edits" - a popular page gets
-- those all day. It is a specific *sequence*: one editor changes a page,
-- a different editor changes it, and the first editor changes it straight
-- back. That is an ordering question, which is what MATCH_RECOGNIZE is for
-- and what a GROUP BY cannot express.
--
-- A->B->A within ten minutes, on the same page, is the shortest sequence that
-- cannot be explained by two people collaborating.

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
    'properties.group.id' = 'wes-editwars',
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

CREATE TABLE edit_wars (
    wiki            STRING,
    title           STRING,
    reverter        STRING,
    opponent        STRING,
    started_at      TIMESTAMP_LTZ(3),
    ended_at        TIMESTAMP_LTZ(3),
    seconds_elapsed BIGINT,
    swing_bytes     INT
) WITH (
    'connector' = 'kafka',
    'topic' = '${topic_alerts}',
    'properties.bootstrap.servers' = '${bootstrap}',
    'format' = 'json',
    'sink.delivery-guarantee' = 'at-least-once'
);

INSERT INTO edit_wars
SELECT
    wiki,
    title,
    reverter,
    opponent,
    started_at,
    ended_at,
    TIMESTAMPDIFF(SECOND, started_at, ended_at) AS seconds_elapsed,
    swing_bytes
FROM clean_edits
MATCH_RECOGNIZE (
    PARTITION BY wiki, title
    ORDER BY event_time
    MEASURES
        FIRST_EDIT.`user`       AS reverter,
        OTHER.`user`            AS opponent,
        FIRST_EDIT.event_time   AS started_at,
        REVERT.event_time       AS ended_at,
        -- How much of the other editor's change was undone.
        OTHER.byte_delta + REVERT.byte_delta AS swing_bytes
    ONE ROW PER MATCH
    -- Do not let one war generate a match per overlapping triple.
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (FIRST_EDIT OTHER REVERT) WITHIN INTERVAL '10' MINUTE
    DEFINE
        -- Bots reverting vandalism are doing their job, not fighting.
        FIRST_EDIT AS NOT FIRST_EDIT.is_bot,
        OTHER      AS OTHER.`user` <> FIRST_EDIT.`user` AND NOT OTHER.is_bot,
        REVERT     AS REVERT.`user` = FIRST_EDIT.`user`
);
