# Wikipedia Edit Stream

A real-time pipeline over Wikimedia's live edit firehose: **Kafka** for
transport, **Flink SQL** for stateful stream processing, Python only for
ingestion. Every edit made to every Wikipedia, as it happens.

```
stream.wikimedia.org  (SSE, ~50 edits/sec, no auth)
        │
        ▼  ingest service: resumable, idempotent producer
  wiki.edits.raw ─────────────────────────────────────────┐
        │                                                 │
        ▼  01_clean.sql   parse · event time · validate   │
  wiki.edits.clean ──┬──────────────────────┐        wiki.edits.dlq
        │            │                      │
        ▼            ▼                      ▼
  02_stats.sql   03_edit_wars.sql     (rejects, with reason)
  tumbling 1-min  MATCH_RECOGNIZE
  windows         A→B→A pattern
        │            │
        ▼            ▼
  wiki.stats      wiki.alerts
  .per_wiki_1m    .edit_wars
```

## Quick start

Needs **JDK 17 or 21** and Kafka (`brew install kafka`). No Docker required.

```bash
make install     # venv + connector jars
make up          # local KRaft broker + topics
make demo        # live ingest + all three jobs, 150s
make offsets     # what landed
```

Output from a real run:

```
wiki.edits.raw               116865
wiki.edits.clean             118124
wiki.edits.dlq                    0
wiki.stats.per_wiki_1m         2515
wiki.alerts.edit_wars           599
```

```
specieswiki    15:27  edits=4    editors=1   bots=0.0%    net_bytes=222
enwiki         15:27  edits=46   editors=17  bots=13.04%  net_bytes=-4644
zhwikisource   15:27  edits=120  editors=1   bots=100.0%  net_bytes=32242
```

## What it demonstrates

| Area | Implementation |
|---|---|
| **Event time** | Watermarks with bounded out-of-orderness; windows keyed on when an edit *happened*, not when it arrived |
| **Stateful pattern matching** | `MATCH_RECOGNIZE` detecting revert wars — a sequence, not an aggregate |
| **Exactly-once** | Idempotent producer, Flink checkpointing, transactional Kafka sink, `read_committed` consumers |
| **Resumable ingestion** | SSE `Last-Event-ID` persisted, so a reconnect resumes instead of skipping |
| **Backpressure** | A full producer queue blocks the reader rather than dropping edits |
| **Dead-letter queue** | Invalid records are routed with a reason, never silently dropped |
| **Bounded replay** | The same SQL runs over a finite source, so a demo is reproducible and CI can assert on it |
| **SQL-first** | Every transformation is a `.sql` file that also runs in `sql-client` or on a cluster |

## The interesting logic

### Detecting an edit war

"Many edits" is not a war — a popular page gets those all day. The signature
is a *sequence*: one editor changes a page, someone else changes it, and the
first editor changes it straight back. That is an ordering question, which is
what `MATCH_RECOGNIZE` is for and a `GROUP BY` cannot express:

```sql
PATTERN (FIRST_EDIT OTHER REVERT) WITHIN INTERVAL '10' MINUTE
DEFINE
    FIRST_EDIT AS NOT FIRST_EDIT.is_bot,
    OTHER      AS OTHER.`user` <> FIRST_EDIT.`user` AND NOT OTHER.is_bot,
    REVERT     AS REVERT.`user` = FIRST_EDIT.`user`
```

Real detections from the run above:

```
commonswiki  Category:CC-Zero                 Zarateman vs Bumpf         76s
commonswiki  Category:Self-published work...  Chabe01   vs Tilman2007     1s
```

### Resuming without losing edits

The stream is unbounded and disconnects are routine — one happened during the
first test run. The last SSE event id is persisted and replayed as
`Last-Event-ID`, so the server resumes where ingestion stopped rather than at
"now". The reconnect in that run recovered a 35-minute backlog at **2,187
events/sec**, with zero failed deliveries.

### Why a window looks "broken" when it is correct

On an unbounded source a window only closes once the watermark passes its end,
and the watermark only advances when *newer* events arrive. Run an aggregation
against a static topic and it emits nothing — that is streaming semantics, not
a bug. `WES_BOUNDED=1` (`make replay`) turns the Kafka source finite so it
emits a final watermark and every window closes, which is what makes the demo
reproducible.

## Bugs worth knowing about

Three things cost real debugging time here and are not obvious from the docs:

**The connector jar does not include compression codecs.** A topic written
with `compression.type=zstd` fails at read time with
`NoClassDefFoundError: com/github/luben/zstd/ZstdOutputStreamNoFinalizer`.
`flink-sql-connector-kafka` shades `kafka-clients` but not the codecs, so
`zstd-jni` has to be added to `lib/` — `scripts/fetch-connectors.sh` does it.

**A key field is not in the value.** The clean sink writes `wiki` into the
Kafka record *key* (`value.fields-include = 'EXCEPT_KEY'`). A downstream table
that declares `wiki` but does not declare `key.format` gets `NULL` for every
row — silently. It cost 118,024 rows of empty aggregates before
`COUNT(wiki) = 0` made it obvious. Both downstream jobs now read the key
explicitly.

**`INTERVAL '1 MINUTE'` is not valid SQL.** It has to be `INTERVAL '1' MINUTE`.
The config now holds `window_minutes: 1` and the unit is fixed in the SQL, so
a config value cannot produce invalid syntax. There is a test for it.

## Layout

```
ingest/          SSE client, resumable offsets, Kafka producer
flink_jobs/
  sql/           the three jobs, as plain SQL
  runner.py      placeholder rendering, statement splitting, job config
conf/
  pipeline.yaml  topics, broker, watermark and window settings
  kafka/         project-local KRaft broker config
scripts/         kafka control, topic creation, connector fetch, demo
tests/           23 tests, no cluster required
```

## Why apache-beam is not installed

PyFlink pins `apache-beam<=2.61`, whose wheels are no longer published for
current Python on arm64 — installing `apache-flink` normally fails outright.
Beam is only used to run **Python UDFs** in a separate process. Every job here
is pure SQL, so the JVM does all the work and beam is never imported.
`scripts/install.sh` installs Flink with `--no-deps` and lists its real runtime
dependencies explicitly.

This is not a workaround so much as a consequence of the design: keeping Python
out of the data path is what makes the jobs portable to a real cluster
unchanged.

## Running on a cluster

Nothing in the jobs assumes a local runtime. The `.sql` files run as-is in
`sql-client` against a session cluster; `conf/pipeline.yaml` supplies the
broker and topics, and `WES_BOOTSTRAP_SERVERS` overrides the broker per
environment.
