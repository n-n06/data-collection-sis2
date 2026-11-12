# Data Collection and Preparation SIS2: Pink Floyd Physical Releases - Kafka and Pandera Pipeline
> By Nursultan Zhantuar
**Course:** Data Collection & Preparation  
**Topic:** Physical Pink Floyd release editions from the public MusicBrainz API  


> Before GitHub submission, replace the bracketed group placeholder here and on the report title/contributions pages. The instructor-notification template is in `docs/instructor_notification.md` (required deadline: 01.11.2026).

## 1. Project overview

This repository implements one connected, finite pipeline:

`MusicBrainz API -> raw JSONL -> Kafka producer -> real Apache Kafka broker/topic -> Kafka consumer -> Pandera -> pandas/NumPy -> SQLite3 -> EDA figures`

It collects dated **physical** Pink Floyd releases (CD, vinyl, cassette, MiniDisc, 8-track, and DVD/Blu-ray), preserves raw records, sends each record as JSON through Kafka, detects three deliberately invalid test messages, quarantines invalid rows, cleans and transforms valid rows, loads them idempotently into SQLite, runs SQL checks, and generates four Matplotlib/Seaborn figures.

The repository contains a 250-record deterministic MusicBrainz metadata snapshot so the reported results can be reproduced even if the live catalog changes. Use `--source api` for the required live collection; use `--source snapshot` only for a stable rerun or offline rehearsal.


## 2. Repo structure

```bash
pink-floyd-kafka-pandera/
|-- README.md
|-- docker-compose.yml
|-- requirements.txt
|-- main.py
|-- src/
|-- data/
|-- figures/
|-- docs/
|-- sql/verification_queries.sql
|-- tests/
`-- report/project_report.pdf
```

## 3. Software requirements

- Git
- Python 3.11 or 3.12
- Docker Desktop, Docker Engine, or another installation with `docker compose`
- At least 2 GB free RAM for the Kafka container
- Internet access to `https://musicbrainz.org` for `--source api`
- No API key is required

The dependencies are in `requirements.txt`. Due to some problems I had with req versions, they are not pinned, and this is a known limitation

## 4. Clone and install dependencies

Clone the repo and crate a virtual python environment
```bash
git clone <YOUR-GITHUB-REPOSITORY-URL>
cd pink-floyd-kafka-pandera
python -m venv .venv
```

Activate the virtual environment:

```bash
# Linux/macOS
source .venv/bin/activate

# Windows PowerShell
.venv\Scripts\Activate.ps1
```

Install dependencies:

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```


## 5. Start the real Kafka broker

```bash
docker compose up -d
docker compose ps
```

Wait until `pink-floyd-kafka` is healthy. Create/inspect the topic explicitly:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic pink-floyd-physical-releases \
  --partitions 1 \
  --replication-factor 1

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic pink-floyd-physical-releases
```

This is an actual Apache Kafka 3.9.1 broker

## 6. Running the Pipeline

### Full Run

```bash
python main.py full --source api --target 250 --inject-invalid 3
```

The command collects exactly 250 dated physical releases, starts the finite consumer first, publishes 253 messages (250 source + 3 test-invalid), waits for exactly 253 matching messages, validates, cleans, loads twice (to verifyt idempotency), verifies, and runs EDA.

### Deterministic snapshot rerun

```bash
python main.py full --source snapshot --target 250 --inject-invalid 3
```

This follows the same real Kafka path but uses the bundled preserved snapshot.

## 7. Exact separate producer and consumer commands

These commands demonstrate that `producer.py` and `consumer.py` are independent scripts. Use two terminals after Kafka is healthy.

First collect 250 records:

```bash
python -m src.api_client --source api --target 250
```

Set the **same** run ID in both terminals:

```bash
# Linux
export RUN_ID="pink-floyd-runid-001"

# Windows
$env:RUN_ID="pink-floyd-runid-001"
```

**Terminal A - start consumer first:**

```bash
python -m src.consumer \
  --run-id "$RUN_ID" \
  --expected 253 \
  --topic pink-floyd-physical-releases \
  --bootstrap-servers localhost:9092
```

Windows PowerShell uses `$env:RUN_ID` instead of `$RUN_ID`.

**Terminal B - run producer:**

```bash
python -m src.producer \
  --run-id "$RUN_ID" \
  --inject-invalid 3 \
  --topic pink-floyd-physical-releases \
  --bootstrap-servers localhost:9092
```

After Terminal A terminates at 253 messages, run the remaining connected stages:

```bash
python main.py process
```

The consumer has two termination safeguards: `consumed == expected`, plus a 45-second idle timeout that raises an error rather than hanging.

## 8. API endpoint and parameters

Endpoint:

```text
GET https://musicbrainz.org/ws/2/release
```

Parameters used by `src/api_client.py`:

```text
artist=83d91898-7763-47d7-b03b-b92132375c47
inc=media+release-groups+labels+artist-credits
fmt=json
limit=100
offset=<0,100,...>
```

The client retries temporary HTTP failures, checks unsuccessful responses, uses a 45-second timeout, waits about 1.1 seconds between requests, filters out `Digital Media`/streaming, rejects missing dates, deduplicates release IDs, and stops only after the requested target is reached. Every original API page is preserved in `data/raw_api_pages/`; normalized but uncleaned records are preserved in both `data/raw_data.jsonl` and `data/raw_data.csv`.

## 9. Validation, cleaning, and transformations

`src/validator.py` defines 14 Pandera rules, including:

- UUID format and uniqueness of `release_id`
- exact artist name
- required non-empty title
- allowed release statuses
- allowed physical format categories
- media and track-count ranges
- country-code format
- date pattern and date precision
- release year range
- data type coercion

Three explicit invalid messages test invalid UUID/empty title, zero tracks/future date, and disallowed status/digital format. Invalid records and all failure cases are written to `data/invalid_records.csv` and `data/validation_errors.csv`; they never enter the processed table.

`src/cleaner.py` handles optional missing values, normalizes whitespace/categories, converts data types, removes duplicates, uses NumPy for age/decade/era/z-score calculations, applies a multi-condition recent multi-disc filter, performs GroupBy aggregation, and creates a decade-by-format pivot table.

## 10. Database generation and SQL verification

The loader creates `data/output.db` and table `releases`. `release_id` is the primary key, and insertion uses `ON CONFLICT(release_id) DO NOTHING`.

`python main.py process` intentionally loads the same processed frame twice. For the bundled reference data the first load inserts 250 rows and the second inserts 0, while the table remains at 250 rows.

Run all submitted SQL checks with the SQLite CLI:

```bash
sqlite3 -header -column data/output.db < sql/verification_queries.sql
```

If the CLI is unavailable:

```bash
python - <<'PY'
import sqlite3
con = sqlite3.connect('data/output.db')
print(con.execute('SELECT COUNT(*), COUNT(DISTINCT release_id) FROM releases').fetchone())
print(con.execute('SELECT release_id, COUNT(*) FROM releases GROUP BY release_id HAVING COUNT(*) > 1').fetchall())
con.close()
PY
```

Expected: `(250, 250)` and an empty duplicate list for the snapshot run.

## 11. Analysis results and figures

Run EDA independently:

```bash
python -m src.analysis --input data/processed_data.csv
```

Generated files:

- `figures/01_format_distribution.png`
- `figures/02_releases_by_decade.png`
- `figures/03_top_countries.png`
- `figures/04_decade_format_heatmap.png`
- `data/eda_statistics.json`
- `data/eda_findings.md`

The packaged 250-row run spans 1973-2025. CD is the most frequent format (138), the 1990s are the peak decade (87), and 36 country/area codes are represented.

## 12. End-to-end success verification

```bash
cat data/pipeline_summary.json
```

Expected snapshot-run counts:

| Stage | Count |
|---|---:|
| Source records collected | 250 |
| Kafka messages published | 253 |
| Kafka messages consumed | 253 |
| Valid records | 250 |
| Invalid test records | 3 |
| Records after cleaning | 250 |
| SQLite rows | 250 |
| Rows inserted by second load | 0 |
| Figures generated | 4 |

The three-message difference between collection and Kafka counts is intentional validation testing. All checks in `pipeline_summary.json` must be `true`.

## 13. Tests

```bash
pytest
```

The tests cover format classification, deliberate invalid records, cleaning outputs, schema structure, and SQLite idempotency.

## 14. Stop and reset Kafka

```bash
docker compose down
```

To delete the broker volume/state and rebuild from a clean Kafka instance:

```bash
docker compose down -v
docker compose up -d
```

## 15. Troubleshooting

- **NoBrokersAvailable:** wait for `docker compose ps` to show healthy; verify port 9092 is free.
- **Consumer timeout:** start the consumer before the producer and use identical run IDs and expected count.
- **MusicBrainz 503/429:** wait and retry; the client already performs exponential retries and rate limiting.
- **Fewer than 250 dated physical records:** rerun later or lower `--target`, but never below 200 for submission.
- **Stale topic data:** messages are scoped by `run_id`; the consumer ignores other runs.
- **Port conflict:** set `KAFKA_BOOTSTRAP_SERVERS` consistently and update the Compose port mapping if needed.
- **Windows shell syntax:** use the PowerShell environment-variable forms shown above.

## 16. Submission checklist

- Replace group placeholders in README, report, and instructor notification.
- Run `python main.py full --source api --target 250 --inject-invalid 3` with Docker Kafka.
- Confirm every check in `data/pipeline_summary.json` is true.
- Run SQL verification and `pytest`.
- Commit source, generated figures, SQLite database, data dictionary, and `report/project_report.pdf`.
- Push the same tested version to GitHub and rehearse the 10-minute defense.
