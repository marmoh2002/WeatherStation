# WeatherStation Monitoring Distributed System

[![Java](https://img.shields.io/badge/Java-21-orange.svg?style=for-the-badge&logo=openjdk)](https://openjdk.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-3.7.0-black.svg?style=for-the-badge&logo=apachekafka)](https://kafka.apache.org/)
[![Kafka Streams](https://img.shields.io/badge/Kafka_Streams-Real--Time-blue.svg?style=for-the-badge&logo=apachekafka)](https://kafka.apache.org/documentation/streams/)
[![Avro](https://img.shields.io/badge/Apache_Avro-1.11.3-E10098.svg?style=for-the-badge&logo=apache)](https://avro.apache.org/)
[![Elasticsearch](https://img.shields.io/badge/Elasticsearch-8.18.0-005571.svg?style=for-the-badge&logo=elasticsearch)](https://www.elastic.co/elasticsearch/)
[![Kibana](https://img.shields.io/badge/Kibana-8.18.0-005571.svg?style=for-the-badge&logo=kibana)](https://www.elastic.co/kibana)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg?style=for-the-badge&logo=docker)](https://www.docker.com/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Minikube-326CE5.svg?style=for-the-badge&logo=kubernetes)](https://kubernetes.io/)

**Simulated weather stations stream Avro telemetry through Apache Kafka into two paths at once: a custom CRC32 BitCask engine for O(1) live state lookups and hourly SNAPPY Parquet archives indexed into Elasticsearch and Kibana.**

Built as a five-module Java 21 Maven platform, the system pairs real-time stream alerting with fault-tolerant storage mechanics: dead-letter routing for malformed payloads, queue-driven consumer backpressure, sequence-gap reconstruction, background segment compaction, and SHA-256 deduplicated search indexing. Runs with one command on Docker Compose or Minikube.

---

## System architecture

![WeatherStation Monitoring Distributed System — System Architecture](./docs/system_architecture_ref.svg)

### What happens on every reading

1. **Telemetry generation (`my_producer`)**: On startup, `StationIdAllocator` allocates a strictly monotonic `station_id` (`1, 2, 3, ...`) shared across containers and restarts by appending a registration marker to partition `0` of the single-partition `weather-station-ids` Kafka topic (`offset + 1`), or honors a `STATION_ID` env override. Each producer then emits one Avro `StatusMessage` per second to `weather-stations` (with a 10% simulated packet drop), registering schemas with Confluent Schema Registry (`:8081`).
2. **Real-time rain alerting (`my_stream`)**: `WarningApp` consumes `weather-stations` with `SpecificAvroSerde`, filters events where `weather != null` and `humidity > 70%`, serializes structured JSON alerts, and publishes them to `rain-alerts`.
3. **Resilient central ingestion (`base_central_station`)**:
   - **Dead-letter isolation**: `SafeStatusMessageDeserializer` catches deserialization errors and attaches `dlq-error` and `dlq-raw-payload` headers so `ConsumerLoop` routes bad payloads to `weather-dead-letter` instead of crashing polling.
   - **Adaptive backpressure**: When `Pipeline`'s bounded `ArrayBlockingQueue` (`10,000` capacity) hits 80% full, `ConsumerLoop` pauses its assigned Kafka partitions until the queue drains to 50%.
   - **Gap reconstruction**: `Pipeline` tracks `lastSeenSNo` per station and synthesizes `"DROPPED"` markers (`weather = null`) into Parquet history whenever packet sequence gaps appear.
4. **Dual-path storage**:
   - **Hot key-value path (`BitCaskStore` + `BitCaskServer`)**: Appends CRC32-checksummed `DataEntry` records to `.data` segments, keeps an in-memory `ConcurrentHashMap` index for O(1) point reads on port `8080`, writes `.hint` files on roll/shutdown (with CRC32 tail-truncation recovery if a hint file isn't on disk), and compacts closed segments every 3 minutes via `Compactor`.
   - **Cold archive path (`ParquetArchiver`)**: Buffers records per UTC hour (`year=YYYY/month=MM/day=DD/hour=HH`), flushing SNAPPY Parquet files at `1000` records or `30s` using atomic `.tmp -> .parquet` moves.
5. **Search & dashboards (`parquet_es_indexer` -> Elasticsearch -> Kibana)**:
   - `IndexerMain` polls `/app/parquet_output` (`:ro`) every `30s`, tracks processed files by path, mtime, and SHA-256 in `/app/state/indexed-files.txt` so files aren't indexed twice, maps records (`docId = <station_id>_<s_no>`, ISO-8601 `@timestamp`), and bulk-indexes `500` docs per batch into `weather-status` (`:9200`), backing Kibana (`:5601`).

### Monotonic station ID allocation (`my_producer`)

![Monotonic Station ID Allocation](./docs/monotonic_station_id.svg)

Multiple producer containers or Kubernetes replicas (`replicas: 10`) can start concurrently or restart at any time. Rather than picking random station IDs (which risk collisions in `Pipeline.lastSeenSNo` and `BitCaskStore`) or adding a separate coordination store, [`StationIdAllocator`](./my_producer/src/main/java/com/weather/my_prod/StationIdAllocator.java) uses a single-partition Kafka topic as a durable atomic counter:

1. **Optional static pin (`STATION_ID`)**: If `STATION_ID` is set to a non-negative integer, [`AppConfigs.parseStationIdOverride()`](./shared_model/src/main/java/com/weather/config/AppConfigs.java) returns it immediately and skips Kafka coordination.
2. **Single-partition topic setup**: Otherwise, `AdminClient` creates (or verifies) the `weather-station-ids` topic (`STATION_ID_TOPIC`) with `1` partition and infinite retention (`retention.ms=-1`, `retention.bytes=-1`), ignoring `TopicExistsException` when replicas boot simultaneously.
3. **Serialized offset assignment**: A short-lived idempotent `KafkaProducer<byte[], byte[]>` (`acks=all`, `enable.idempotence=true`, bypassing Schema Registry) appends one registration marker to partition `0` and awaits `RecordMetadata`. Because the broker serializes all writes to partition `0`, every container receives a unique, strictly increasing offset (`0, 1, 2, ...`) persisted across restarts, mapped to **`station_id = offset + 1`** (`1, 2, 3, ...`). Transient startup errors retry up to `5` times (`1s` backoff) before failing fast.
4. **Telemetry emission**: [`ProducerLoop`](./my_producer/src/main/java/com/weather/my_prod/ProducerLoop.java) retains that assigned `station_id` as both the Kafka record key and `StatusMessage.station_id` when emitting readings to `weather-stations` every second.

> **Scope notes**: `rain-alerts` and `weather-dead-letter` are produced by services in this repository, though downstream consumers aren't implemented here.

---

## Apache Kafka flow

![Complete Apache Kafka Flow in WeatherStation](./docs/kafka_complete_flow.svg)

If you are new to Apache Kafka, think of the **Kafka Broker** (`:9092`, running in KRaft mode without ZooKeeper) as a shared, durable log of **topics** (named event channels). **Producers** append records (`key` + `value`) to topics, **Consumers** read them at their own pace without deleting them for others, and **Schema Registry** (`:8081`) acts as a central dictionary that checks and versions the Avro binary message format (`StatusMessage.avsc`) so producers and consumers stay compatible.

### Kafka 101 in this project

#### The 4 Kafka topics

| Topic | Key Format | Value Format | Why It Exists |
| :--- | :--- | :--- | :--- |
| **`weather-station-ids`** (`STATION_ID_TOPIC`) | `byte[]` (random UUID) | `byte[]` (UTC timestamp) | Single-partition (`1` partition, infinite retention `-1`) counter so every booting producer gets a unique, strictly monotonic `station_id = offset + 1`. |
| **`weather-stations`** (`KAFKA_PRODUCER_TOPIC` / `KAFKA_CONSUMER_TOPIC`) | `Long` (`station_id`) | Avro [`StatusMessage`](./shared_model/src/main/resources/avro/StatusMessage.avsc) | Primary telemetry stream carrying one weather reading per second per station. Read independently by both `my_stream` and `base_central_station`. |
| **`rain-alerts`** (`KAFKA_WARNING_OUTPUT_TOPIC`) | `Long` (`station_id`) | `String` (JSON) | High-humidity warnings (`RAIN_TRIGGER` when `humidity > 70%`) emitted by Kafka Streams. *No downstream consumer implemented in this repo.* |
| **`weather-dead-letter`** (`KAFKA_DEAD_LETTER_TOPIC`) | `Long` (`station_id`) | `byte[]` (raw payload) | Dead-letter queue (DLQ) that quarantines corrupted or incompatible payloads alongside a `dlq-error` header instead of crashing the main consumer. *No downstream consumer implemented in this repo.* |

#### Every producer, stream processor, and consumer

| Role | Class / Component | Serializers / Deserializers | Plain-English Job |
| :--- | :--- | :--- | :--- |
| **Producer #1 (ID Allocator)** | [`StationIdAllocator`](./my_producer/src/main/java/com/weather/my_prod/StationIdAllocator.java) | `ByteArraySerializer` (key & value, bypasses Schema Registry) | Runs once on startup (unless `STATION_ID >= 0` is pinned). Uses `AdminClient` to ensure `weather-station-ids` exists, writes one idempotent (`acks=all`) marker to partition `0`, and uses `offset + 1` as its `station_id`. |
| **Producer #2 (Telemetry)** | [`Producer`](./my_producer/src/main/java/com/weather/my_prod/Producer.java) & [`ProducerLoop`](./my_producer/src/main/java/com/weather/my_prod/ProducerLoop.java) | `LongSerializer` + `KafkaAvroSerializer` (uses Schema Registry `:8081`) | Runs every `1s` (`10%` simulated packet drop) and publishes `StatusMessage` readings to `weather-stations`, setting `station_id` as both the Kafka record key and payload field. |
| **Kafka Streams App** | [`WarningApp`](./my_stream/src/main/java/com/weather/my_stream/WarningApp.java) (`application.id = weather-streams`) | In: `Serdes.Long()` + `SpecificAvroSerde` (`:8081`)<br>Out: `Serdes.Long()` + `Serdes.String()` | Continuously reads `weather-stations`, filters for `weather != null && humidity > 70%`, converts matching events to JSON, and writes them to `rain-alerts`. |
| **Main Consumer** | [`BaseStationConsumer`](./base_central_station/src/main/java/com/weather/consumer/BaseStationConsumer.java) & [`ConsumerLoop`](./base_central_station/src/main/java/com/weather/consumer/ConsumerLoop.java) (`group.id = base-station-group`) | `LongDeserializer` + [`SafeStatusMessageDeserializer`](./base_central_station/src/main/java/com/weather/consumer/SafeStatusMessageDeserializer.java) (`:8081`) | Polls `weather-stations` every `1000ms`, pauses/resumes partitions when the `Pipeline` queue (`10,000`) fills to 80% / drains to 50%, and forwards valid messages to BitCask + Parquet. |
| **Producer #3 (DLQ)** | `BaseStationConsumer.createDeadLetterProducer()` | `LongSerializer` + `ByteArraySerializer` (bypasses Schema Registry) | Triggered inside `ConsumerLoop` whenever `SafeStatusMessageDeserializer` catches bad bytes and attaches `dlq-error` and `dlq-raw-payload` headers; forwards raw bytes to `weather-dead-letter`. |
| **Schema Registry** | Confluent Schema Registry (`:8081`) | Avro over HTTP + Kafka (`_schemas`) | Stores the `StatusMessage` Avro schema (persisted back inside Kafka via `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=kafka:9092`) so producers and consumers agree on binary layout. |

### Step-by-step walkthrough

1. **Get a safe Station ID on startup**: Before sending weather data, [`StationIdAllocator`](./my_producer/src/main/java/com/weather/my_prod/StationIdAllocator.java) checks `STATION_ID`. If set to a non-negative number, the producer skips Kafka coordination and uses that ID immediately. Otherwise, it creates/verifies `weather-station-ids` (`1` partition, infinite retention, ignoring `TopicExistsException`) and uses **Producer #1** (`acks=all`, `enable.idempotence=true`, retrying up to `5` times with `1s` backoff) to append one marker to partition `0` -> `station_id = offset + 1` (`1, 2, 3, ...`).
2. **Stream telemetry every second**: [`ProducerLoop`](./my_producer/src/main/java/com/weather/my_prod/ProducerLoop.java) keeps that assigned `station_id` and uses **Producer #2** (`LongSerializer` + `KafkaAvroSerializer` talking to Schema Registry `:8081`) to send one `StatusMessage` per second (`10%` simulated drop) to `weather-stations` with `station_id` as both the record key and payload field.
3. **Fan-out #1 — Real-time rain alerts**: [`WarningApp`](./my_stream/src/main/java/com/weather/my_stream/WarningApp.java) (`weather-streams`) reads `weather-stations` with `SpecificAvroSerde`, drops null/non-rainy events, transforms readings with `humidity > 70%` into a JSON `RAIN_TRIGGER` payload, and publishes them to `rain-alerts`.
4. **Fan-out #2 — Central ingestion, backpressure, and DLQ**: [`ConsumerLoop`](./base_central_station/src/main/java/com/weather/consumer/ConsumerLoop.java) (`base-station-group`) reads the same `weather-stations` topic independently using [`SafeStatusMessageDeserializer`](./base_central_station/src/main/java/com/weather/consumer/SafeStatusMessageDeserializer.java):
   - **If deserialization fails**: Instead of crashing `poll()`, the deserializer attaches `dlq-error` and `dlq-raw-payload` headers and returns `null`, letting **Producer #3** forward the raw payload and error header to `weather-dead-letter`.
   - **If valid**: `ConsumerLoop` passes the `StatusMessage` to [`Pipeline`](./base_central_station/src/main/java/com/weather/pipeline/Pipeline.java), which reconstructs dropped packets, writes synchronously to **BitCask KV** (`:8080`), and enqueues onto a `10,000`-item `ArrayBlockingQueue` feeding **Parquet -> `parquet_es_indexer` -> Elasticsearch (`:9200`) -> Kibana (`:5601`)**. When the queue reaches 80%, `ConsumerLoop` pauses its partitions until the queue drains to 50%.

---

## Modules and components

| Module / Component | Stack | Role | Key Files |
| :--- | :--- | :--- | :--- |
| **`shared_model`** | Java 21 / Avro | Shared `StatusMessage.avsc` contract (`SpecificRecord`) and env-backed `AppConfigs` | [StatusMessage.avsc](./shared_model/src/main/resources/avro/StatusMessage.avsc), [AppConfigs.java](./shared_model/src/main/java/com/weather/config/AppConfigs.java) |
| **`my_producer`** | Java 21 / Kafka Client | Simulates weather station readings (1s interval, 10% simulated drops) with monotonic station IDs | [Main.java](./my_producer/src/main/java/com/weather/my_prod/Main.java), [ProducerLoop.java](./my_producer/src/main/java/com/weather/my_prod/ProducerLoop.java), [StationIdAllocator.java](./my_producer/src/main/java/com/weather/my_prod/StationIdAllocator.java) |
| **`my_stream`** | Java 21 / Kafka Streams | Filters `humidity > 70%` and emits JSON alerts to `rain-alerts` | [WarningApp.java](./my_stream/src/main/java/com/weather/my_stream/WarningApp.java) |
| **`base_central_station`** | Java 21 / BitCask + Parquet | Kafka consumer (DLQ + backpressure), gap detector, BitCask KV engine + HTTP `:8080`, Parquet archiver | [Main.java](./base_central_station/src/main/java/com/weather/Main.java), [Pipeline.java](./base_central_station/src/main/java/com/weather/pipeline/Pipeline.java), [BitCaskStore.java](./base_central_station/src/main/java/com/weather/bitcask/engine/BitCaskStore.java), [ParquetArchiver.java](./base_central_station/src/main/java/com/weather/archive/engine/ParquetArchiver.java) |
| **`parquet_es_indexer`** | Java 21 / ES Java Client | Polls Parquet partitions, tracks SHA-256 file ledger, bulk-indexes Elasticsearch | [IndexerMain.java](./parquet_es_indexer/src/main/java/com/weather/indexer/IndexerMain.java), [ElasticsearchBulkIndexer.java](./parquet_es_indexer/src/main/java/com/weather/indexer/ElasticsearchBulkIndexer.java) |
| **`bitcask_client.sh`** | Bash / `curl` | CLI client and `/view-key` benchmark tool for BitCask HTTP endpoints (`-view-key`, `-view-all`, `-n=<threads>`, `-bench-key`) | [bitcask_client.sh](./bitcask_client.sh) |
| **Platform services** | Kafka (KRaft), Schema Registry 7.5.0, Elastic 8.18.0 | Broker (`:9092`), schema registry (`:8081`), search (`:9200`), dashboards (`:5601`) | [docker-compose.yml](./docker-compose.yml), [k8s/](./k8s) |

---

## Telemetry contract

Events conform to [`shared_model/src/main/resources/avro/StatusMessage.avsc`](./shared_model/src/main/resources/avro/StatusMessage.avsc):

```json
{
  "station_id": 104,
  "s_no": 4821,
  "status": "RECEIVED",
  "battery_status": "medium",
  "status_timestamp": 1721643065,
  "weather": {
    "humidity": 82,
    "temperature": 76,
    "wind_speed": 15
  }
}
```

- **Simulated readings (`ProducerLoop`)**: `humidity` `20-100%`, `temperature` `30-110 F`, `wind_speed` `0-50 km/h`, and `battery_status` in `{low, medium, high}`.
- **Synthetic drop records (`Pipeline`)**: `"status": "DROPPED"`, `"battery_status": "unknown"`, and `"weather": null`.

---

## Prerequisites

- **JDK 21** and **Apache Maven 3.8+**
- **Docker Engine** and **Docker Compose**
- **Minikube** and **`kubectl`** (only needed for Kubernetes deployment)
- **Hardware**: **8 GB RAM** and **4 CPU cores** recommended for the full stack

---

## Quickstart

### One-command launch

Docker Compose stack:

```bash
chmod +x start-docker.sh && ./start-docker.sh
```

Minikube / Kubernetes stack (scales `producer` to `10` replicas):

```bash
chmod +x start-k8s.sh && ./start-k8s.sh
```

Search stack only (`parquet-indexer`, `elastic-search`, `kibana`):

```bash
chmod +x start_search_stack.sh && ./start_search_stack.sh
```

### Manual Docker Compose

```bash
# 1. Build shaded JARs across all modules
mvn clean package -Dmaven.test.skip=true

# 2. Start services in detached mode
docker-compose up -d --build

# 3. Inspect status or tail logs
docker-compose ps
docker-compose logs -f consumer stream

# 4. Tear down containers and volumes
docker-compose down -v
```

### Manual Minikube / Kubernetes

```bash
# 1. Start cluster and target Minikube's Docker daemon
minikube start --memory=8192 --cpus=4 --driver=docker
eval $(minikube docker-env)

# 2. Build application images
docker build -f Dockerfile.producer.minimal -t weather-producer:latest .
docker build -f Dockerfile.stream.minimal   -t weather-stream:latest .
docker build -f Dockerfile.consumer.minimal  -t weather-consumer:latest .
docker build -f DockerFile.indexer           -t weather-indexer:latest .

# 3. Apply volumes, ConfigMap, and ordered deployments
chmod +x k8s/deploy-all.sh && ./k8s/deploy-all.sh

# 4. Watch pods and open Kibana
kubectl get pods -w
minikube service kibana --url

# 5. Clean up
kubectl delete -f k8s/
```

---

## Querying live state (BitCask HTTP & CLI)

`BitCaskServer` listens on port `8080` and serves `GET` endpoints directly from the in-memory `ConcurrentHashMap` index (`inMemoryIndex`) and open `.data` `SegmentFile` readers, returning `Content-Type: text/plain; charset=UTF-8` with explicit UTF-8 encoding:

| Request | Status | Response |
| :--- | :--- | :--- |
| `GET /view-key?id=101` (or `?foo=1&id=101`) | `200 OK` | `StatusMessage` string from active, sealed, or compacted segment |
| `GET /view-key?id=99999` (absent key) | `404 Not Found` | `Key Not Found` |
| `GET /view-key`, `/view-key?id=`, `/view-key?id=abc` | `400 Bad Request` | `Missing ID parameter` or `Invalid ID format` |
| `GET /view-all` | `200 OK` | Newline-delimited `key,StatusMessage` lines for all indexed keys |
| Non-`GET` request to either endpoint | `405 Method Not Allowed` | `Method Not Allowed` |

Query parameters are decoded from the raw query string (`&`-delimited pairs split at the first `=`), so extra parameters before or after `id` work and empty/missing values return `400` safely.

`BitCaskServer` dispatches incoming HTTP requests to a bounded `ThreadPoolExecutor` (`corePoolSize = Math.max(4, cpus * 2)`, `maximumPoolSize = Math.max(16, cpus * 4)`, `60s` idle keep-alive, `bitcask-http-worker-*` daemon threads, and `AbortPolicy` overload rejection) backed by a bounded `ArrayBlockingQueue<>(1024)`:
1. If fewer than `corePoolSize` threads exist, it starts a new thread immediately.
2. Otherwise, it tries putting the request into the `ArrayBlockingQueue<>(1024)` waiting line.
3. Only if the queue is completely full does it start extra threads up to `maximumPoolSize` (rejecting excess traffic via `AbortPolicy` once both the queue and `maximumPoolSize` are saturated so excess connections close cleanly instead of stalling the HTTP dispatcher).

```bash
# Latest reading for station 101
curl -s "http://localhost:8080/view-key?id=101"

# Extra parameters in any order work
curl -s "http://localhost:8080/view-key?foo=1&id=101"

# CSV dump (key,StatusMessage) of all stations
curl -s "http://localhost:8080/view-all"
```

Use [`bitcask_client.sh`](./bitcask_client.sh) to query, export timestamped CSVs, or run concurrent load checks (`API_URL` defaults to `http://localhost:8080` locally and `http://consumer:8080` in Compose):

```bash
# Point lookup and full CSV export
./bitcask_client.sh -view-key 101
./bitcask_client.sh -view-all

# Concurrent /view-all load check (writes N timestamped CSV files)
./bitcask_client.sh -n=50

# Concurrent /view-key benchmark (in-memory metrics only; no CSV files written)
./bitcask_client.sh -bench-key -c=50 -n=1000

# Optional explicit station ID range or list (defaults to live keys from /view-all, fallback 1-10)
./bitcask_client.sh -bench-key -c=50 -n=1000 -keys=1-10
```

`-bench-key` keeps `-c` concurrent `curl` workers sending `GET /view-key?id=<random_station_id>` requests (for `-n` total requests, defaulting to `-c=10 -n=1000`), discards response bodies to `/dev/null` instead of writing per-request CSV files, and prints summary metrics: total requests, HTTP `2xx`/`404`/`5xx` (and connection/other) counts, `requests/sec`, and `P50`/`P95`/`P99` latency in `ms`.

---

## BitCask storage engine internals

[`BitCaskStore`](./base_central_station/src/main/java/com/weather/bitcask/engine/BitCaskStore.java), [`SegmentFile`](./base_central_station/src/main/java/com/weather/bitcask/engine/SegmentFile.java), [`AvroSerializer`](./base_central_station/src/main/java/com/weather/bitcask/engine/AvroSerializer.java), [`HintFile`](./base_central_station/src/main/java/com/weather/bitcask/engine/HintFile.java), and [`Compactor`](./base_central_station/src/main/java/com/weather/bitcask/engine/Compactor.java) implement an append-only log-structured key-value store:

- **Record layout (`SegmentFile`)**: Each entry writes a 24-byte header (`4B CRC32` + `8B timestamp` + `8B key` + `4B valueSize`) followed by the Avro binary `StatusMessage` payload. `readEntry(offset)` and startup recovery verify CRC32 checksums on read.
- **Concurrent read hot path (`BitCaskStore.get`, `SegmentFile`, `AvroSerializer`)**:
  - `SegmentFile.readEntry(offset)` performs lock-free positional reads via `FileChannel.read(ByteBuffer, position)` (looping in `readFully` to handle partial channel reads and throwing `EOFException` on `-1`) instead of `synchronized` `RandomAccessFile.seek()`, allowing parallel reads across active and sealed segments.
  - `BitCaskStore.get()` logs hit lookups at `DEBUG` (rather than `INFO`) to avoid synchronous `ConsoleAppender` bottlenecks under high-QPS `/view-key` traffic.
  - `AvroSerializer` reuses per-thread `ThreadLocal` `SpecificDatumWriter`/`SpecificDatumReader`, `BinaryEncoder`/`BinaryDecoder`, and a `ByteArrayOutputStream` buffer (clearing `ENCODER`/`DECODER` on errors) to avoid per-request allocations.
- **Active segment rotation (`BitCaskStore.put`)**:
  - When `currentOffset >= MAX_SEGMENT_SIZE_KB * 1024`, rotation allocates a strictly monotonic segment ID via `Math.max(Instant.now().toEpochMilli(), currentSegmentId + 1)` so rapid rotations within the same millisecond never collide on `datafile_<id>.data`.
  - The new active `SegmentFile` is opened before sealing the old segment with `closeWriter()`, which flushes and closes only the `DataOutputStream` writer while keeping the `FileChannel` reader open in `segmentFilesById` for `/view-key` and `store.get()` lookups.
  - A `.hint` file (`key`, `segmentId`, `offset`, `valueSize`) is written best-effort in a `try-with-resources` block; if `.hint` creation fails, the error is logged without leaving `activeSegment` null or closed, and startup recovery (`recoverSegmentsWithoutHints()`) rebuilds missing hint files by scanning `.data` records and truncating any torn tail.
- **Compaction (`Compactor.compact`)**:
  - Triggered periodically (`COMPACTION_INTERVAL_MINS`, default `3` min) when at least two closed segments exist.
  - Streams only live records (matching `inMemoryIndex` segment ID and offset) whose CRC32 validates into a new monotonic segment file and writes its `.hint` file.
  - Registers the compacted segment, atomically updates `inMemoryIndex` via `updateIndexIfNewer()` (preserving newer active writes), swaps segment maps via `replaceSegments()`, and then calls `newSegment.closeWriter()` so reads against the compacted segment stay open.
  - Closes old `SegmentFile` handles in `segmentsToCompact` before deleting their `.data` and `.hint` files so file descriptors are not leaked. If compaction aborts or throws before `replaceSegments()`, `newSegment` is closed completely and its uncommitted `.data`/`.hint` files are deleted.

---

## Testing

JUnit 4 unit and HTTP integration tests live in `base_central_station` (and `shared_model`). Because `base_central_station` depends on `com.weather:shared_model:1.0-SNAPSHOT`, install `shared_model` to your local Maven repository first if schema or `AppConfigs` changes haven't been installed yet:

```bash
# Install parent POM and shared_model locally
mvn -pl shared_model -am install

# Run producer and central station test suites
mvn -pl my_producer,base_central_station test
```

Test coverage:
- [`StationIdAllocatorTest`](./my_producer/src/test/java/com/weather/my_prod/StationIdAllocatorTest.java) & [`ProducerLoopTest`](./my_producer/src/test/java/com/weather/my_prod/ProducerLoopTest.java): verify monotonic `1, 2, 3, ...` station ID allocation across sequential/concurrent containers via partition `0`, `STATION_ID` override parsing and bypass behavior, transient topic-creation retries, and `ProducerLoop` emission with the allocated ID.
- [`BitCaskStoreTest`](./base_central_station/src/test/java/com/weather/bitcask/engine/BitCaskStoreTest.java): verifies `store.get(key)` reads from sealed segments after sub-millisecond rotations with strictly monotonic segment IDs, multi-threaded concurrent `get()` calls across active and sealed segments, partial `FileChannel` reads / `EOFException` / `closeReader()` behavior, rotation resilience when `.hint` file creation fails, `Compactor.compact()` merging multiple closed segments while preserving live reads and disk recovery, and clean abort cleanup when a segment read fails before `replaceSegments()`.
- [`BitCaskServerTest`](./base_central_station/src/test/java/com/weather/bitcask/server/BitCaskServerTest.java): runs an embedded `BitCaskServer` on an ephemeral port and checks `200` responses across active, sealed, and compacted segments (including extra query parameters like `?foo=1&id=101`), bounded `ThreadPoolExecutor` / queue / named-worker configuration under concurrent `/view-key` traffic, `404` for unknown keys, `400` for missing/empty/malformed `id` queries, `405` for non-`GET` methods, `/view-all`, and UTF-8 `Content-Type` headers.

---

## Visualizing history in Kibana

1. Open Kibana at `http://localhost:5601` (or `minikube service kibana --url`).
2. Go to **Stack Management** > **Data Views**.
3. Create a data view for `weather-status*` using `@timestamp` as the timestamp field.
4. Inspect `station_id`, `status` (`RECEIVED` vs `DROPPED`), `temperature`, `humidity`, `wind_speed`, and `battery_status` in **Discover** or **Dashboards**.

---

## Configuration reference

Environment variables read by [AppConfigs.java](./shared_model/src/main/java/com/weather/config/AppConfigs.java) and [IndexerConfig.java](./parquet_es_indexer/src/main/java/com/weather/indexer/IndexerConfig.java):

| Variable | Code Default | Compose / K8s Value | Purpose |
| :--- | :--- | :--- | :--- |
| `KAFKA_BROKER` | `localhost:9092` | `kafka:9092` | Kafka bootstrap servers |
| `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` | `http://schema-registry:8081` | Confluent Schema Registry URL |
| `KAFKA_PRODUCER_TOPIC` | `weather-stations` | `weather-stations` | Producer target topic |
| `STATION_ID` | unset (dynamic via Kafka) | unset | Optional pinned non-negative station ID override for a producer instance |
| `STATION_ID_TOPIC` | `weather-station-ids` | `weather-station-ids` | Single-partition Kafka coordination topic for monotonic station ID allocation |
| `KAFKA_WARNING_INPUT_TOPIC` | `weather-stations` | `weather-stations` | Kafka Streams input topic |
| `KAFKA_WARNING_OUTPUT_TOPIC` | `rain-alerts` | `rain-alerts` | Kafka Streams JSON alert topic |
| `KAFKA_HUMIDITY_THRESHOLD` | `70` | `70` | Rain alert humidity cutoff (%) |
| `KAFKA_CONSUMER_TOPIC` | `weather-stations` | `weather-stations` | Central station input topic |
| `KAFKA_CONSUMER_GROUP_ID` | `base-station-group` | `base-station-group` | Kafka consumer group ID |
| `KAFKA_DEAD_LETTER_TOPIC` | `weather-dead-letter` | `weather-dead-letter` | DLQ topic for deserialization failures |
| `PARQUET_OUTPUT_BASE_PATH` | `/data/parquet_output` | `/app/parquet_output` | Root directory for hourly Parquet output |
| `PARQUET_BATCH_SIZE` | `1000` | `1000` | Records buffered per partition before flush |
| `PARQUET_FLUSH_INTERVAL_SECS` | `30` | `30` | Max batch age and stale-partition sweep interval |
| `BITCASK_STORE_DIRECTORY` | `bitcask_data` | `/app/bitcask_data` | Directory for `.data` and `.hint` files |
| `MAX_SEGMENT_SIZE_KB` | `51200` (50 MB) | `10000` (10 MB) | Active segment rotation threshold |
| `DISK_FLUSH_THRESHOLD` | `1000` | `10000` | Writes before flushing segment stream |
| `HINT_FLUSH_THRESHOLD` | `100` | `20` | Entries before flushing `.hint` stream |
| `COMPACTION_INTERVAL_MINS` | `3` | `3` | Period in minutes for `Compactor` runs |
| `ELASTICSEARCH_URL` | `http://localhost:9200` | `http://elastic-search:9200` | Elasticsearch endpoint for `parquet_es_indexer` |
| `ELASTICSEARCH_INDEX` | `weather-status` | `weather-status` | Target Elasticsearch index |
| `INDEXER_POLL_INTERVAL_SEC` | `30` | `30` | Parquet polling interval (seconds) |
| `INDEXER_STATE_FILE` | `/app/state/indexed-files.txt` | `/app/state/indexed-files.txt` | Persistent ledger of indexed Parquet files |

---

## Repository structure

```text
.
├── base_central_station/         # Consumer, pipeline, BitCask KV + HTTP server, Parquet archiver
│   ├── src/main/java/com/weather/
│   │   ├── Main.java             # Wires store, archiver, pipeline, compactor, API server, consumer
│   │   ├── archive/              # ParquetArchiver, PartitionKey, TimeBucketExtractor
│   │   ├── bitcask/              # BitCaskStore, AvroSerializer, Compactor, SegmentFile, HintFile, BitCaskServer
│   │   ├── consumer/             # BaseStationConsumer, ConsumerLoop, SafeStatusMessageDeserializer
│   │   └── pipeline/             # Pipeline (gap synthesis, backpressure, async Parquet queue)
│   └── src/test/java/com/weather/bitcask/
│       ├── engine/BitCaskStoreTest.java
│       └── server/BitCaskServerTest.java
├── my_producer/                  # Weather station simulator and Kafka Avro producer
│   ├── src/main/java/com/weather/my_prod/
│   │   ├── Main.java
│   │   ├── Producer.java
│   │   ├── ProducerLoop.java
│   │   └── StationIdAllocator.java
│   └── src/test/java/com/weather/my_prod/
│       ├── ProducerLoopTest.java
│       └── StationIdAllocatorTest.java
├── my_stream/                    # Kafka Streams humidity > 70% alert processor
├── parquet_es_indexer/           # Parquet scanner, SHA-256 ledger, mapper, Elasticsearch bulk indexer
├── shared_model/                 # StatusMessage.avsc schema and AppConfigs
├── docs/
│   ├── kafka_complete_flow.excalidraw
│   ├── kafka_complete_flow.svg
│   ├── monotonic_station_id.excalidraw
│   ├── monotonic_station_id.svg
│   └── system_architecture_ref.svg
├── k8s/                          # Kubernetes deployments, services, PVCs, ConfigMap, deploy-all.sh
├── bitcask_client.sh             # Bash HTTP client for BitCaskServer (:8080)
├── start-docker.sh               # Maven build and Docker Compose launcher
├── start-k8s.sh                  # Minikube image build and K8s deployment launcher
├── start_search_stack.sh         # Starts indexer, Elasticsearch, and Kibana via Docker Compose
├── docker-compose.yml            # Multi-container Compose definition
└── pom.xml                       # Root Maven multi-module POM
```
