# Decomposing the Graylog server into independently scalable components

Status: exploration / discussion draft. Nothing in this document is implemented.

This document records what the `server` process looks like today, the concrete
couplings that stop it from scaling by concern, the seams that already exist, and
a proposed target model with the architectural changes each step requires.
Every statement about current behaviour references the code it was derived from
(paths are relative to `graylog2-server/src/main/java/` unless noted). Sections
marked *Proposal* are design options, not facts about the codebase.

Out of scope: closed-source enterprise plugins and the Forwarder. The OSS tree
contains accommodations for them (see §3) but their internals could not be
verified here.

---

## 1. The server process today

`org.graylog2.commands.Server` builds one Guice injector from roughly 45 modules
(`Server#getNodeCommandBindings`) plus the shared modules from
`ServerBootstrap#getSharedBindingsModules`. All runtime work is expressed as
Guava `Service`s collected into a single, flat, unordered
`ServiceManager` (`shared/bindings/providers/ServiceManagerProvider.java`).
Startup order between services is ad hoc: `InputSetupService` waits for
`Lifecycle.RUNNING`, `JobSchedulerService` waits on `ServerStatus#awaitRunning`,
`OutputSetupService` tears down on `BufferSynchronizerService.terminated`.
One failing service moves the whole node to `Lifecycle.FAILED`
(`shared/initializers/ServiceManagerListener.java`).

The services, grouped by concern:

| Concern | Services (all started on every `server` node) | External touch points |
|---|---|---|
| Ingest | `InputSetupService`, `InputBufferImpl` (Disruptor), `LocalKafkaJournal`, `LocalKafkaMessageQueueWriter` | Listening sockets, local disk |
| Processing / indexing | `LocalKafkaMessageQueueReader` → `ProcessBuffer` → `OutputBuffer` → `BatchedMessageFilterOutput`/`ElasticSearchOutput`, `OutputSetupService`, `BufferSynchronizerService`, `FailureHandlingService`, `LookupTableService`, `StreamCacheService`, `GeoIpDbFileChangeMonitorService` | OpenSearch, Mongo, third-party lookups, local files |
| API / UI | `JerseyService` (REST + SPA on one port), `PrometheusExporter` | Listening socket |
| Background work | `PeriodicalsService` (34 periodicals), `UserJobSchedulerService`, `SystemJobSchedulerService`, `NotificationSystemEventPublisher`, `ConfigureCertRenewalJobOnStartupService` | Mongo, OpenSearch, SMTP/HTTP |
| Cluster plumbing | `ClusterEventService` (tailable cursor on capped `cluster_events`), `LeaderElectionService` (started outside the ServiceManager), `MongoDBProcessingStatusRecorderService`, `NodePingThread` | Mongo |
| Misc | `UserSessionTerminationService`, `UrlAllowlistService`, sidecar `EtagService`, collectors caches | Mongo |

Behaviour is gated today by exactly four mechanisms, none of which is a node role:

1. **Leadership**: `PeriodicalsService` starts/stops `leaderOnly()` periodicals on
   `LeaderChangedEvent`; `DefaultJobSchedulerConfig#canExecute` returns
   `leaderElectionService.isLeader()`; six REST endpoints carry `@RestrictToLeader`;
   `InputEventListener` starts `onlyOnePerCluster()` inputs on the leader.
2. **Configuration flags**: `message_journal_enabled`/`message_journal_mode`,
   `is_cloud` (read at ~25 sites), `global_inputs_only`, `no_retention`,
   `content_packs_loader_enabled`, and the `GraylogNodeConfiguration#with*()` switches.
3. **A system property** `graylog.forwarder`, checked in `AWSModule` and
   `IntegrationsModule` to skip Mongo-dependent bindings inside the Forwarder.
4. **Plugin capabilities**: `CmdLineTool#loadPlugins` filters plugins on
   `ServerStatus.Capability` (`SERVER`, `MASTER`, `LOCALMODE`, `CLOUD`), which is
   otherwise vestigial.

`Node`/`ServerNodeDto` (`cluster/nodes/`) carry `is_leader`, `is_processing`,
`lifecycle`, `lb_status`, `transport_address`. There is no role field.

---

## 2. Why it scales badly: the coupling map

### 2.1 Ingest is welded to processing by the local journal

- The only hand-off between inputs and processing is `MessageQueueWriter` →
  `LocalKafkaJournal` (on-disk, per host) → `LocalKafkaMessageQueueReader`.
  The reader is compiled against the in-process `ProcessBuffer` and a
  `@Named("JournalSignal")` semaphore: it reads exactly
  `processBuffer.getRemainingCapacity()` entries and blocks on the semaphore
  when the journal is empty (`shared/messageq/localkafka/LocalKafkaMessageQueueReader.java:109-117`).
- Backpressure is entirely intra-process: `ThrottleStateUpdaterThread` only
  starts when the journal is a `LocalKafkaJournal`, computes `ThrottleState`
  from local journal offsets and process-buffer capacity, and inputs throttle
  themselves via `ThrottleableTransport`. The load-balancer status
  (`/system/lbstatus`) flips to `THROTTLED` from journal utilisation inside
  `LocalKafkaJournal` (`lb_throttle_threshold_percentage`).
- Journal retention will delete uncommitted segments above
  `message_journal_max_size` (default 5 GB); this is the data-loss valve, and
  it is a per-node disk, so ingest capacity is bounded by local storage.

### 2.2 Processing is welded to indexing by the acknowledgement path

- `ElasticSearchOutput#writeFiltered` resolves `stream.getIndexSet().getWriteIndexAlias()`
  inline, and `Messages#bulkIndex` advances the `post_indexing` watermark.
- The journal commit is a **monotone high-water mark** (`LocalKafkaJournal#markJournalOffsetCommitted`
  uses `max(offset, prev)`), triggered from `BatchedMessageFilterOutput#flush`
  *after* all filtered outputs wrote the batch. `flush` catches exceptions and
  still acknowledges, so a throwing individual output yields at-most-once past
  that batch. Any split must preserve "ack only after indexing" and should take
  the opportunity to make this per-message.
- `processing_status` (`system/processing/ProcessingStatusDto.java`) is one
  document per node that mixes ingest facts (`input_journal.uncommitted_entries`,
  `process_buffer_usage`) with indexing facts (`receive_times.post_indexing`).
  `DBProcessingStatusService#calculateProcessingState` and therefore
  `AggregationEventProcessor` (via `EventProcessorDependencyCheck`) depend on
  both halves being in the same document. Splitting tiers breaks alerting
  unless this schema is reworked.

### 2.3 Inputs run everywhere because placement is a boolean

- Placement is `global == true` (every node) or `node_id == X` (pinned, no
  failover). The single choke point is `PersistedInputsImpl#iterator` →
  `InputServiceImpl#allOfThisNode`, plus three verbatim copies of
  `input.isGlobal() || nodeId.equals(input.getNodeId())` in `InputEventListener`.
- `onlyOnePerCluster()` inputs migrate with leadership.
- The input control plane assumes Mongo (`inputs`, `input_runtime_states`,
  `input_states` cursors), the `cluster_events` tail, and REST fan-out
  (`ClusterInputStatesResource` → `RemoteInputStatesResource` on every node).
  Start/stop is *not* cluster-broadcast; it persists `desired_state` then
  posts to the local bus, relying on the fan-out to hit every node.
- TLS material for inputs is file paths on the node (`tls_cert_file`, …),
  with a self-signed fallback minted into `java.io.tmpdir`.
- Helpful: `RawMessage` carries a source-node chain (`addSourceNode`), and
  extractors/static fields already run *after* the journal, keyed by
  `gl2_source_input` (`filters/ExtractorFilter.java`), so they naturally stay
  on the processing side.

### 2.4 Leader obsession

- 24 of 34 periodicals are `leaderOnly()`; the highest-frequency ones are
  `IndexFieldTypePollerPeriodical` (1 s), `GraylogCertificateProvisioningPeriodical`
  (2 s), `DataNodeHousekeepingPeriodical` (2 s), `IndexRotationThread` (10 s).
- The **user job scheduler executes only on the leader** with a hard-coded
  pool of 5 threads (`scheduler/DefaultJobSchedulerConfig.java`). That is every
  event-definition execution and every notification delivery in OSS, on one node.
  The system job scheduler, by contrast, already runs on every node and claims
  triggers with a Mongo lease (`DBJobTriggerService#nextRunnableTrigger`).
- Default election mode is `STATIC` (`is_leader` in the config file). Automatic
  mode is a Mongo lock with a TTL index; the class comment warns expiry may take
  ~2 minutes. In-flight job triggers are reclaimed only after
  `job_scheduler_lock_expiration_duration` (5 min). So a leader crash means
  minutes of no rotation, no alerting, no notifications.
- Migrations of type `STANDARD`/`PREFLIGHT` run on the leader at startup
  (`ServerBootstrap#startCommand`), so the leader must boot first.

### 2.5 The REST layer assumes every node is a full node

- `ProxiedResource#requestOnAllNodes` fans out to every entry in `nodes`
  via `transport_address`; the only filter is `Node::isLeader`. Roughly fifteen
  `Cluster*Resource`s aggregate in-heap state: `MetricRegistry`,
  `InputRegistry`, `LocalKafkaJournal`, `LegacySystemJobManager` (deprecated,
  in-memory jobs), Log4j levels and the `MemoryAppender` ring, node-local lookup
  caches (`ClusterLookupTableResource` purge), `ProcessBuffer#getDump`,
  the live `ProcessingStatusRecorder`.
- Views search jobs are in-memory per node (`InMemorySearchJobService`, a Guava
  cache, `nodeId` stamped on the job). `SearchJobsStatusResource` exists solely
  to route status/cancel to the originating node. `SearchJobService` is an
  `OptionalBinder` default, and a Mongo-backed `SearchJobStateService` already
  exists for data-lake jobs.
- The web UI is served by every node (`WebResourcesModule` installed
  unconditionally by `RestApiBindings`); there is no flag to disable it.
- Sessions live in Mongo (`MongoDbSessionService`), with a per-node Shiro cache;
  any node can authenticate any request. This part is already stateless.
- `SimulatorResource` uses the node's live compiled pipeline state; results
  depend on which node answers.

### 2.6 Configuration distribution

Every node loads streams, pipelines, rules, extractors, grok patterns, lookup
tables, index sets, processor order, and cluster config from Mongo and refreshes
them from the `cluster_events` capped-collection tail (`events/ClusterEventService.java`).
This works for any new tier, but it means each tier needs Mongo connectivity,
the `ClusterEventService`, and identical plugin sets (unknown event classes are
silently skipped). There is no consumer offset persisted across restarts beyond
"now", and `ClusterConfigService#get` hits Mongo uncached on every call.

### 2.7 Bootstrap and composition

- `AbstractNodeCommand` + `GraylogNodeConfiguration` is the sanctioned node
  template, but only `GraylogNodeModule` is genuinely shared; the data node
  re-implements `GenericBindings`, `GenericInitializerBindings`,
  `SchedulerBindings`, `PeriodicalBindings`, `ConfigurationModule`, `Main`
  by copy.
- `PluginModule`/`Graylog2Module` expose ~120 extension keys (`addMessageInput`,
  `addPeriodical`, `addRestResource`, `addSchedulerJob`, `installLookupDataAdapter`,
  …) that all land in one injector. A decomposition must decide, per key, which
  component receives it. `DatanodePlugin`/`DatanodePluginLoader` is the existing
  precedent for scoping plugins to a node type.

---

## 3. Seams that already exist

These are the places where the code already has an abstraction we can build on
rather than invent.

| Seam | Where | What it gives us |
|---|---|---|
| Message queue SPI | `shared/messageq/{MessageQueueReader,Writer,Acknowledger}`, `PluginModule#bindMessageQueueImplementation`, `message_journal_mode` | Broker-agnostic reader/writer/ack with an opaque `messageQueueId`; `MessageQueueModule` comments imply other modes plug in from outside |
| Node template | `AbstractNodeCommand`, `GraylogNodeConfiguration#with*()`, `CliCommandsProvider` via `META-INF/services`, `AbstractJournalCommand` as a minimal-node example | Declaring a new command that opts out of Mongo/event bus/plugins/inputs |
| Node-type plugin scoping | `DatanodePlugin`, `DatanodePluginLoader` | Marker-interface pattern for role-specific plugin loading |
| Periodical routing | `Periodical#leaderOnly()`, `#startOnThisNode()` | Per-node gating of background work |
| Distributed job claiming | `DBJobTriggerService#nextRunnableTrigger` (lease by node id, stale-lock steal), `JobTriggerDto#constraints` + `SchedulerCapabilitiesService` (`org.graylog.cluster.is-leader`), `SystemJobManager#submitWithConstraints` | Work placement by capability, already used cluster-wide by the system scheduler |
| Scheduler override | `JobSchedulerModule:52` `OptionalBinder<JobSchedulerConfig>` | Replacing "leader only" with a role predicate without touching the engine |
| Search job override | `ViewsBindings:232` `OptionalBinder<SearchJobService>`, `SearchJobStateService` (Mongo) | Making async search node-independent |
| Per-node state in Mongo | `input_runtime_states` (unique `(input_id,node_id)`), `processing_status` (unique `node_id`), `StaleInputRuntimeStateCleanup` | Fleet-wide status without HTTP fan-out; `InputRuntimeStatusProvider` already avoids it |
| Source attribution | `RawMessage#addSourceNode`, `gl2_source_input`, `FIELD_GL2_FORWARDER_INPUT` handling in `DecodingProcessor`/`ProcessBufferProcessor` | Messages crossing process boundaries keep input identity |
| Forwarder accommodations | `MessageInput#isForwarderCompatible`, `graylog.forwarder` property, `FakeLeaderElectionService`, `IOState.Type.UNRECOGNIZED` | Evidence that an input-only process without Mongo is viable for a subset of inputs |
| Topology discovery | `NodeService<T extends NodeDto>` with typed DTOs and collections (`nodes`, `datanodes`), `IndexerDiscoveryListener` | Typed per-role registries |
| Cluster-wide quiesce | `system/processing/control/ClusterProcessingControl` | Existing pause/drain protocol across nodes |

---

## 4. Proposal: target component model

*Proposal.* Split by scaling characteristic, not by package. Each component is
a role; a process can run one or several roles. `server` remains "all roles"
so single-node and existing multi-node deployments keep working.

| Role | Runs | External deps | Scaling model | Scale to zero? |
|---|---|---|---|---|
| `ingest` | Inputs, input buffer, queue writer, input runtime-state reporting | Sockets, broker, Mongo (config + state) | Horizontal by input type / partition; listening inputs need ≥1 replica per input | Pull-based inputs yes; listening inputs no (but per input type) |
| `processing` | Queue reader, process buffer, message processors, output buffer, outputs incl. indexer | Broker, Mongo, OpenSearch, lookup sources | Horizontal on consumer-group lag | Yes, when the broker holds the backlog |
| `api` | REST, web UI, sync search, export streaming | Mongo, OpenSearch | Horizontal on request load, stateless | Yes |
| `worker` | User + system job schedulers (event processors, notifications, index maintenance jobs), everything that is `leaderOnly()` today, expressed as jobs | Mongo, OpenSearch, SMTP/HTTP | Horizontal on trigger backlog; singleton jobs via lock/constraint | Yes, apart from a minimum cadence for rotation/retention |
| `migrate` (one-shot) | Schema migrations, preflight, CA/cert bootstrap | Mongo, OpenSearch | Job / init container | n/a |

Shared substrate for all roles: MongoDB (config, state, `cluster_events`), the
node registry with roles, the message broker, and OpenSearch/Data Node.

Design decisions embedded in this model, each of which is a choice you may
want to make differently:

- **Processing and indexing stay together in the first cut.** The ack path and
  `post_indexing` watermark are the tightest coupling (§2.2). Splitting them
  later means a second queue between processing and indexing; it is possible
  but is not what is needed for independent ingest/search scaling.
- **The leader disappears as a concept; singleton work becomes a job.** The
  worker role absorbs the leader-only periodicals as scheduler jobs with a
  singleton constraint. Leader election survives only as an implementation
  detail of "who runs singleton jobs", ideally replaced by the per-trigger
  lease that already exists.
- **Search async state moves to Mongo.** Without this the `api` role is sticky.
- **One binary, role list in config.** See §5.1 for the alternative.

---

## 5. What it takes: architectural changes in dependency order

Each item lists the changes it requires and the options where a real choice
exists. The numbering is a dependency order, not a strict sequence.

### 5.1 Introduce node roles (prerequisite for everything)

Required:
- A `roles` set on `ServerNodeDto`/`nodes`, written by `NodePingThread`,
  exposed by `NodeService` (`allActive(role)`), and a `NodeRoles` service
  bound per process.
- `ProxiedResource` becomes role-aware: `requestOnAllNodes(role, …)`; each
  `Cluster*Resource` declares the role its target state lives on (journal →
  `ingest`, input states → `ingest`, metrics → all, deflector → `worker`, …).
- Module composition by role: split `Server#getNodeCommandBindings` into
  role-tagged groups; extend `GraylogNodeConfiguration` (or a sibling) with the
  role set; plugin extension keys routed by role (a marker interface per role
  like `DatanodePlugin`, or a `@ForRoles` annotation on `PluginModule`s).
- Replace ad-hoc gates (`is_cloud`, `graylog.forwarder`, `global_inputs_only`)
  with role checks where they are really role checks.

Options:
1. **One binary, `node_roles = ingest,processing,api,worker`** (recommended to
   start). `server` with no setting means all roles. Lowest migration cost,
   one artifact, roles can be combined freely (e.g. `ingest,processing` for a
   classic node).
2. **One command per role** (`graylog ingest`, …) via `CliCommandsProvider`.
   Cleaner classpath per role, but multiplies bootstrap code that is already
   share-by-copy (§2.7). Could be added later on top of option 1.

### 5.2 Make the `api` role stateless

Required:
- Bind `SearchJobService` to a Mongo-backed implementation (extend
  `SearchJobStateService`) or accept sticky routing on `nodeId`; the former
  removes `SearchJobsStatusResource`'s reason to exist.
- Retire `LegacySystemJobManager` (already `@Deprecated(since="7.1")`); the
  remaining legacy jobs (`FixDeflectorBy*Job`, `IndexSetCleanupJob`) move to
  the system scheduler.
- Lookup-table purge and logger-level changes become cluster events instead
  of REST fan-out, or stay node-local by design.
- `SimulatorResource` and `ExtractorsResource#test` need the processing
  state and lookup tables; either the `api` role loads them too (cheap, Mongo
  only) or these calls are proxied to a `processing` node.
- Optionally make the web UI a separate role or a flag so `api` can be
  headless.

No broker involvement; this step is independently shippable.

### 5.3 Dissolve the leader into the `worker` role

Required:
- `JobSchedulerConfig#canExecute` → "this node has role `worker`";
  `numberOfWorkerThreads` configurable.
- Convert `leaderOnly()` periodicals into scheduler jobs with an interval
  schedule and a singleton constraint. The `constraints` + `SchedulerCapabilities`
  mechanism exists; a `singleton` capability or a per-job lock via
  `RefreshingLockService` provides exclusivity. Candidates with their current
  cadence: rotation (10 s), retention (5 m), field-type polling (1 s; needs
  redesign as it is also event-driven), index-range cleanup, token cleaners,
  cert provisioning (2 s), data-node housekeeping (2 s), version check,
  telemetry, sidecar/collector purges.
- `@RestrictToLeader` endpoints: deflector cycle becomes a system-job
  submission; support-bundle build writes to shared storage (Mongo GridFS or
  object storage) instead of the leader's disk.
- Migrations: the `migrate` command already exists; make it the only path and
  have `server` verify schema version rather than run migrations (or keep
  "first `worker` to acquire the migration lock runs them").
- Failover latency: replace the Mongo TTL-index lock expiry with an explicit
  heartbeat + `updated_at` comparison (the scheduler's `lock.last_lock_time`
  pattern), and lower `job_scheduler_lock_expiration_duration` defaults.
- `InputEventListener#leaderChanged` for `onlyOnePerCluster()` inputs is
  replaced by the input placement in §5.5.

### 5.4 Replace the local journal with a broker (ingest ⟂ processing)

This is the structural change. Required regardless of broker:

- A `MessageQueueWriter`/`Reader`/`Acknowledger` implementation for the broker,
  bound through `bindMessageQueueImplementation`, selected by
  `message_journal_mode`.
- Decouple the reader from `ProcessBuffer`: replace "read exactly remaining
  capacity + semaphore" with a pull loop with bounded prefetch; `Acknowledgeable`
  ids become `(partition, offset)` or broker-native ids; acks committed per
  consumer group after `BatchedMessageFilterOutput#flush`.
- Redefine backpressure: `ThrottleState` from consumer lag / broker quota
  instead of local journal offsets; LB status for `ingest` from broker write
  health; for `processing` from consumer lag.
- Split `processing_status`: `ingest` nodes report `ingest` receive time and
  queue write health; `processing` nodes report `post_processing`/`post_indexing`
  and lag. `DBProcessingStatusService#calculateProcessingState` is rewritten to
  combine them so `AggregationEventProcessor`'s guard keeps working.
- Ordering and duplicates: today ordering is per-node journal order and
  replays after a crash produce duplicates (ULIDs are assigned in
  `ProcessBufferProcessor`, i.e. after the journal). A partitioned broker keeps
  per-partition order; at-least-once stays the semantic. Decide whether
  `gl2_message_id` should be assigned at ingest so replays are idempotent.
- Payload: journal entries are `RawMessage` protobuf with codec name and
  filtered codec config already embedded (`MessageInput#codecConfig`), so the
  wire format exists.
- Keep `disk` mode: single-node and on-prem deployments keep the local journal,
  which means `ingest,processing` on one node must remain a supported
  combination.

Options for the broker:
1. **Kafka-compatible** (Kafka, Redpanda, cloud-managed equivalents). The
   journal is already Kafka's log format; partitions map to processing
   parallelism; mature consumer-group semantics. Operational weight on-prem.
2. **NATS JetStream / Pulsar**: lighter (NATS) or more multi-tenant (Pulsar);
   fewer users will already run them next to Graylog.
3. **Keep the local journal on `ingest` and ship over gRPC to `processing`**
   (the Forwarder pattern). No new infrastructure, but it re-creates a
   point-to-point topology, needs its own load balancing and ack protocol, and
   ingest disks remain the buffer. Reasonable as a transition or for edge cases.

The SPI shape does not force this decision; option 1 is the one the existing
code most naturally fits.

### 5.5 Input placement and ingest scaling

Required:
- Replace `global`/`node_id` with a placement spec on the input: role
  selector (default `ingest`), replica count or "all nodes with role",
  singleton flag (replaces `onlyOnePerCluster()`), and optional node
  affinity for legacy pinned inputs.
- A reconciler on `ingest` nodes: desired state (`inputs` + placement) vs
  actual (`input_runtime_states`), with singleton inputs claimed through
  `LockService`. This removes the three copies of the placement predicate and
  `PersistedInputsImpl#iterator`.
- Certificates for TLS inputs from Mongo/secret store rather than node-local
  paths (`AbstractTcpTransport`/`KeyUtil`), so any `ingest` replica can serve
  the input.
- Per-input-type sizing is a deployment concern (one Deployment per input
  group, all with role `ingest` and a selector), not a code concern, once the
  selector exists.
- Scale-to-zero for listening inputs is not possible while a listener must
  exist; for pull inputs (AWS, Kafka, HTTP poll, …) it is, as jobs on `worker`
  or as singleton `ingest` inputs.

### 5.6 Bootstrap and composition hygiene

Not strictly required, but the cost of every step above is lower after it:
- Extract a genuinely shared "node runtime" module set (service manager,
  periodicals, scheduler executors, node registry, cluster events, config
  module) used by `server`, `datanode`, and any future role command.
- Group `Server#getNodeCommandBindings` into per-concern modules with role tags.
- Make the ServiceManager start order explicit (dependency graph or phases) so a
  role's readiness is meaningful.
- Per-role readiness/liveness on `ServerStatus`/`Lifecycle` so orchestrators
  can act on them; `Lifecycle.FAILED` on one service is too coarse for a
  multi-role process.

---

## 6. Suggested sequencing

Each phase is shippable on its own and keeps `server` = all roles working.

| Phase | Outcome | Depends on | Risk |
|---|---|---|---|
| 0 | Roles exist in config, `nodes`, `ProxiedResource`; no behaviour change for default deployments | – | Low |
| 1 | `api` role can run standalone and stateless; search jobs in Mongo; legacy system jobs gone | 0 | Low |
| 2 | `worker` role; leader-only periodicals are jobs; scheduler runs on all workers; migrations explicit | 0 | Medium (touches rotation/retention correctness) |
| 3 | Broker-backed queue mode; `ingest` and `processing` roles run in separate processes; `processing_status` split | 0, 2 | High (data path) |
| 4 | Input placement spec + reconciler; certificates from store; ingest groups per input type | 3 | Medium |
| 5 | Optional: processing ⟂ indexing split, headless `api`, per-role commands | 3 | Medium |

Phases 1 and 2 deliver most of the "no leader obsession" and "scale search
independently" goals without touching the data path, which is why they come
before the broker.

---

## 7. Open questions to decide before phase 3

1. **Broker choice and on-prem story.** Is a Kafka-compatible dependency
   acceptable for on-prem, or must `disk` mode remain the default forever with
   the broker as an opt-in cloud mode?
2. **One binary vs. per-role commands.** §5.1 recommends one binary; confirm.
3. **Enterprise plugins and the Forwarder.** They are not in this tree. Which
   extension keys do they rely on, and does the Forwarder already implement a
   remote `MessageQueueWriter` (the `MessageQueueModule` comment suggests
   external journal modes exist)? If so, phase 3 may partly exist.
4. **Search jobs.** Is there already an enterprise `SearchJobService`
   override? If not, is Mongo-backed job state acceptable for latency?
5. **Message identity.** Assign `gl2_message_id` at ingest for idempotent
   replay, or keep assignment in processing?
6. **Data Node relationship.** Should `worker` own data-node provisioning and
   housekeeping (currently leader periodicals), or does the Data Node grow its
   own coordination?
7. **Compatibility floor.** Which existing REST endpoints that expose node-local
   state (`/cluster/{nodeId}/journal`, `/cluster/inputstates`, `/cluster/jobs`)
   must keep their shape for the UI and for customers' automation?

---

## Appendix: key files

| Topic | Files |
|---|---|
| Bootstrap | `bootstrap/CmdLineTool.java`, `bootstrap/ServerBootstrap.java`, `commands/Server.java`, `commands/AbstractNodeCommand.java`, `bindings/GraylogNodeModule.java`, `GraylogNodeConfiguration.java` |
| Services | `shared/bindings/GenericBindings.java`, `shared/bindings/GenericInitializerBindings.java`, `bindings/InitializerBindings.java`, `bindings/ServerBindings.java` |
| Queue / journal | `shared/messageq/*`, `shared/journal/LocalKafkaJournal.java`, `shared/buffers/InputBufferImpl.java`, `shared/buffers/JournallingMessageHandler.java`, `shared/buffers/ProcessBuffer.java`, `buffers/OutputBuffer.java` |
| Processing | `shared/buffers/processors/ProcessBufferProcessor.java`, `messageprocessors/OrderedMessageProcessors.java`, `buffers/processors/OutputBufferProcessor.java`, `outputs/BatchedMessageFilterOutput.java`, `outputs/ElasticSearchOutput.java`, `indexer/messages/Messages.java` |
| Processing status | `system/processing/MongoDBProcessingStatusRecorderService.java`, `system/processing/DBProcessingStatusService.java`, `org/graylog/events/processor/EventProcessorDependencyCheck.java` |
| Inputs | `inputs/InputServiceImpl.java`, `inputs/PersistedInputsImpl.java`, `inputs/InputEventListener.java`, `shared/inputs/InputLauncher.java`, `shared/initializers/InputSetupService.java`, `inputs/persistence/MongoInputStateService.java`, `periodical/ThrottleStateUpdaterThread.java` |
| Leader / jobs | `cluster/leader/*`, `cluster/lock/MongoLockService.java`, `shared/initializers/PeriodicalsService.java`, `bindings/PeriodicalBindings.java`, `org/graylog/scheduler/DefaultJobSchedulerConfig.java`, `org/graylog/scheduler/DBJobTriggerService.java`, `org/graylog/scheduler/capabilities/*`, `org/graylog/scheduler/system/SystemJobManager.java` |
| Cluster plumbing | `events/ClusterEventService.java`, `cluster/ClusterConfigServiceImpl.java`, `cluster/nodes/AbstractNodeService.java`, `periodical/NodePingThread.java` |
| REST fan-out | `shared/rest/resources/ProxiedResource.java`, `rest/RemoteInterfaceProvider.java`, `rest/resources/cluster/*`, `shared/security/RestrictToLeader*.java` |
| Search | `org/graylog/plugins/views/search/db/InMemorySearchJobService.java`, `org/graylog/plugins/views/search/jobs/SearchJobStateService.java`, `org/graylog/plugins/views/search/rest/remote/SearchJobsStatusResource.java`, `org/graylog/plugins/views/ViewsBindings.java` |
| Sessions | `security/sessions/MongoDbSessionService.java`, `bindings/providers/DefaultSecurityManagerProvider.java` |
