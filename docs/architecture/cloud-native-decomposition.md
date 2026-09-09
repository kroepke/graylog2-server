# Decomposing the Graylog server into independently scalable components

Status: exploration / discussion draft, revision 3. Revision 2 added the Kubernetes and
multi-tenant deployment model; revision 3 records the decision to run the control plane
on NATS and moves the job scheduler onto work queues. Nothing in this document is implemented.

This document records what the `server` process looks like today, the concrete
couplings that stop it from scaling by concern, the seams that already exist, and
a proposed target model with the architectural changes each step requires.
Every statement about current behaviour references the code it was derived from
(paths are relative to `graylog2-server/src/main/java/` unless noted). Sections
4 to 9 are design options, not facts about the codebase, except where they cite code.

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

## 4. Deployment context: Kubernetes, Graylog Cloud, MSP/MSSP tenants

The primary target is Kubernetes, for Graylog Cloud and for MSP/MSSP-style
providers running many tenants. The economic goal is that a tiny tenant costs
close to nothing at idle (scale to zero, or to one small pod) and that any
tenant can absorb a spike by scaling out on demand. Kafka or NATS is the
assumed transport between ingest and processing, and NATS carries the control plane.

This section is *proposal* except where it cites code.

### 4.1 Tenancy topology

The codebase assumes one MongoDB database, one cluster id and one plugin set per
JVM: `ClusterConfigService`, every `MongoCollection<T>` binding, the
`cluster_events` tail, `LookupTableService`, the pipeline interpreter state and
the stream router are all process singletons. Making the processing, API or
worker code tenant-aware inside one JVM would touch nearly every service and
cache. That rules out "one shared deployment of every role for all tenants" as
a near-term design.

The workable shape is therefore:

| Layer | Sharing | Rationale |
|---|---|---|
| Ingest gateway (§4.2) | **Shared per cell**, tenant-aware | The only role that must stay up for a tenant with listening inputs; the Forwarder precedent shows inputs can run without Mongo |
| NATS (control plane) | Shared per cell; one account per tenant | Locks, presence, events, job dispatch, node requests (§5) |
| Data-plane transport | Shared per cell where used; topic/stream per tenant; or the disk journal inside the all-roles pod | Buffers while a tenant's processing is scaled to zero (§5.3) |
| `processing`, `api`, `worker` | **Per tenant**, scale 0..N | Keeps today's single-database assumption; scale-to-zero removes the idle cost instead of a multi-tenant rewrite |
| MongoDB | Shared replica set / Atlas, database per tenant | Idle tenants hold zero connections once their pods are gone |
| OpenSearch / Data Node | Per tenant or pooled with index-level isolation | Out of scope here; the same choice exists today |
| Operator | Shared per cell | Owns tenant lifecycle, NATS accounts and buckets, scaling policies, gateway configuration |

A **tiny tenant** in this model is one pod running all roles (`server` with
no role restriction) at `minReplicas: 1`, or 0 with an activator, plus the
shared gateway. A **growing tenant** adds role-specific pods next to it:
extra `processing` pods join the same consumer group, extra `api` pods sit
behind the same service. Nothing has to be redeployed to move from one shape
to the other, which is the main argument for "one binary, roles in config".

### 4.2 A shared, tenant-aware ingest gateway

Listening inputs cannot scale to zero per tenant: something must own the
socket. The way out is a shared gateway per cell that accepts on behalf of all
tenants and writes into each tenant's topic. Evidence that the codebase can
support this without Mongo on the gateway side:

- `AWSModule`/`IntegrationsModule` already skip Mongo-dependent bindings under
  the `graylog.forwarder` property, and `MessageInput#isForwarderCompatible`
  marks input types that run without a server context.
- The journal payload (`RawMessage` protobuf) already carries the codec name,
  the filtered codec config and a source-node chain; decoding, extractors and
  static fields run on the processing side (`DecodingProcessor`,
  `ExtractorFilter`, `StaticFieldFilter`).
- `is_cloud` already forces global inputs and filters to cloud-compatible
  input types (`InputsResource`, `InputTypesResource`), and
  `CollectorsConfigResource` already assumes an `ingest-` hostname scheme.

What the gateway needs that does not exist yet:

- **Tenant identification per connection**: hostname/SNI per tenant for TLS
  protocols, client certificates (the collectors code already has
  `CertBindingResolver`), a token header for HTTP/OTLP, dedicated ports or
  source addresses for plain TCP/UDP. UDP syslog is the hard case; it only
  works with per-tenant ports or IPs.
- **Configuration from the control plane instead of Mongo**: which input
  types, ports, codecs and TLS material per tenant, pushed as a document the
  gateway reconciles. The `InputCreateRequest` shape and the `inputs`
  collection remain the tenant-facing model; the control plane projects them
  onto the gateway.
- **Tenant-scoped throttling**: today's `ThrottleState` comes from the local
  journal. The gateway needs per-tenant write quotas at the broker
  (both Kafka and NATS support them) and a per-tenant `THROTTLED` status.
- **Pull-based inputs** (AWS, HTTP poll, Kafka/AMQP consumers, cloud APIs)
  do not belong on the gateway. They become singleton jobs on the tenant's
  `worker`, which can scale to zero between polls.

Whether Graylog Cloud already runs something like this is outside the OSS
tree; the Forwarder hints suggest at least part of it exists.

### 4.3 Scaling signals per role

| Role | Scaler | Signal | Min replicas | Notes |
|---|---|---|---|---|
| Ingest gateway | HPA/KEDA | CPU, open connections, transport write latency | ≥ 2 per cell | Shared; never per tenant |
| `processing` | KEDA on the data-plane transport (Kafka lag, JetStream pending) | lag > 0, or oldest unacked message age > T for tiny tenants (batch the cold start) | 0 with a broker; 1 with the disk journal (§5.3) | Consumer group membership is the parallelism unit |
| `api` | KEDA HTTP add-on or Knative | in-flight requests | 0 | Cold start (§4.4) is the whole game; UI polling keeps a pod warm |
| `worker` | KEDA NATS JetStream scaler | pending messages on the tenant's job work queue | 0 | Wakes exactly when work is dispatched (§5.2); interval jobs on idle tenants are the remaining cost |
| Migrations / preflight | Job | tenant create / upgrade | n/a | Never on pod start |

The `worker` still needs a policy for idle tenants: rotation, retention and
event definitions are interval jobs, so a tenant with any alerting is woken
every minute. Options, in order of preference:

1. Skip evaluation when there is nothing to evaluate: the
   `AggregationEventProcessor` guard already reschedules when
   `hasMessagesIndexedUpTo` is false, and rotation/retention can be dispatched
   only when `processing_status` shows ingest since the last run. The
   dispatcher (§5.2) is the natural place for this, because it sees both the
   schedule and the tenant's activity.
2. Accept `minReplicas: 1` for tenants with alerting; the all-roles pod covers
   tiny tenants anyway.
3. Longer term, a shared multi-tenant worker that opens a tenant's database per
   job. Jobs already receive their context from the trigger, so this is the
   smallest role to make tenant-aware, but it still needs per-tenant injectors.

### 4.4 Scale-to-zero mechanics and what the code needs

- **Cold start is the primary engineering target for `api` and `processing`.**
  Today a `server` start runs preflight checks and migrations
  (`ServerBootstrap#beforeInjectorCreation`, `#startCommand`), builds a Guice
  injector from ~45 modules plus plugins, probes the indexer version
  (`elasticsearch_version_probe_*`), runs indexer discovery with retries
  (`IndexerDiscoveryProvider`), starts all lookup adapters and caches
  (`LookupTableService`), and only then flips to `RUNNING`. The startup
  timings are already logged (`serviceManager.startupTimes()` in
  `ServerBootstrap`); measure them per role before deciding on JVM-level help
  (AppCDS, CRaC, or a warm pool of pre-started pods per cell).
- **Preflight, migrations and CA bootstrap move to a Job.** They are leader
  gated today and run on every start; a scaled-from-zero pod must not run
  them. The `migrate` command exists; `skip_preflight_checks` and
  `run_migrations=false` are the interim switches.
- **Node identity.** `FilePersistedNodeIdProvider` generates a new id when no
  file exists, so an ephemeral pod is a new node every time. With the control
  plane on NATS (§5) most of the consequences disappear: presence is a KV entry
  with a TTL rather than a row in `nodes`, and a job lease is an unacked
  message rather than a lock bound to a node id, so a killed pod's work is
  redelivered after `AckWait` instead of waiting out a 5-minute lock expiry.
  Use the pod name as node id for log correlation; nothing else should depend
  on it.
- **Graceful scale-down of `processing`.** `BufferSynchronizerService` drains
  process/output buffers only if the indexer is healthy. With a transport
  that redelivers un-acked messages, the pod needs: stop pulling, flush the
  output batch, ack, exit, within `terminationGracePeriodSeconds`. With the
  disk journal the pod must not be scaled down while the journal is
  non-empty, which is why disk mode keeps `processing` at one replica.
- **Readiness per role.** `Lifecycle` is one state for the whole JVM and one
  failing service sets `FAILED` for everything. Each role needs its own
  readiness (gateway: sockets bound and transport reachable; processing:
  consumer joined; api: Jersey up and Mongo reachable; worker: NATS consumer
  bound).
- **No local state on scaled roles.** `content_packs_dir`, GeoIP files (an S3
  puller exists under `is_cloud`), input TLS files, support bundles on the
  leader's disk, `node_id_file`. In broker mode the journal directory goes
  too; in disk mode it is the one volume the all-roles pod keeps.
- **Metrics via Prometheus, not fan-out.** `PrometheusExporter` exists per
  node; `ClusterMetricsResource` and friends fanning out over `nodes` do not fit
  pods that come and go. Where the UI needs a per-node view, the fan-out
  becomes a NATS request over the tenant's subjects (§5.1).

## 5. Control plane on NATS

Decision: on Kubernetes, NATS is a required component of a cell, managed by the
Graylog operator, and it carries the **control plane** first. The data plane
(messages between ingest and processing) is a separate choice (§5.3) and may
stay on the local disk journal or go to Kafka. Control-plane traffic is small
(heartbeats, configuration change events, job dispatch at trigger rates), so
this does not put ingest volume on NATS.

### 5.1 What moves off "Mongo as a message bus"

Every mechanism below uses MongoDB today as a bus, a lock or a registry rather
than as a database. Each is what fights Kubernetes hardest, and each has a
direct NATS primitive. The entity data stays in Mongo.

| Today | Code | On NATS | Notes |
|---|---|---|---|
| Cluster events: capped `cluster_events` + tailable cursor | `events/ClusterEventService.java` | Core pub/sub on `events.<class>` in the tenant's account | Same fire-and-forget semantics as today (a node that missed events rebuilds from Mongo); no JetStream needed |
| Locks and leader election: `cluster_locks` with a TTL index | `cluster/lock/MongoLockService.java`, `cluster/leader/AutomaticLeaderElectionService.java` | KV bucket `locks`, create-if-absent with a per-key TTL, refreshed by the holder | Expiry becomes deterministic instead of "whenever the Mongo TTL monitor runs"; `LockService` is already an interface |
| Node registry and heartbeat: `nodes` + `NodePingThread` + `dropOutdated` | `cluster/nodes/AbstractNodeService.java`, `periodical/NodePingThread.java` | KV bucket `nodes`, key per node with TTL, value = role set, lifecycle, LB status, version | `NodeService#allActive(role)` is a KV scan; no `transport_address` needed |
| Node-to-node REST fan-out over `transport_address` | `shared/rest/resources/ProxiedResource.java`, `rest/RemoteInterfaceProvider.java`, the `Cluster*Resource`s | Request/reply scatter-gather on `node.<role>.<nodeId>.<op>` with the NATS services framework, timeout per request | Removes the need for every pod to be HTTP-reachable from the API pod; auth context travels as a header |
| Per-node processing status: `processing_status` document per node | `system/processing/MongoDBProcessingStatusRecorderService.java` | KV bucket `processing_status`, key per node with TTL | `DBProcessingStatusService#calculateProcessingState` reads the bucket; stale nodes expire instead of being filtered by `updated_at` |
| Job execution: polling, `findOneAndUpdate` claim, heartbeat, stale-lock steal, `cluster_locks` for concurrency | `org/graylog/scheduler/JobSchedulerService.java`, `JobExecutionEngine.java`, `DBJobTriggerService.java` | JetStream work-queue stream per tenant, pull consumers (§5.2) | The schedule definitions and trigger records stay in Mongo |
| Support bundles, content packs on the leader's disk | `SupportBundleService`, `ContentPackLoaderPeriodical` | Object store bucket per tenant | Optional |

Tenancy maps onto NATS accounts: one account per tenant with its own streams,
KV buckets and JetStream limits, credentials injected into the tenant's pods by
the operator. A cell is one NATS cluster.

### 5.2 Jobs on work queues

The split that keeps the UI and event-definition CRUD working:

- **Mongo remains the source of truth for schedules and job records.**
  `scheduler_job_definitions` and `scheduler_triggers` keep their shape:
  interval/cron/once schedules, `next_time`, status, per-trigger data (the
  event processor's catch-up window), constraints. Event-definition CRUD keeps
  rewriting triggers as it does now, and the UI keeps reading "next
  execution", "last run", "status" from them.
- **NATS carries execution intents.** A work-queue stream `jobs` per tenant
  with subjects `jobs.<pool>.<jobType>`; a pull consumer per pool (or per job
  type where a concurrency limit exists, using `MaxAckPending` as the limit).
  Message payload is the trigger id and job definition id, nothing else.
  `AckWait` is the lease; a worker sends in-progress acks while a job runs
  (replaces the 15-second heartbeat); redelivery after `AckWait` replaces the
  stale-lock steal; `MaxDeliver` caps retries.
- **A dispatcher turns due triggers into intents.** It reads
  `next_time <= now` from Mongo and publishes with
  `Nats-Msg-Id = <triggerId>:<nextTime>` so the dedup window guarantees one
  intent per period even if two dispatchers overlap. It is the one singleton
  left, elected through the `locks` bucket, and it is trivial enough to run
  inside every `worker` pod. If the message-scheduling feature in recent NATS
  server versions (2.12, as far as I recall; verify) proves solid, the
  dispatcher can be replaced by scheduled messages published at trigger save
  time; keep that as an optimisation, not a dependency.
- **Workers keep the `Job` API.** `Job`, `JobExecutionContext`,
  `JobTriggerUpdate` and the `addSchedulerJob`/`addSystemSchedulerJob`
  extension points do not change; event processors, notifications and system
  jobs are untouched. On completion the worker writes the `JobTriggerUpdate`
  to the trigger document as today, then acks. Cancel stays a flag on the
  trigger, re-read by `isCancelled()`.
- **Constraints become subjects.** `org.graylog.cluster.is-leader` and the
  `SchedulerCapabilities` mechanism map to subject filters; a worker pulls only
  the subjects for capabilities it has. Leader-only periodicals become
  interval job definitions dispatched on `jobs.system.singleton.*`.
- **Consistency rule.** Mongo and NATS are not updated transactionally, so the
  design must hold that every intent is idempotent and re-derivable from Mongo:
  a duplicate or lost intent is corrected by the next dispatcher pass.

### 5.3 Data-plane transport, deferred

The data plane is the only high-volume path and is decided separately. Three
configurations, all behind the existing `MessageQueueReader`/`Writer`/
`Acknowledger` SPI and `message_journal_mode`:

| Mode | Where it fits | Consequence |
|---|---|---|
| `disk` (today's `LocalKafkaJournal`) | Tiny tenants on one all-roles pod; on-prem | Ingest and processing stay in one pod; `processing` cannot scale out or to zero; one PVC per tenant |
| Kafka-compatible topic per tenant | Large tenants, or a platform that already runs Kafka | `processing` scales on lag to zero; offset commit needs a per-partition acked-range tracker because output completion is out of order (§2.2) |
| NATS JetStream stream per tenant | Uniform stack; moderate per-tenant rates | Per-message ack matches the pipeline directly; puts ingest volume on the same NATS cluster as the control plane, which needs separate sizing or a second cluster |

Recommendation: ship `disk` for the all-roles profile first, design the SPI to
the per-message-ack contract (bounded pull, ack per message, redelivery on
timeout) because Kafka can implement it with an acked-range tracker and the
reverse is not true, and choose Kafka vs JetStream for scaled tenants after a
spike on a synthetic fleet. The comparison that informs that spike:

| Concern | Kafka (incl. Redpanda/WarpStream) | NATS JetStream |
|---|---|---|
| Unit of parallelism | Partition; one active consumer per partition per group; partition count fixed upward only | Pull consumer; any number of workers pull from one consumer (`MaxAckPending` bounds in-flight); no partitions |
| Ack model | Offset commit per partition, monotone; acked-range tracker needed for out-of-order completion | Explicit per-message ack with redelivery after `AckWait` |
| Tenancy | Topic + ACL + quota per tenant; thousands of topics are routine on KRaft | Account per tenant; each replicated stream is a Raft group, so very large stream counts need a test |
| Buffer semantics (replaces `message_journal_max_size/age`) | Retention by size/time per topic; object-storage tiering in recent Kafka and in Redpanda/WarpStream | Limits retention by bytes/age/messages per stream; no built-in object-storage tier as far as I know |
| Dedup / idempotent ingest | Idempotent producer per session; no cross-session dedup | `Nats-Msg-Id` dedup within a window, useful if `gl2_message_id` moves to ingest |
| Max message size | ~1 MB default, configurable | 1 MB default, configurable upward with a hard ceiling |
| Scale-to-zero scaler | KEDA Kafka scaler on consumer lag | KEDA NATS JetStream scaler on pending messages |
| Throughput ceiling | Very high, scales with partitions | Adequate for moderate per-tenant rates; single-stream throughput is lower than a multi-partition topic |

## 6. Proposal: target component model

Split by scaling characteristic, not by package. Each component is a role; a
process can run one or several roles. `server` remains "all roles" so
single-node and existing multi-node deployments keep working, and so a tiny
tenant can be one pod.

| Role | Runs | External deps | Scaling model | Scale to zero? |
|---|---|---|---|---|
| Ingest gateway | Listening inputs for all tenants of a cell, transport writer | Sockets, data-plane transport, control plane | Horizontal on connections/CPU; shared | No, by design (shared) |
| `processing` | Transport reader, process buffer, message processors, output buffer, outputs incl. indexer | Transport, Mongo, NATS (control), OpenSearch, lookup sources | Horizontal on lag | Yes with a broker; one replica in `disk` mode |
| `api` | REST, web UI, sync search, export streaming | Mongo, NATS (control), OpenSearch | Horizontal on request load, stateless | Yes, cold-start bound |
| `worker` | Job consumers (event processors, notifications, index maintenance, former leader-only periodicals), the dispatcher, pull-based inputs as singleton jobs | Mongo, NATS, OpenSearch, SMTP/HTTP, external APIs | Horizontal on pending work | Yes, subject to the idle-tenant policy (§4.3) |
| `migrate` (Job) | Schema migrations, preflight, CA/cert bootstrap | Mongo, OpenSearch | Kubernetes Job on create/upgrade | n/a |

Cell-level shared components: the operator, NATS (control plane, one account
per tenant), the ingest gateway, MongoDB (database per tenant), the data-plane
broker where used, and OpenSearch/Data Node (per tenant or pooled; out of
scope here).

Design decisions embedded here, each open to a different call:

- **Processing and indexing stay together in the first cut.** The ack path and
  `post_indexing` watermark are the tightest coupling (§2.2). A later split
  needs a second queue and is not required for the scaling goals above.
- **The leader disappears as a concept.** Singleton work is a job on a
  work queue; the only election left is for the dispatcher, through a KV key.
- **Search async state moves to Mongo.** Without this the `api` role is sticky.
- **One binary, role list in config.** See §7.1 for the alternative.
- **Mongo holds entities and schedules; NATS holds ephemeral coordination.**
  Nothing in NATS is a source of truth.

## 7. What it takes: architectural changes in dependency order

### 7.1 Introduce node roles (prerequisite for everything)

Required:
- A role set per process, exposed through `NodeService#allActive(role)`
  (backed by the `nodes` KV bucket once §7.2 lands, by the `nodes` collection
  until then).
- Module composition by role: split `Server#getNodeCommandBindings` into
  role-tagged groups; extend `GraylogNodeConfiguration` (or a sibling) with the
  role set; plugin extension keys routed by role (a marker interface per role
  like `DatanodePlugin`, or a role annotation on `PluginModule`s).
- Replace ad-hoc gates (`is_cloud`, `graylog.forwarder`, `global_inputs_only`)
  with role checks where they are really role checks.
- Per-role readiness on `ServerStatus` (§4.4).

Options:
1. **One binary, `node_roles = ingest,processing,api,worker`** (recommended).
   `server` with no setting means all roles; a tiny tenant is one pod.
2. **One command per role** via `CliCommandsProvider`. Cleaner classpath per
   role, but multiplies bootstrap code that is already share-by-copy (§2.7).
   Can be layered on option 1 later.

### 7.2 Control plane on NATS

Required, in the order that keeps each step independently deployable:
- A NATS connection and account/credential binding in `GraylogNodeModule`,
  next to the Mongo connection; the operator provisions the account, the
  `jobs` stream and the `locks`, `nodes`, `processing_status` KV buckets.
- `LockService` on KV with per-key TTL; `LeaderElectionService` on top of it
  (until §7.3 removes most callers).
- `ClusterEventService` publishing and subscribing over core NATS; the capped
  collection and tailable cursor go away. Event classes and the
  `RestrictedChainingClassLoader` deserialization stay.
- Node presence in KV: `NodeService` implementation over the bucket;
  `NodePingThread` writes there; `dropOutdated` becomes TTL expiry.
- `ProcessingStatusRecorder` persisting to KV.
- Node-to-node calls over request/reply: a `NodeRequestService` replacing
  `RemoteInterfaceProvider`; each `Cluster*Resource` declares the role its
  target state lives on, and each node registers responders for the
  operations it can answer. Metrics move to Prometheus regardless.
- Keep the Mongo implementations behind the same interfaces only if a
  non-Kubernetes deployment without NATS must remain supported (§9).

### 7.3 Worker role on work queues

Required:
- The work-queue stream and consumers per §5.2; a `JobDispatcher` reading due
  triggers from Mongo and publishing intents with dedup ids; workers pulling,
  executing `Job`s, writing `JobTriggerUpdate`s, acking.
- Remove `JobSchedulerService`, `JobExecutionEngine`, the polling loop, the
  heartbeat and `forceReleaseOwnedTriggers`; keep `DBJobTriggerService` for
  the records, minus the claim query.
- Convert `leaderOnly()` periodicals into interval job definitions on
  singleton subjects. Candidates with current cadence: rotation (10 s),
  retention (5 m), field-type polling (1 s, also event-driven, needs a
  redesign), index-range cleanup, token cleaners, cert provisioning (2 s),
  data-node housekeeping (2 s), version check, telemetry, sidecar/collector
  purges.
- `@RestrictToLeader` endpoints: deflector cycle becomes a job submission;
  support bundles go to the object store.
- Migrations: `migrate` command as the only path; `server` verifies the schema
  version instead of running migrations.
- The idle-tenant policy in the dispatcher (§4.3).
- `InputEventListener#leaderChanged` for `onlyOnePerCluster()` inputs goes
  away with input placement (§7.5).

### 7.4 Make the `api` role stateless and fast to start

Required:
- Bind `SearchJobService` to a Mongo-backed implementation (extend
  `SearchJobStateService`); `SearchJobsStatusResource` becomes unnecessary.
- Retire `LegacySystemJobManager` (already `@Deprecated(since="7.1")`); move
  `FixDeflectorBy*Job` and `IndexSetCleanupJob` to the work queue.
- Lookup-table purge and logger-level changes become cluster events.
- `SimulatorResource` and `ExtractorsResource#test` need pipeline state and
  lookup tables: either `api` loads them too (Mongo only, cheap) or forwards
  the request to a `processing` pod over NATS.
- Preflight and migrations out of the start path; measure and cut cold start.

### 7.5 Data-plane transport and input placement

Required regardless of transport:
- Reader decoupled from `ProcessBuffer`: bounded-prefetch pull loop instead of
  "read exactly remaining capacity + semaphore"; `Acknowledgeable` ids become
  transport-native; acks per message after `BatchedMessageFilterOutput#flush`.
- Backpressure from lag and transport quotas instead of local journal
  offsets; per-tenant `THROTTLED` at the gateway.
- `processing_status` split into gateway/ingest facts and processing facts
  (in KV after §7.2); `calculateProcessingState` rewritten so the
  `AggregationEventProcessor` guard treats "lag > 0, processing at zero" as
  "not up to date", which is the correct answer.
- Message identity: decide whether `gl2_message_id` is assigned at ingest so
  redelivery is idempotent.
- Gateway role built from the forwarder-compatible subset of inputs, no Mongo,
  configuration pushed by the control plane over NATS (§4.2); tenant id and
  source-node chain stamped on every `RawMessage`.
- Placement spec on the input: `gateway` (listening, shared), `worker`
  singleton (pull-based), or legacy node pinning for `disk` mode. A reconciler
  replaces `PersistedInputsImpl#iterator` and the three placement predicates
  in `InputEventListener`.
- Certificates for TLS inputs from the control plane/secret store rather than
  node-local paths.

### 7.6 Bootstrap and composition hygiene

Not strictly required, but every step above is cheaper after it: a genuinely
shared node-runtime module set (used by `server`, `datanode`, gateway),
per-concern role-tagged modules instead of the 45-module block, explicit
ServiceManager start order, and per-role readiness.

## 8. Suggested sequencing

Each phase is shippable on its own and keeps `server` = all roles working.

| Phase | Outcome | Depends on | Risk |
|---|---|---|---|
| 0 | Roles in config and registry; per-role readiness; migrations as a Job; startup measured | – | Low |
| 1 | Control plane on NATS: locks, leader, cluster events, presence, processing status, node request/reply; Mongo bus code retired or kept behind interfaces | 0, operator provisions NATS | Medium (touches every node, but each piece has an interface today) |
| 2 | `worker` on work queues; dispatcher; leader-only periodicals as jobs; KEDA on pending messages | 1 | Medium (rotation/retention correctness) |
| 3 | `api` standalone, stateless, request-scaled; search jobs in Mongo; legacy jobs gone | 0, 1 | Low |
| 4 | Data-plane transport behind the SPI (Kafka or JetStream, chosen by spike); `processing` scales on lag to zero; `processing_status` split | 1, 2 | High (data path) |
| 5 | Shared ingest gateway; placement spec; certificates from the control plane | 4 | Medium |
| 6 | Operator: tenant CRD → NATS account, per-role deployments, KEDA objects, gateway config; tiny-tenant all-roles profile with `disk` mode | 0–3 for a first cut, 4–5 for scaled tenants | Medium |
| 7 | Optional: processing ⟂ indexing split, shared multi-tenant worker, per-role commands | 4 | Medium |

Phases 1 to 3 remove the leader, make background work and search scale to zero
and never touch the data path; a tiny tenant is fully served after phase 3
with `disk` mode. Phase 4 is where scaled tenants get elastic ingest.

## 9. Open questions

1. **Non-Kubernetes deployments.** Does on-prem without an operator remain a
   supported target for this architecture? If yes, either bundle `nats-server`
   the way the Data Node bundles OpenSearch (single binary, low footprint), or
   keep the Mongo implementations behind `LockService`, `NodeService`,
   `ClusterEventBus` and the scheduler interfaces. The first keeps one code
   path; the second doubles the surface permanently.
2. **Data-plane transport for scaled tenants.** Kafka vs JetStream, decided by
   a spike (§5.3). Does the platform already run Kafka, and what is the
   expected per-tenant rate distribution across a cell?
3. **NATS message scheduling.** Verify the feature and its update/cancel
   semantics before letting it replace the dispatcher.
4. **Tiny-tenant baseline.** Is "one all-roles pod at min 1 with `disk` mode"
   acceptable for the smallest tenants, or must true zero (request/lag
   activation, cold start in tens of seconds) be the target from the start?
5. **What Graylog Cloud does today** for ingest hostnames, the Forwarder, and
   any existing remote journal mode. These are outside the OSS tree and may
   already cover part of phases 4 and 5.
6. **Search jobs.** Is there an enterprise `SearchJobService` override, and is
   Mongo-backed job state acceptable for latency?
7. **Message identity.** Assign `gl2_message_id` at ingest for idempotent
   replay, or keep assignment in processing?
8. **Data Node.** Does `worker` own data-node provisioning and housekeeping,
   or does the Data Node grow its own coordination? Per-tenant OpenSearch vs
   pooled with index isolation is a separate decision with cost implications
   of the same order as this whole exercise.
9. **Compatibility floor.** Which node-local REST endpoints
   (`/cluster/{nodeId}/journal`, `/cluster/inputstates`, `/cluster/jobs`) must
   keep their shape for the UI and for customers' automation?

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
