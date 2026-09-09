# Decomposing the Graylog server into independently scalable components

Status: exploration / discussion draft, revision 2 (adds the Kubernetes and multi-tenant
deployment model and the broker comparison). Nothing in this document is implemented.

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
assumed transport between ingest and processing.

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
| Broker | Shared per cell; one topic/stream (or account) per tenant | Buffers while a tenant's processing is scaled to zero |
| `processing`, `api`, `worker` | **Per tenant**, scale 0..N | Keeps today's single-database assumption; scale-to-zero removes the idle cost instead of a multi-tenant rewrite |
| MongoDB | Shared replica set / Atlas, database per tenant | Idle tenants hold zero connections once their pods are gone |
| OpenSearch / Data Node | Per tenant or pooled with index-level isolation | Out of scope here; the same choice exists today |
| Control plane | Shared per cell (operator) | Owns tenant lifecycle, wake-up, scaling policies, gateway configuration |

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
| Ingest gateway | HPA/KEDA | CPU, open connections, broker write latency | ≥ 2 per cell | Shared; never per tenant |
| `processing` | KEDA Kafka lag / NATS JetStream pending | lag > 0, or oldest unacked message age > T for tiny tenants (batch the cold start) | 0 | Consumer group membership is the parallelism unit |
| `api` | KEDA HTTP add-on or Knative | in-flight requests | 0 | Cold start (§4.4) is the whole game; UI polling keeps a pod warm |
| `worker` | KEDA MongoDB scaler | count of `scheduler_triggers`/`scheduler_system_triggers` with `status=RUNNABLE` and `next_time <= now` | 0 in theory | Interval event definitions (every minute) keep it up permanently; see below |
| Migrations / preflight | Job | tenant create / upgrade | n/a | Never on pod start |

The `worker` is the awkward one. Rotation, retention and event definitions are
interval jobs, so a tenant with any alerting never idles. Options:

1. Accept `minReplicas: 1` for tenants with event definitions (the "at least
   one" the goal allows), with the all-roles pod covering it for tiny tenants.
2. Stretch intervals for idle tenants: rotation and retention every N minutes
   when `processing_status` shows no ingest; event definitions with
   `hasMessagesIndexedUpTo` false are already rescheduled (`AggregationEventProcessor`
   guard), so a tenant with no traffic could skip evaluation entirely.
3. Longer term, a shared multi-tenant worker that opens a tenant's database per
   trigger. It is the smallest role to make tenant-aware (jobs already receive
   their context from the trigger), but it still needs per-tenant injectors.

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
  file exists, so an ephemeral pod is a new node every time. Consequences in
  the code: `nodes` churn (`dropOutdated` handles it), `input_runtime_states`
  rows for dead nodes (cleaned by a leader-only periodical), and job triggers
  locked by a killed pod stay locked for `job_scheduler_lock_expiration_duration`
  (5 min) because `forceReleaseOwnedTriggers` only fires for the same node id.
  Use the pod name as node id, release triggers on SIGTERM, and lower the lock
  expiry.
- **Graceful scale-down of `processing`.** `JobSchedulerService#triggerShutdown`
  stops claiming and waits for running jobs; `BufferSynchronizerService` drains
  process/output buffers only if the indexer is healthy. With a broker,
  un-acked messages are simply redelivered, so the pod needs: stop pulling,
  flush the output batch, ack, exit, within `terminationGracePeriodSeconds`.
  That is simpler than today's journal drain, provided acks are per message
  (§5).
- **Readiness per role.** `Lifecycle` is one state for the whole JVM and one
  failing service sets `FAILED` for everything. Each role needs its own
  readiness (gateway: sockets bound and broker reachable; processing: consumer
  joined; api: Jersey up and Mongo reachable; worker: scheduler loop running).
- **No local state on scaled roles.** Journal (`message_journal_dir`),
  `content_packs_dir`, GeoIP files (an S3 puller exists under `is_cloud`),
  input TLS files, support bundles on the leader's disk, `node_id_file`. In
  broker mode none of these should require a volume.
- **Metrics via Prometheus, not fan-out.** `PrometheusExporter` exists per
  node; `ClusterMetricsResource` and friends fanning out over `nodes` do not fit
  pods that come and go. The UI's cluster pages need a metrics source that is
  not the REST fan-out.
- **Cluster events under churn.** `ClusterEventService` resumes from "now" on
  start; a pod scaled from zero rebuilds its caches from Mongo anyway, so this is
  fine, but the capped collection must be sized for the cell, not the tenant.

## 5. Broker choice: Kafka vs NATS JetStream

Both fit behind the existing `MessageQueueReader`/`Writer`/`Acknowledger` SPI.
The differences that matter for this deployment model:

| Concern | Kafka (incl. Redpanda/WarpStream) | NATS JetStream |
|---|---|---|
| Unit of parallelism | Partition; one active consumer per partition per group; partition count fixed upward only | Pull consumer; any number of workers pull from one consumer (`MaxAckPending` bounds in-flight); no partitions |
| Ack model | Offset commit per partition, monotone. With Graylog's parallel output buffer, completion is out of order, so a per-partition "contiguous acked" tracker is needed before committing, or the batch keeps today's high-water-mark semantics | Explicit per-message ack with redelivery after `AckWait`. Matches out-of-order completion directly and removes the §2.2 at-most-once edge |
| Tenancy | Topic + ACL + quota per tenant in one cluster; thousands of topics are routine on KRaft | Account per tenant with JetStream limits; stream per tenant. Each replicated stream is a Raft group, so very large stream counts need a test |
| Buffer semantics (replaces `message_journal_max_size/age`) | Retention by size/time per topic; object-storage tiering in recent Kafka and in Redpanda/WarpStream | Limits retention by bytes/age/messages per stream; no built-in object-storage tier as far as I know |
| Dedup / idempotent ingest | Idempotent producer per session; no cross-session dedup | `Nats-Msg-Id` header dedup within a window, useful if `gl2_message_id` moves to ingest |
| Max message size | ~1 MB default, configurable | 1 MB default, configurable upward with a hard ceiling |
| Scale-to-zero scaler | KEDA Kafka scaler on consumer lag | KEDA NATS JetStream scaler on pending messages |
| Ordering | Per partition | Per subject as published; not across parallel pullers |
| Kubernetes operations | Strimzi; KRaft removes ZooKeeper; heavier footprint; managed offerings everywhere | Helm chart / NACK operator; small footprint; managed offering exists |
| Throughput ceiling | Very high, scales with partitions; the safe choice for a few very large tenants | Adequate for moderate per-tenant rates; single-stream throughput is lower than a multi-partition topic |

Recommendation, as an opinion to be tested rather than a fact: for the "many
tiny tenants, scale to zero" objective, **NATS JetStream** fits better. Its
per-message ack matches the pipeline's out-of-order completion, accounts give
per-tenant isolation without an ACL layer, and idle streams are cheap. Kafka
wins when a few tenants dominate throughput or when the platform already runs
Kafka. The SPI should be designed to the JetStream-shaped contract (pull a
bounded batch, ack per message, redelivery on timeout) because it is
implementable on Kafka with an acked-range tracker, whereas the reverse is not
true. Prototype both behind the SPI on a synthetic tenant fleet before
committing; the existing `MessageQueueModule` switch on `message_journal_mode`
is the only integration point either needs.

## 6. Proposal: target component model

Split by scaling characteristic, not by package. Each component is a role; a
process can run one or several roles. `server` remains "all roles" so
single-node and existing multi-node deployments keep working, and so a tiny
tenant can be one pod.

| Role | Runs | External deps | Scaling model | Scale to zero? |
|---|---|---|---|---|
| Ingest gateway | Listening inputs for all tenants of a cell, queue writer | Sockets, broker, control plane | Horizontal on connections/CPU; shared | No, by design (shared) |
| `processing` | Queue reader, process buffer, message processors, output buffer, outputs incl. indexer | Broker, Mongo, OpenSearch, lookup sources | Horizontal on consumer lag | Yes |
| `api` | REST, web UI, sync search, export streaming | Mongo, OpenSearch | Horizontal on request load, stateless | Yes, cold-start bound |
| `worker` | User + system job schedulers (event processors, notifications, index maintenance), everything `leaderOnly()` today as jobs, pull-based inputs as singleton jobs | Mongo, OpenSearch, SMTP/HTTP, external APIs | Horizontal on due triggers; singleton jobs via lease | To one for tenants with alerting; zero otherwise |
| `migrate` (Job) | Schema migrations, preflight, CA/cert bootstrap | Mongo, OpenSearch | Kubernetes Job on create/upgrade | n/a |

Design decisions embedded here, each open to a different call:

- **Processing and indexing stay together in the first cut.** The ack path and
  `post_indexing` watermark are the tightest coupling (§2.2). A later split
  needs a second queue and is not required for the scaling goals above.
- **The leader disappears as a concept; singleton work becomes a job** claimed
  through the lease that `DBJobTriggerService` already implements.
- **Search async state moves to Mongo.** Without this the `api` role is sticky.
- **One binary, role list in config.** See §7.1 for the alternative.

## 7. What it takes: architectural changes in dependency order

### 7.1 Introduce node roles (prerequisite for everything)

Required:
- A `roles` set on `ServerNodeDto`/`nodes`, written by `NodePingThread`,
  exposed by `NodeService` (`allActive(role)`), and a `NodeRoles` service
  bound per process.
- `ProxiedResource` becomes role-aware: `requestOnAllNodes(role, …)`; each
  `Cluster*Resource` declares the role its target state lives on.
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

### 7.2 Make the `api` role stateless and fast to start

Required:
- Bind `SearchJobService` to a Mongo-backed implementation (extend
  `SearchJobStateService`); remove the need for `SearchJobsStatusResource`.
- Retire `LegacySystemJobManager` (already `@Deprecated(since="7.1")`); move
  `FixDeflectorBy*Job` and `IndexSetCleanupJob` to the system scheduler.
- Lookup-table purge and logger-level changes become cluster events; metrics
  move to Prometheus.
- `SimulatorResource` and `ExtractorsResource#test` need pipeline state and
  lookup tables: either `api` loads them too (Mongo only, cheap) or proxies to
  a `processing` pod.
- Preflight and migrations out of the start path; measure and cut cold start.

No broker involvement; independently shippable.

### 7.3 Dissolve the leader into the `worker` role

Required:
- `JobSchedulerConfig#canExecute` → "this node has role `worker`";
  `numberOfWorkerThreads` configurable; release owned triggers on SIGTERM.
- Convert `leaderOnly()` periodicals into scheduler jobs with an interval
  schedule and a singleton constraint via `constraints`/`SchedulerCapabilities`
  or `RefreshingLockService`. Candidates with current cadence: rotation (10 s),
  retention (5 m), field-type polling (1 s, also event-driven), index-range
  cleanup, token cleaners, cert provisioning (2 s), data-node housekeeping
  (2 s), version check, telemetry, sidecar/collector purges.
- `@RestrictToLeader` endpoints: deflector cycle becomes a system-job
  submission; support bundles go to shared storage.
- Migrations: `migrate` command as the only path; `server` verifies the schema
  version instead of running migrations.
- Replace the Mongo TTL-index lock expiry with an explicit heartbeat compare
  (the scheduler's `lock.last_lock_time` pattern) and lower
  `job_scheduler_lock_expiration_duration`.
- Idle-tenant behaviour for interval jobs (§4.3 option 2).

### 7.4 Broker-backed queue (ingest ⟂ processing)

Required regardless of broker:
- `MessageQueueWriter`/`Reader`/`Acknowledger` for the broker, bound through
  `bindMessageQueueImplementation`, selected by `message_journal_mode`.
- Reader decoupled from `ProcessBuffer`: bounded-prefetch pull loop instead of
  "read exactly remaining capacity + semaphore"; `Acknowledgeable` ids become
  broker-native; acks per message after `BatchedMessageFilterOutput#flush`
  (Kafka: acked-range tracker per partition before commit).
- Backpressure: `ThrottleState` from consumer lag and broker quotas; LB status
  for the gateway from broker write health, for `processing` from lag.
- `processing_status` split into gateway/ingest facts and processing facts;
  `DBProcessingStatusService#calculateProcessingState` rewritten so the
  `AggregationEventProcessor` guard keeps working with processing at zero
  (a tenant with lag > 0 is "not up to date", which is the correct answer).
- Message identity: decide whether `gl2_message_id` is assigned at ingest so
  redelivery is idempotent (JetStream dedup header can then be used).
- Keep `disk` mode for single-node and on-prem: `ingest,processing` on one
  node stays supported.

### 7.5 Shared ingest gateway and input placement

Required:
- Gateway role built from the forwarder-compatible subset of inputs, no Mongo,
  configuration pushed by the control plane (§4.2); tenant id and source-node
  chain stamped on every `RawMessage`; per-tenant topic/stream routing.
- Replace `global`/`node_id` with a placement spec on the input: `gateway`
  (listening, shared), `worker` singleton (pull-based), or legacy node
  pinning for on-prem. A reconciler replaces `PersistedInputsImpl#iterator`
  and the three placement predicates in `InputEventListener`.
- Certificates for TLS inputs from the control plane/secret store rather than
  node-local paths.
- Input runtime state for gateway-hosted inputs reported per tenant into
  `input_runtime_states` (already keyed by input and node).

### 7.6 Bootstrap and composition hygiene

Not strictly required, but every step above is cheaper after it: a genuinely
shared node-runtime module set (used by `server`, `datanode`, gateway), per-concern
role-tagged modules instead of the 45-module block, explicit ServiceManager
start order, and per-role readiness.

## 8. Suggested sequencing

Each phase is shippable on its own and keeps `server` = all roles working.

| Phase | Outcome | Depends on | Risk |
|---|---|---|---|
| 0 | Roles in config, `nodes`, `ProxiedResource`; per-role readiness; migrations as a Job; startup measured | – | Low |
| 1 | `api` runs standalone, stateless, scaled by requests; search jobs in Mongo; legacy jobs gone | 0 | Low |
| 2 | `worker` role; leader-only periodicals are jobs; scheduler on all workers; triggers released on SIGTERM; idle-tenant cadence | 0 | Medium |
| 3 | Broker SPI implementation (JetStream first, Kafka behind the same contract); `processing` scales on lag to zero; `processing_status` split | 0, 2 | High (data path) |
| 4 | Shared ingest gateway; placement spec; certificates from the control plane | 3 | Medium |
| 5 | Operator: tenant CRD → per-role deployments, KEDA objects, gateway config; tiny-tenant all-roles profile | 0–4, deployment side | Medium |
| 6 | Optional: processing ⟂ indexing split, shared multi-tenant worker, per-role commands | 3 | Medium |

Phases 1 and 2 remove the leader and make search scale without touching the
data path; phase 3 is where scale-to-zero for the data path arrives; phase 4
is what makes tiny tenants cheap.

## 9. Open questions

1. **Tenancy topology.** Confirm stack-per-tenant for `processing`/`api`/`worker`
   with a shared gateway and broker per cell, as opposed to multi-tenant
   components. The latter is a rewrite of the singleton assumptions.
2. **Broker.** JetStream-shaped SPI, prototype both; pick after a spike. Does
   the platform already run Kafka, and how many tenants per cell are expected?
3. **Tiny-tenant baseline.** Is "one all-roles pod at min 1" acceptable, or
   must true zero (with request/lag activation and a cold start measured in
   tens of seconds) be the target from the start?
4. **What Graylog Cloud does today** for ingest hostnames, the Forwarder, and
   any existing remote journal mode. These are outside the OSS tree and may
   already cover part of phases 3 and 4.
5. **Node identity for pods**: pod name as node id, and how much of the
   `nodes`/`input_runtime_states`/trigger-lock cleanup to keep.
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
