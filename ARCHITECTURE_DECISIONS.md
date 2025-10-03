# Architecture Decisions & Evolution Strategy

## 📋 Executive Summary

This document consolidates all architectural decisions made for ReplicaDB's evolution from a CLI tool to a distributed, scalable API-first system. The strategy follows a **pragmatic 3-phase evolution** that starts simple and scales naturally without rewrites.

**Date**: October 3, 2025  
**Status**: Approved  
**Stakeholders**: Development Team

---

## 🎯 Core Decisions

### **Decision 1: No Separate Shared Library - Single Codebase**
**Status**: ✅ APPROVED

**Original Proposal**: Extract ReplicaDB core into separate `replicadb-core` library.

**Final Decision**: **Single Spring Boot codebase** with profile-based deployment slots.

**Rationale**:
- ✅ **Simpler maintenance**: One codebase, one build
- ✅ **Zero duplication**: No need to sync core logic between projects
- ✅ **Flexible deployment**: Same JAR deployed as API or Worker via profiles
- ✅ **CLI compatibility**: CLI mode uses optimized startup (lazy loading)

**Implementation**:
```
replicadb/
├── src/main/java/org/replicadb/
│   ├── ReplicaDbApplication.java    # Main with CLI detection
│   ├── cli/                         # CLI commands (existing)
│   ├── manager/                     # Database managers (existing)
│   ├── api/                         # NEW: REST controllers
│   ├── service/                     # NEW: Business logic
│   ├── worker/                      # NEW: Worker slot
│   └── config/                      # NEW: Profile configs
└── src/main/resources/
    └── application.yml               # Multi-profile configuration
```

**Deployment Slots**:
```yaml
# API Slot
SPRING_PROFILES_ACTIVE=api
# Enables: REST endpoints, Quartz scheduler, WebSocket monitoring

# Worker Slot  
SPRING_PROFILES_ACTIVE=worker
# Enables: Job execution, queue listening, ReplicaDB core

# CLI Mode
# Auto-detected: No Spring Boot startup if CLI args present
java -jar replicadb.jar --source... --sink...  # Fast CLI mode
```

---

### **Decision 2: Three-Phase Evolution Strategy**
**Status**: ✅ APPROVED

The evolution follows a **proven path** from monolithic to distributed architecture, with each phase delivering value while preparing for the next.

---

## 🚀 Phase 1: Monolithic Spring Boot API (CURRENT)

### **Overview**
Add REST API, scheduling, and web monitoring to existing ReplicaDB CLI without breaking backward compatibility.

### **Architecture**
```
┌─────────────────────────────────────────────┐
│     Single Spring Boot Application         │
├─────────────────────────────────────────────┤
│ REST API + WebSocket                        │
│ Quartz Scheduler                            │
│ In-Memory Job Queue                         │
│ ReplicaDB Core (direct integration)         │
│ SQLite Database (job metadata)              │
└─────────────────────────────────────────────┘
```

### **Key Features**
- ✅ REST API for job management (CRUD operations)
- ✅ Quartz scheduler for cron-based job execution
- ✅ Real-time monitoring via WebSocket
- ✅ Job history and audit trail
- ✅ **CLI remains fully functional** (optimized startup)

### **Job Execution Model**
```java
// Phase 1: Direct execution in API service
@Service
public class JobExecutionService {
    
    public void executeJob(JobDefinition job) {
        // Convert API job to ReplicaDB ToolOptions
        ToolOptions options = toToolOptions(job);
        
        // Execute directly using existing ReplicaDB code
        int result = ReplicaDB.processReplica(options);
        
        // Already uses threads internally with --jobs parameter
    }
}
```

### **Parallelism Strategy**
- **Model**: 1 Job = N Threads (ReplicaDB's current model)
- **Configuration**: `--jobs` parameter determines internal thread count
- **Typical**: `--jobs=4` creates 4 threads for parallel processing
- **No changes**: Uses ReplicaDB's existing partitioning strategies

### **CLI Optimization**
```java
public static void main(String[] args) {
    // Detect CLI mode vs API mode
    if (isCliMode(args)) {
        // Fast path: No Spring Boot startup
        runCliMode(args);
    } else {
        // API mode: Full Spring Boot
        SpringApplication.run(ReplicaDbApplication.class, args);
    }
}
```

### **Deployment**
```yaml
# Single container deployment
docker run -p 8080:8080 replicadb:latest

# Or traditional CLI
java -jar replicadb.jar --source... --sink...
```

### **Size Impact**
- **Current CLI**: 134 MB (182 KB core + 134 MB dependencies)
- **Phase 1 API**: ~209 MB (+75 MB Spring Boot overhead)
- **Memory**: 270-320 MB (similar to current CLI)

---

## 🔄 Phase 2: Kubernetes with Redis Queue

### **Overview**
Separate API service from worker execution using Redis message queue for job distribution and horizontal worker scaling.

### **Architecture**
```
┌──────────────────┐    ┌─────────────┐    ┌──────────────────┐
│  API Service     │───→│   Redis     │←───│ Worker Service   │
│  (REST + UI)     │    │   Queue     │    │ (Job Execution)  │
│  Scheduler       │    │             │    │                  │
└────────┬─────────┘    └─────────────┘    └──────────────────┘
         │                                           ↑
         │                                           │
    ┌────▼────┐                                     │
    │Database │                                     │
    │(SQLite) │─────────────────────────────────────┘
    └─────────┘           (Job metadata)
```

### **Key Changes from Phase 1**
- ✅ Redis queue for job distribution
- ✅ Separate API and Worker deployments
- ✅ Horizontal worker scaling via Kubernetes
- ✅ Same codebase, different deployment slots

### **Job Execution Model**
```java
// API Service: Submit jobs to queue
@Service
@Profile("api")
public class JobDispatchService {
    
    public void submitJob(JobDefinition job) {
        // Calculate optimal --jobs parameter
        int optimalJobs = calculateOptimalJobs(job);
        job.setJobs(optimalJobs);
        
        // Publish complete job to Redis
        redisTemplate.convertAndSend("replication-jobs", job);
    }
    
    private int calculateOptimalJobs(JobDefinition job) {
        long estimatedRows = estimateTotalRows(job);
        
        if (estimatedRows < 100_000) return 1;      // Small: no parallelism
        if (estimatedRows < 1_000_000) return 2;    // Medium: 2 threads
        if (estimatedRows < 10_000_000) return 4;   // Large: 4 threads
        return 6;                                    // XLarge: max 6 threads
    }
}

// Worker Service: Process jobs from queue
@Component
@Profile("worker")
public class WorkerJobExecutor {
    
    @RabbitListener(queues = "replication-jobs")
    public void processJob(JobDefinition job) {
        LOG.info("Worker processing job {} with {} threads",
            job.getId(), job.getJobs());
        
        // Execute with ReplicaDB core
        ToolOptions options = toToolOptions(job);
        int result = ReplicaDB.processReplica(options);
        
        publishJobCompletedEvent(job.getId(), result);
    }
}
```

### **Critical Decision: 1 Pod = 1 Complete Job**

**Why this model?**
- ✅ **Simplicity**: Uses ReplicaDB's existing partitioning logic
- ✅ **Performance**: ReplicaDB's internal threads are optimized per database
- ✅ **Compatibility**: No changes to core replication logic
- ✅ **Proven**: Current CLI model already works this way

**How ReplicaDB Partitions Internally:**

Each database has its own optimized strategy:

1. **Oracle**: `ora_hash(rowid, N-1) = threadId` - Hash-based distribution
2. **SQL Server**: `ABS(CHECKSUM(%%physloc%%)) % N = threadId` - Physical location hash
3. **PostgreSQL/MySQL**: `OFFSET threadId*chunkSize LIMIT chunkSize` - Calculated ranges
4. **MongoDB**: `skip(threadId*chunkSize).limit(chunkSize)` - Skip/limit with _id sort

**Example Execution:**
```
Job: 10M rows, --jobs=4

Worker Pod receives job → Launches ReplicaDB with --jobs=4
├─ Thread 0: Processes partition 0 (2.5M rows)
├─ Thread 1: Processes partition 1 (2.5M rows)
├─ Thread 2: Processes partition 2 (2.5M rows)
└─ Thread 3: Processes partition 3 (2.5M rows)

Result: All threads in same pod, job completes in ~15 minutes
```

### **Kubernetes Deployment**
```yaml
# api-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: replicadb-api
spec:
  replicas: 2  # High availability
  template:
    spec:
      containers:
      - name: replicadb
        image: replicadb:latest
        env:
        - name: SPRING_PROFILES_ACTIVE
          value: "api"
        - name: REDIS_URL
          value: "redis://redis-service:6379"
        resources:
          requests:
            cpu: "500m"
            memory: "512Mi"

---
# worker-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: replicadb-worker
spec:
  replicas: 5  # Scale based on load
  template:
    spec:
      containers:
      - name: replicadb
        image: replicadb:latest
        env:
        - name: SPRING_PROFILES_ACTIVE
          value: "worker"
        - name: REDIS_URL
          value: "redis://redis-service:6379"
        - name: REPLICA_JOBS_DEFAULT
          value: "4"  # Default threads per job
        resources:
          requests:
            cpu: "4000m"    # 4 CPUs for 4 threads
            memory: "4Gi"
          limits:
            cpu: "4000m"
            memory: "4Gi"
```

### **Scaling Strategy**
```yaml
# HPA based on Redis queue depth
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: replicadb-worker-scaler
spec:
  scaleTargetRef:
    name: replicadb-worker
  minReplicaCount: 2
  maxReplicaCount: 20
  triggers:
  - type: redis
    metadata:
      listName: replication-jobs
      listLength: "2"  # Scale up if more than 2 jobs queued
```

**Scaling Behavior:**
- 0-2 jobs in queue: 2 workers (minimum)
- 3-4 jobs: Scale to 4 workers
- 5-10 jobs: Scale to 10 workers
- 11-20 jobs: Scale to 20 workers (maximum)

### **Performance Characteristics**
```
Single Job (10M rows):
- 1 worker pod with --jobs=4
- Time: ~15 minutes
- CPU: 100% utilization (4 cores)

Multiple Jobs (5 jobs × 10M rows):
- 5 worker pods, each with --jobs=4
- Time: ~15 minutes (parallel)
- CPU: 100% utilization (20 cores total)
- Throughput: 3.3M rows/minute
```

---

## 🎯 Phase 3: Hybrid Chunking Model (ADVANCED)

### **Overview**
Introduce intelligent chunking at the API level for optimal resource utilization, while maintaining job-level execution for smaller workloads.

### **Architecture**
```
┌──────────────────┐    ┌─────────────┐    
│  API Service     │───→│   Redis     │    
│  + Smart         │    │   Queues    │    
│  Chunking        │    │             │    
│  Analyzer        │    │ ┌─────────┐ │
└──────────────────┘    │ │ jobs    │ │
                        │ │ chunks  │ │
                        │ └─────────┘ │
                        └──────┬──────┘
                               │
                    ┌──────────▼────────────┐
                    │   Worker Pool         │
                    ├───────────────────────┤
                    │ • Job Processor       │
                    │ • Chunk Processor     │
                    │ (Same worker, dual Q) │
                    └───────────────────────┘
```

### **Decision Logic**
```java
@Service
public class SmartJobDispatcher {
    
    public void submitJob(JobDefinition job) {
        // Analyze table characteristics
        TableAnalysis analysis = analyzeTable(job);
        
        // Decision: Chunking vs Complete Job
        if (shouldUseChunking(analysis)) {
            submitWithChunking(job, analysis);
        } else {
            submitCompleteJob(job);
        }
    }
    
    private boolean shouldUseChunking(TableAnalysis analysis) {
        // Chunking enabled IF:
        // 1. Table is large (>5M rows)
        // 2. Has efficient chunking strategy available
        
        if (analysis.getTotalRows() < 5_000_000) {
            return false; // Small/medium: use complete job
        }
        
        // Check if fast chunking strategy exists
        if (analysis.hasNumericPrimaryKey()) return true;  // PK range chunking
        if (analysis.getDatabaseType() == DatabaseType.ORACLE) return true;  // ora_hash
        if (analysis.getDatabaseType() == DatabaseType.SQLSERVER) return true;  // physloc
        
        return false; // No efficient strategy: use complete job
    }
}
```

### **Chunking Strategies by Database**

#### **1. PostgreSQL/MySQL with Numeric PK (OPTIMAL)**
```java
public class PKRangeChunkingStrategy {
    
    public List<JobChunk> createChunks(JobDefinition job) {
        // Query PK range
        long minPK = queryMin("SELECT MIN(id) FROM " + table);
        long maxPK = queryMax("SELECT MAX(id) FROM " + table);
        
        // Calculate chunk size (target: 500K-1M rows per chunk)
        int chunkSize = 500_000;
        
        List<JobChunk> chunks = new ArrayList<>();
        for (long start = minPK; start <= maxPK; start += chunkSize) {
            long end = Math.min(start + chunkSize - 1, maxPK);
            
            JobChunk chunk = new JobChunk();
            chunk.setSourceWhere("id BETWEEN " + start + " AND " + end);
            chunk.setJobs(1); // IMPORTANT: Single thread per chunk!
            
            chunks.add(chunk);
        }
        
        return chunks;
    }
}
```

**Generated SQL per worker:**
```sql
-- Worker 1
SELECT * FROM orders WHERE id BETWEEN 1 AND 500000;

-- Worker 2  
SELECT * FROM orders WHERE id BETWEEN 500001 AND 1000000;

-- Worker N
SELECT * FROM orders WHERE id BETWEEN 9500001 AND 10000000;
```

**Benefits:**
- ✅ Index-optimized queries (WHERE on PK)
- ✅ Perfect load balancing (equal chunk sizes)
- ✅ Fault tolerance (chunk failure doesn't affect others)
- ✅ Maximum parallelism (20 chunks = 20 workers possible)

#### **2. Oracle with ROWID Hash**
```java
public class OracleRowidHashChunkingStrategy {
    
    public List<JobChunk> createChunks(JobDefinition job, int targetChunks) {
        List<JobChunk> chunks = new ArrayList<>();
        
        for (int i = 0; i < targetChunks; i++) {
            JobChunk chunk = new JobChunk();
            chunk.setSourceWhere(
                "ora_hash(rowid, " + (targetChunks - 1) + ") = " + i
            );
            chunk.setJobs(1);
            chunks.add(chunk);
        }
        
        return chunks;
    }
}
```

**Benefits:**
- ✅ Uniform distribution (hash-based)
- ✅ No PK required
- ✅ Fast queries (physical ROWID)

#### **3. SQL Server with Physical Location**
```java
public class SqlServerPhyslocChunkingStrategy {
    
    public List<JobChunk> createChunks(JobDefinition job, int targetChunks) {
        List<JobChunk> chunks = new ArrayList<>();
        
        for (int i = 0; i < targetChunks; i++) {
            JobChunk chunk = new JobChunk();
            chunk.setSourceWhere(
                "ABS(CHECKSUM(%%physloc%%)) % " + targetChunks + " = " + i
            );
            chunk.setJobs(1);
            chunks.add(chunk);
        }
        
        return chunks;
    }
}
```

### **Worker Implementation**
```java
@Component
@Profile("worker")
public class HybridWorkerExecutor {
    
    // Queue 1: Complete jobs
    @RabbitListener(queues = "replication-jobs")
    public void processCompleteJob(JobDefinition job) {
        LOG.info("Processing complete job {} with {} threads",
            job.getId(), job.getJobs());
        
        ToolOptions options = toToolOptions(job);
        ReplicaDB.processReplica(options);
    }
    
    // Queue 2: Individual chunks
    @RabbitListener(queues = "replication-chunks")
    public void processChunk(JobChunk chunk) {
        LOG.info("Processing chunk {}/{} of job {}",
            chunk.getChunkNumber(),
            chunk.getTotalChunks(),
            chunk.getJobId());
        
        ToolOptions options = toToolOptions(chunk);
        options.setJobs(1); // ALWAYS 1 for chunks!
        
        ReplicaDB.processReplica(options);
        
        // Check if job complete
        checkJobCompletion(chunk.getJobId());
    }
}
```

### **Decision Matrix**

| Table Size | Has PK | Database | Strategy | Chunks | --jobs |
|------------|--------|----------|----------|--------|--------|
| <500K rows | Any | Any | Complete Job | 0 | 1 |
| 500K-5M | Any | Any | Complete Job | 0 | 2-4 |
| >5M | Numeric PK | PostgreSQL/MySQL | PK Range Chunking | 20+ | 1 |
| >5M | Any | Oracle | ROWID Hash Chunking | 20+ | 1 |
| >5M | Any | SQL Server | Physloc Chunking | 20+ | 1 |
| >5M | No PK/UUID | PostgreSQL/MySQL | Complete Job | 0 | 4-6 |

### **Performance Comparison**

**Scenario: 10M rows table**

#### **Phase 2 (Complete Job)**
```
Configuration:
- 1 worker pod
- --jobs=4 (4 threads)
- 4 CPUs

Time: 15 minutes
Throughput: 666K rows/min
```

#### **Phase 3 (Chunking)**
```
Configuration:
- 20 worker pods (auto-scaled)
- 20 chunks × 500K rows
- --jobs=1 per chunk
- 1 CPU per pod

Time: 10 minutes
Throughput: 1M rows/min
Improvement: 33% faster
```

### **Kubernetes Auto-Scaling**
```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: replicadb-worker-scaler-v2
spec:
  scaleTargetRef:
    name: replicadb-worker
  minReplicaCount: 2
  maxReplicaCount: 50  # Support up to 50 chunks in parallel
  triggers:
  # Scale by complete jobs
  - type: redis
    metadata:
      listName: replication-jobs
      listLength: "2"
  # Scale by chunks (higher priority)
  - type: redis
    metadata:
      listName: replication-chunks
      listLength: "5"  # 1 pod per 5 chunks
```

---

## 📊 Summary of Decisions

### **✅ Architecture: Single Codebase with Profiles**
- No separate core library
- Profile-based deployment (api/worker/cli)
- Same image, different configuration

### **✅ Phase 1: Monolithic Start**
- Single Spring Boot service
- Direct ReplicaDB core integration
- In-memory job queue
- CLI remains functional (optimized startup)

### **✅ Phase 2: Kubernetes with Redis**
- 1 pod = 1 complete job
- Job uses ReplicaDB's internal threading (--jobs=N)
- Redis queue for job distribution
- Horizontal worker scaling

### **✅ Phase 3: Hybrid Chunking**
- Smart chunking for large tables (>5M rows)
- Strategy selection based on table analysis
- Dual queue: jobs + chunks
- Maximum parallelism for optimal performance

---

## 🎯 Implementation Priorities

### **Priority 1: Phase 1 (IMMEDIATE)**
- [ ] Add REST API to existing codebase
- [ ] Implement profile-based configuration
- [ ] Add Quartz scheduler
- [ ] Create WebSocket monitoring
- [ ] Optimize CLI startup

### **Priority 2: Phase 2 (6-12 MONTHS)**
- [ ] Add Redis integration
- [ ] Implement job queue pattern
- [ ] Create K8s deployment manifests
- [ ] Add HPA configuration
- [ ] Performance testing

### **Priority 3: Phase 3 (12+ MONTHS)**
- [ ] Implement table analysis service
- [ ] Create chunking strategies
- [ ] Add chunk tracking and recovery
- [ ] Dual queue implementation
- [ ] Advanced performance optimization

---

## 📈 Success Metrics

### **Phase 1**
- ✅ API response time <500ms
- ✅ CLI startup time <2 seconds
- ✅ Zero breaking changes to CLI
- ✅ 100% backward compatibility

### **Phase 2**
- ✅ Support 10+ concurrent jobs
- ✅ Worker auto-scaling <30 seconds
- ✅ Zero job loss on worker failure
- ✅ 99% queue processing success

### **Phase 3**
- ✅ 30-50% performance improvement on large tables
- ✅ Support 50+ parallel chunks
- ✅ Smart strategy selection 95% accuracy
- ✅ Optimal resource utilization

---

## 🔒 Constraints & Limitations

### **Phase 1 Constraints**
- Single instance deployment only
- No horizontal scalability
- SQLite database limitations

### **Phase 2 Constraints**
- Job-level parallelism only (no chunking)
- Potential thread imbalance in Phase 2
- OFFSET/LIMIT inefficiency for some databases

### **Phase 3 Considerations**
- Chunking requires PK or database-specific features
- Increased complexity in job tracking
- More moving parts for monitoring

---

## 🚦 Migration Strategy

### **Phase 1 → Phase 2**
```bash
# 1. Add Redis to infrastructure
kubectl apply -f redis-deployment.yaml

# 2. Deploy API with Redis config
kubectl apply -f api-deployment.yaml

# 3. Deploy workers
kubectl apply -f worker-deployment.yaml

# 4. Migrate data (if needed)
# SQLite → PostgreSQL migration script
```

### **Phase 2 → Phase 3**
```bash
# 1. Deploy updated API with chunking logic
kubectl apply -f api-deployment-v2.yaml

# 2. Update worker to support dual queues
kubectl apply -f worker-deployment-v2.yaml

# 3. Update HPA for chunk scaling
kubectl apply -f worker-hpa-v2.yaml

# 4. Enable chunking flag
kubectl set env deployment/replicadb-api CHUNKING_ENABLED=true
```

---

## 📚 References

### **Internal Documentation**
- `implementation_plan.md` - Detailed implementation tasks
- `strategic_architecture_plan.md` - Original architecture analysis
- ReplicaDB Source Code Analysis - Partitioning strategies

### **External Resources**
- Spring Boot Profiles: https://spring.io/guides/gs/multi-module/
- KEDA Scaling: https://keda.sh/docs/
- ReplicaDB GitHub: https://github.com/osalvador/ReplicaDB

---

**Document Version**: 1.0  
**Last Updated**: October 3, 2025  
**Next Review**: After Phase 1 completion
