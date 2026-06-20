# PulseDB: Operations Guide

> **Version:** 1.0.0  
> **Status:** Approved  
> **Last Updated:** February 2026  
> **Owner:** PulseDB Team

---

## 1. Overview

This document provides operational guidance for deploying, monitoring, and maintaining PulseDB in production environments.

### 1.1 Deployment Model

PulseDB is an embedded library, not a standalone service:

```
┌─────────────────────────────────────────────────────────┐
│                   Your Application                       │
│  ┌──────────────────┐    ┌──────────────────────────┐   │
│  │   Your Code      │    │      PulseDB Library     │   │
│  │                  │───►│   (embedded in process)  │   │
│  └──────────────────┘    └───────────┬──────────────┘   │
│                                      │                   │
│                          ┌───────────┴───────────┐      │
│                          │                       │      │
│                    ┌─────▼─────┐           ┌─────▼────┐ │
│                    │ pulse.db  │           │ *.hnsw   │ │
│                    │ (redb)    │           │ (index)  │ │
│                    └───────────┘           └──────────┘ │
└─────────────────────────────────────────────────────────┘
```

**Implications:**
- No separate PulseDB server to manage
- Database lifecycle tied to your application
- File system access required
- Single process has write access

---

## 2. Installation

### 2.1 Add Dependency

```toml
# Cargo.toml
[dependencies]
pulsedb = "0.1"

# Without ONNX (smaller binary)
[dependencies]
pulsedb = { version = "0.1", default-features = false }
```

### 2.2 Binary Size

| Configuration | Size | Trade-off |
|---------------|------|-----------|
| Default (with ONNX) | ~18 MB | Full functionality |
| No default features | ~5 MB | External embeddings only |
| Release + LTO | -20% | Longer build time |

### 2.3 Build Configuration

```toml
# Cargo.toml - for production
[profile.release]
lto = true
codegen-units = 1
opt-level = 3
strip = true
```

---

## 3. Configuration

### 3.1 Basic Configuration

```rust
use pulsedb::{PulseDB, Config, EmbeddingProvider, EmbeddingDimension, SyncMode};

let config = Config {
    embedding_provider: EmbeddingProvider::Builtin { model_path: None },
    embedding_dimension: EmbeddingDimension::D384,
    cache_size_mb: 64,
    sync_mode: SyncMode::Normal,
    ..Default::default()
};

let db = PulseDB::open("./data/pulse.db", config)?;
```

### 3.2 Configuration Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `embedding_provider` | `EmbeddingProvider` | Builtin | Embedding source |
| `embedding_dimension` | `EmbeddingDimension` | D384 | Vector dimension |
| `cache_size_mb` | `usize` | 64 | redb cache size |
| `sync_mode` | `SyncMode` | Normal | Durability level |
| `watch_config.in_process` | `bool` | true | In-process watch |
| `watch_config.poll_interval_ms` | `u64` | 100 | Cross-process poll |
| `watch_config.buffer_size` | `usize` | 1000 | Watch buffer |

### 3.3 Environment-Specific Configuration

```rust
fn get_config() -> Config {
    let env = std::env::var("ENVIRONMENT").unwrap_or("development".into());
    
    match env.as_str() {
        "production" => Config {
            cache_size_mb: 256,
            sync_mode: SyncMode::Normal,  // Durability
            ..Default::default()
        },
        "staging" => Config {
            cache_size_mb: 128,
            sync_mode: SyncMode::Normal,
            ..Default::default()
        },
        _ => Config {
            cache_size_mb: 64,
            sync_mode: SyncMode::Fast,  // Speed for dev
            ..Default::default()
        },
    }
}
```

---

## 4. File System Layout

### 4.1 Database Files

```
/data/
├── pulse.db              # Main redb database
├── pulse.db.lock         # Process lock file
├── pulse.db.hnsw/        # HNSW index directory
│   ├── collective_abc123.hnsw      # Per-collective index
│   ├── collective_abc123.hnsw.meta # Index metadata
│   ├── collective_def456.hnsw
│   └── ...
└── backups/              # (User-managed)
    ├── pulse.db.20260301
    └── ...
```

### 4.2 Required Permissions

```bash
# Minimum permissions
chmod 600 pulse.db           # Owner read/write only
chmod 600 pulse.db.lock      # Owner read/write only
chmod 700 pulse.db.hnsw      # Owner all, dir access
chmod 600 pulse.db.hnsw/*    # Owner read/write only
```

### 4.3 Disk Space Requirements

| Experiences | redb | HNSW (384d) | Total |
|-------------|------|-------------|-------|
| 10K | ~8 MB | ~15 MB | ~25 MB |
| 100K | ~70 MB | ~150 MB | ~220 MB |
| 1M | ~650 MB | ~1.5 GB | ~2.2 GB |
| 10M | ~6.5 GB | ~15 GB | ~22 GB |

---

## 5. Monitoring

### 5.1 Metrics to Track

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| Database file size | File system | > 80% disk |
| Experience count | `get_collective_stats()` | Application-specific |
| Active agents | `get_active_agents()` | Stale detection |
| Operation latency | Application metrics | > 100ms |
| Error rate | Application logs | > 1% |

### 5.2 Health Check Implementation

```rust
impl HealthCheck for PulseDB {
    fn check_health(&self) -> HealthStatus {
        // Check database accessible
        let db_ok = self.list_collectives().is_ok();
        
        // Check disk space
        let stats = fs::metadata(&self.path).ok();
        let disk_ok = stats.map(|s| s.len() < MAX_SIZE).unwrap_or(false);
        
        // Check HNSW indices
        let hnsw_ok = self.check_hnsw_indices().is_ok();
        
        if db_ok && disk_ok && hnsw_ok {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unhealthy {
                db: db_ok,
                disk: disk_ok,
                hnsw: hnsw_ok,
            }
        }
    }
}
```

### 5.3 Logging Configuration

```rust
use tracing_subscriber::{fmt, EnvFilter};

// Initialize logging
fn init_logging() {
    fmt()
        .with_env_filter(EnvFilter::from_env("RUST_LOG"))
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .init();
}

// Log levels:
// RUST_LOG=pulsedb=info          # General info
// RUST_LOG=pulsedb=debug         # Detailed debug
// RUST_LOG=pulsedb::search=trace # Search operations only
```

### 5.4 Metrics Export

```rust
use prometheus::{Counter, Histogram, register_counter, register_histogram};

lazy_static! {
    static ref EXPERIENCE_WRITES: Counter = register_counter!(
        "pulsedb_experience_writes_total",
        "Total experience writes"
    ).unwrap();
    
    static ref SEARCH_LATENCY: Histogram = register_histogram!(
        "pulsedb_search_latency_seconds",
        "Search operation latency",
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
    ).unwrap();
}

impl PulseDB {
    pub fn record_experience_instrumented(&self, exp: NewExperience) -> Result<ExperienceId> {
        let result = self.record_experience(exp);
        if result.is_ok() {
            EXPERIENCE_WRITES.inc();
        }
        result
    }
    
    pub fn search_similar_instrumented(
        &self,
        collective_id: CollectiveId,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<(Experience, f32)>> {
        let timer = SEARCH_LATENCY.start_timer();
        let result = self.search_similar(collective_id, query, k);
        timer.observe_duration();
        result
    }
}
```

---

## 6. Backup & Recovery

### 6.1 Backup Strategy

```bash
#!/bin/bash
# backup.sh - Consistent backup script

PULSE_DB_PATH="/data/pulse.db"
BACKUP_DIR="/backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Stop writes (application-specific)
# Or use file locking approach

# Copy database files
cp "$PULSE_DB_PATH" "$BACKUP_DIR/pulse.db.$TIMESTAMP"
cp -r "${PULSE_DB_PATH}.hnsw" "$BACKUP_DIR/pulse.db.hnsw.$TIMESTAMP"

# Compress
tar -czf "$BACKUP_DIR/pulsedb_backup_$TIMESTAMP.tar.gz" \
    "$BACKUP_DIR/pulse.db.$TIMESTAMP" \
    "$BACKUP_DIR/pulse.db.hnsw.$TIMESTAMP"

# Cleanup temp files
rm -f "$BACKUP_DIR/pulse.db.$TIMESTAMP"
rm -rf "$BACKUP_DIR/pulse.db.hnsw.$TIMESTAMP"

# Retain last N backups
ls -t "$BACKUP_DIR"/pulsedb_backup_*.tar.gz | tail -n +8 | xargs -r rm

echo "Backup complete: $BACKUP_DIR/pulsedb_backup_$TIMESTAMP.tar.gz"
```

### 6.2 Restore Procedure

```bash
#!/bin/bash
# restore.sh - Restore from backup

BACKUP_FILE=$1
RESTORE_PATH="/data"

if [ -z "$BACKUP_FILE" ]; then
    echo "Usage: restore.sh <backup_file>"
    exit 1
fi

# Stop application
echo "Stop your application before continuing..."
read -p "Press enter to continue"

# Backup current (if exists)
if [ -f "$RESTORE_PATH/pulse.db" ]; then
    mv "$RESTORE_PATH/pulse.db" "$RESTORE_PATH/pulse.db.pre_restore"
    mv "$RESTORE_PATH/pulse.db.hnsw" "$RESTORE_PATH/pulse.db.hnsw.pre_restore"
fi

# Extract backup
tar -xzf "$BACKUP_FILE" -C "$RESTORE_PATH"

# Rename to standard names
mv "$RESTORE_PATH"/pulse.db.* "$RESTORE_PATH/pulse.db"
mv "$RESTORE_PATH"/pulse.db.hnsw.* "$RESTORE_PATH/pulse.db.hnsw"

echo "Restore complete. Start your application."
```

### 6.3 Point-in-Time Recovery

PulseDB does not have built-in point-in-time recovery. For critical applications:

1. **Frequent backups**: Increase backup frequency
2. **External WAL**: Implement application-level write-ahead log
3. **Replication**: Run multiple instances with coordinated writes

---

## 7. Maintenance

### 7.1 Routine Maintenance Tasks

| Task | Frequency | Command/Action |
|------|-----------|----------------|
| Backup | Daily | `backup.sh` |
| Disk space check | Hourly | Monitor metric |
| Log rotation | Daily | logrotate |
| Index stats | Weekly | `get_collective_stats()` |

### 7.2 Compaction

```rust
// redb handles compaction automatically, but you can force it:
impl PulseDB {
    /// Force compaction to reclaim deleted space
    pub fn compact(&self) -> Result<()> {
        // This is primarily useful after bulk deletes
        self.storage.compact()?;
        Ok(())
    }
}

// Usage:
db.compact()?;
```

### 7.3 HNSW Index Maintenance

```rust
impl PulseDB {
    /// Rebuild HNSW index (use after many deletes)
    pub fn rebuild_index(&self, collective_id: CollectiveId) -> Result<()> {
        // Rebuilding may improve search quality after many deletes
        let experiences = self.get_all_experiences(collective_id)?;
        
        // Delete old index
        self.hnsw.delete_index(collective_id)?;
        
        // Rebuild
        for exp in experiences {
            self.hnsw.add(exp.id, &exp.embedding)?;
        }
        
        Ok(())
    }
}
```

### 7.4 Database Upgrade

```rust
// Handle schema migrations on open
impl PulseDB {
    fn check_and_migrate(&self) -> Result<()> {
        let version = self.get_schema_version()?;
        
        match version {
            0 => {
                // Initial version, no migration needed
            }
            1 => {
                // Migrate from v1 to v2
                self.migrate_v1_to_v2()?;
            }
            _ => {
                return Err(PulseDBError::UnsupportedVersion(version));
            }
        }
        
        Ok(())
    }
}
```

---

## 8. Troubleshooting

### 8.1 Common Issues

#### Database Won't Open

| Symptom | Cause | Solution |
|---------|-------|----------|
| "Database locked" | Another process has lock | Close other process |
| "Permission denied" | File permissions | Check chmod 600 |
| "Corrupted" | Crash during write | Restore from backup |
| "Version mismatch" | Wrong PulseDB version | Update library |

```rust
// Diagnostic code
fn diagnose_open_failure(path: &Path) -> String {
    let mut issues = Vec::new();
    
    // Check file exists
    if !path.exists() {
        issues.push("Database file does not exist");
    }
    
    // Check permissions
    if let Ok(metadata) = fs::metadata(path) {
        if metadata.permissions().readonly() {
            issues.push("Database file is read-only");
        }
    }
    
    // Check lock file
    let lock_path = path.with_extension("db.lock");
    if lock_path.exists() {
        issues.push("Lock file exists - check for running processes");
    }
    
    issues.join(", ")
}
```

#### Slow Search Performance

| Symptom | Cause | Solution |
|---------|-------|----------|
| > 100ms latency | Large dataset | Tune HNSW parameters |
| Increasing latency | Many deletes | Rebuild index |
| High CPU | ef_search too high | Lower ef_search |

#### High Memory Usage

| Symptom | Cause | Solution |
|---------|-------|----------|
| OOM on start | HNSW loading all | Reduce M parameter |
| Growing memory | Memory leak | Check for unreleased streams |
| Large cache | cache_size_mb high | Reduce cache size |

### 8.2 Diagnostic Commands

```rust
// Get database diagnostics
pub fn diagnose(&self) -> Diagnostics {
    Diagnostics {
        database_size: self.get_database_size(),
        collective_count: self.list_collectives().map(|c| c.len()).unwrap_or(0),
        total_experiences: self.get_total_experience_count(),
        hnsw_index_count: self.hnsw.index_count(),
        cache_stats: self.storage.cache_stats(),
        last_write: self.get_last_write_time(),
    }
}

// Usage
let diag = db.diagnose();
println!("Database: {:?}", diag);
```

### 8.3 Recovery Procedures

#### Recover from Corruption

```bash
# 1. Stop application
# 2. Backup corrupted files (for analysis)
cp pulse.db pulse.db.corrupted
cp -r pulse.db.hnsw pulse.db.hnsw.corrupted

# 3. Restore from last good backup
./restore.sh /backups/pulsedb_backup_latest.tar.gz

# 4. Verify
cargo run --bin verify_db -- /data/pulse.db

# 5. Restart application
```

#### Recover from HNSW Index Corruption

```rust
// If HNSW index is corrupted but redb is fine
fn recover_hnsw_index(&self, collective_id: CollectiveId) -> Result<()> {
    // Delete corrupted index
    let index_path = self.hnsw_path(collective_id);
    if index_path.exists() {
        fs::remove_dir_all(&index_path)?;
    }
    
    // Rebuild from experiences
    self.rebuild_index(collective_id)?;
    
    Ok(())
}
```

---

## 9. Multi-Process Deployment

### 9.1 Single Writer Pattern

```
┌───────────────────────────────────────────────────────────────────┐
│                     MULTI-PROCESS DEPLOYMENT                       │
├───────────────────────────────────────────────────────────────────┤
│                                                                    │
│  Process A (Writer)                                                │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │  - Handles all writes                                       │   │
│  │  - record_experience, store_relation, etc.                  │   │
│  │  - Holds file lock during writes                            │   │
│  └────────────────────────────────────────────────────────────┘   │
│                              │                                     │
│                              ▼                                     │
│                       ┌──────────────┐                             │
│                       │  pulse.db    │                             │
│                       └──────────────┘                             │
│                              ▲                                     │
│                              │                                     │
│  ┌────────────────────┬──────┴──────┬────────────────────┐        │
│  │                    │             │                    │        │
│  ▼                    ▼             ▼                    ▼        │
│  Process B          Process C     Process D           Process E   │
│  (Reader)           (Reader)      (Reader)            (Reader)    │
│  ┌──────────┐      ┌──────────┐  ┌──────────┐       ┌──────────┐ │
│  │ search   │      │ search   │  │ search   │       │ search   │ │
│  │ only     │      │ only     │  │ only     │       │ only     │ │
│  └──────────┘      └──────────┘  └──────────┘       └──────────┘ │
│                                                                    │
└───────────────────────────────────────────────────────────────────┘
```

### 9.2 Writer Process Configuration

```rust
// Writer process
let config = Config {
    sync_mode: SyncMode::Normal,  // Durability
    ..Default::default()
};

let db = PulseDB::open_write("./pulse.db", config)?;
```

### 9.3 Reader Process Configuration

```rust
// Reader processes
let config = Config {
    watch_config: WatchConfig {
        in_process: false,
        poll_interval_ms: 100,  // Poll for changes
        ..Default::default()
    },
    ..Default::default()
};

let db = PulseDB::open_read_only("./pulse.db", config)?;
```

---

## 10. Capacity Planning

### 10.1 Resource Estimation

```
Memory = Base (50 MB) + HNSW (1.5 KB * experiences) + Cache (cache_size_mb)

Disk = redb (650 bytes * experiences) + HNSW (1.5 KB * experiences)

Example: 1M experiences
- Memory: 50 MB + 1.5 GB + 64 MB = ~1.6 GB
- Disk: 650 MB + 1.5 GB = ~2.2 GB
```

### 10.2 Scaling Limits

| Resource | Limit | Notes |
|----------|-------|-------|
| Experiences per collective | ~50M | HNSW memory limit |
| Collectives per database | ~1000 | File handle limit |
| Content size | 100 KB | Per experience |
| Embedding dimension | 4096 | Practical limit |
| Concurrent readers | ~100 | OS/hardware dependent |

### 10.3 When to Shard

Consider sharding when:
- Single collective > 10M experiences
- Total database > 50 GB
- Read latency > 100ms consistently
- Write throughput bottleneck

**Sharding Strategies:**
1. **By collective**: Each collective in separate database
2. **By time**: Archive old experiences to separate database
3. **By tenant**: Multi-tenant with tenant per database

---

## 11. References

- [03-Architecture.md](./03-Architecture.md) — System architecture
- [06-Performance.md](./06-Performance.md) — Performance targets
- [07-Security.md](./07-Security.md) — Security model
- [08-Testing.md](./08-Testing.md) — Testing strategy

---

## Changelog

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0.0 | February 2026 | PulseDB Team | Initial operations guide |
