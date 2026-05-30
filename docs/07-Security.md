# PulseDB: Security Model

> **Version:** 1.0.0  
> **Status:** Approved  
> **Last Updated:** February 2026  
> **Owner:** PulseDB Team

---

## 1. Overview

This document defines the security model for PulseDB, including threat model, trust boundaries, data protection measures, and security testing strategy.

### 1.1 Security Philosophy

PulseDB follows the principle of **minimal attack surface**:
- Embedded library, no network exposure
- No authentication/authorization (consumer responsibility)
- Input validation and size limits
- Fail-safe defaults

### 1.2 Security Scope

| In Scope | Out of Scope |
|----------|--------------|
| Data integrity | Authentication |
| Collective isolation | Authorization |
| Input validation | Network security |
| Resource limits | Encryption at rest |
| Crash safety | Access control lists |

---

## 2. Threat Model

### 2.1 Trust Boundaries

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           TRUST BOUNDARIES                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    CONSUMER APPLICATION                              │    │
│  │                       (Trusted)                                      │    │
│  │                                                                      │    │
│  │  Responsibilities:                                                   │    │
│  │  • User authentication                                               │    │
│  │  • User authorization                                                │    │
│  │  • Input sanitization (content meaning)                              │    │
│  │  • Rate limiting                                                     │    │
│  │  • UserId generation and validation                                  │    │
│  │  • CollectiveId access control                                       │    │
│  │                                                                      │    │
│  └──────────────────────────────┬──────────────────────────────────────┘    │
│                                 │                                           │
│                          PulseDB API                                        │
│                                 │                                           │
│  ┌──────────────────────────────▼──────────────────────────────────────┐    │
│  │                         PULSEDB                                      │    │
│  │                      (Semi-Trusted)                                  │    │
│  │                                                                      │    │
│  │  Responsibilities:                                                   │    │
│  │  • Data integrity                                                    │    │
│  │  • Collective isolation (by ID)                                      │    │
│  │  • Input validation (format, size)                                   │    │
│  │  • Resource limits                                                   │    │
│  │  • Crash recovery                                                    │    │
│  │                                                                      │    │
│  └──────────────────────────────┬──────────────────────────────────────┘    │
│                                 │                                           │
│                          Storage Layer                                      │
│                                 │                                           │
│  ┌──────────────────────────────▼──────────────────────────────────────┐    │
│  │                      FILE SYSTEM                                     │    │
│  │                       (Untrusted)                                    │    │
│  │                                                                      │    │
│  │  Threats:                                                            │    │
│  │  • Unauthorized file access                                          │    │
│  │  • File tampering                                                    │    │
│  │  • Disk corruption                                                   │    │
│  │                                                                      │    │
│  │  Mitigations (OS/Consumer):                                          │    │
│  │  • File permissions                                                  │    │
│  │  • Disk encryption                                                   │    │
│  │  • Backups                                                           │    │
│  │                                                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Threat Categories

#### 2.2.1 Malformed Input (STRIDE: Tampering)

| Threat | Vector | Impact | Mitigation |
|--------|--------|--------|------------|
| Oversized content | Large content string | Memory exhaustion | Content size limit (100KB) |
| Invalid embedding | Wrong dimension vector | Search corruption | Dimension validation |
| Invalid UTF-8 | Malformed string | Crash/undefined behavior | UTF-8 validation |
| Negative values | importance < 0 | Logic errors | Range validation |
| Self-referential relation | source_id == target_id | Graph corruption | Validation |

#### 2.2.2 Resource Exhaustion (STRIDE: Denial of Service)

| Threat | Vector | Impact | Mitigation |
|--------|--------|--------|------------|
| Memory exhaustion | Many large experiences | OOM crash | Size limits, lazy loading |
| Disk exhaustion | Unlimited writes | Disk full | Configurable quotas |
| CPU exhaustion | Complex searches | Unresponsive | Query timeouts |
| File handle exhaustion | Many opens | System limit | Connection pooling |
| Lock starvation | Long write transactions | Read blocking | Transaction timeouts |

#### 2.2.3 Data Corruption (STRIDE: Tampering)

| Threat | Vector | Impact | Mitigation |
|--------|--------|--------|------------|
| Partial writes | Crash during write | Inconsistent state | Atomic transactions |
| Index/data mismatch | Crash after redb, before HNSW | Orphan vectors | Write ordering |
| Bit rot | Storage degradation | Silent corruption | Checksums |
| Concurrent writes | Race conditions | Data loss | Single-writer model |

#### 2.2.4 Information Disclosure (STRIDE: Information Disclosure)

| Threat | Vector | Impact | Mitigation |
|--------|--------|--------|------------|
| Cross-collective leak | CollectiveId guessing | Privacy violation | CollectiveId in all queries |
| Error message leak | Detailed errors | Path disclosure | Generic error messages |
| Memory disclosure | Buffer over-read | Data leak | Rust memory safety |
| Timing attacks | Search latency variance | Existence inference | N/A (accepted risk) |

### 2.3 Out-of-Scope Threats

These threats are explicitly NOT mitigated by PulseDB:

| Threat | Reason | Consumer Responsibility |
|--------|--------|------------------------|
| Unauthorized access | No auth layer | Implement access control |
| Network attacks | Embedded, no network | N/A or wrap in secure service |
| Encryption at rest | Performance/complexity | Use disk encryption |
| Privilege escalation | Same process | OS-level isolation |
| Malicious consumer | Library is trusted | N/A |

---

## 3. Security Controls

### 3.1 Input Validation

```rust
impl NewExperience {
    pub fn validate(&self) -> Result<(), ValidationError> {
        // Content validation
        if self.content.is_empty() {
            return Err(ValidationError::InvalidField {
                field: "content".into(),
                reason: "cannot be empty".into(),
            });
        }
        if self.content.len() > MAX_CONTENT_SIZE {
            return Err(ValidationError::ContentTooLarge {
                size: self.content.len(),
                max: MAX_CONTENT_SIZE,
            });
        }
        
        // Score validation
        if self.importance < 0.0 || self.importance > 1.0 {
            return Err(ValidationError::InvalidField {
                field: "importance".into(),
                reason: "must be between 0.0 and 1.0".into(),
            });
        }
        
        // Domain validation
        if self.domain.len() > MAX_DOMAIN_TAGS {
            return Err(ValidationError::InvalidField {
                field: "domain".into(),
                reason: format!("max {} tags allowed", MAX_DOMAIN_TAGS),
            });
        }
        for tag in &self.domain {
            if tag.len() > MAX_TAG_LENGTH {
                return Err(ValidationError::InvalidField {
                    field: "domain".into(),
                    reason: format!("tag too long: max {} chars", MAX_TAG_LENGTH),
                });
            }
        }
        
        Ok(())
    }
}

// Size limits
const MAX_CONTENT_SIZE: usize = 100 * 1024;      // 100 KB
const MAX_DOMAIN_TAGS: usize = 50;
const MAX_TAG_LENGTH: usize = 100;
const MAX_FILES: usize = 100;
const MAX_FILE_PATH_LENGTH: usize = 500;
const MAX_METADATA_SIZE: usize = 10 * 1024;      // 10 KB
```

### 3.2 Embedding Validation

```rust
impl PulseDB {
    fn validate_embedding(
        &self,
        collective_id: CollectiveId,
        embedding: &[f32],
    ) -> Result<(), ValidationError> {
        let collective = self.get_collective(collective_id)?
            .ok_or(ValidationError::CollectiveNotFound(collective_id))?;
        
        // Dimension check
        if embedding.len() != collective.embedding_dimension {
            return Err(ValidationError::DimensionMismatch {
                expected: collective.embedding_dimension,
                got: embedding.len(),
            });
        }
        
        // NaN/Inf check
        for (i, &v) in embedding.iter().enumerate() {
            if v.is_nan() || v.is_infinite() {
                return Err(ValidationError::InvalidField {
                    field: "embedding".into(),
                    reason: format!("invalid value at index {}: {}", i, v),
                });
            }
        }
        
        Ok(())
    }
}
```

### 3.3 Collective Isolation

```rust
impl PulseDB {
    // All queries MUST include collective_id
    pub fn search_similar(
        &self,
        collective_id: CollectiveId,  // Required, not optional
        query: &[f32],
        k: usize,
    ) -> Result<Vec<(Experience, f32)>> {
        // Validate collective exists
        self.validate_collective_exists(collective_id)?;
        
        // Search only within collective's HNSW index
        let index = self.get_hnsw_index(collective_id)?;
        let results = index.search(query, k)?;
        
        // Filter results to ensure collective isolation
        let experiences: Vec<_> = results
            .into_iter()
            .filter_map(|(id, score)| {
                let exp = self.get_experience(id).ok()??;
                // Double-check collective (defense in depth)
                if exp.collective_id == collective_id {
                    Some((exp, score))
                } else {
                    log::error!("Cross-collective leak prevented: {:?}", id);
                    None
                }
            })
            .collect();
        
        Ok(experiences)
    }
}
```

### 3.4 Resource Limits

```rust
#[derive(Clone, Debug)]
pub struct ResourceLimits {
    /// Maximum experiences per collective
    pub max_experiences_per_collective: Option<u64>,
    
    /// Maximum total storage bytes
    pub max_storage_bytes: Option<u64>,
    
    /// Maximum concurrent read transactions
    pub max_concurrent_reads: usize,
    
    /// Transaction timeout
    pub transaction_timeout: Duration,
    
    /// Query timeout
    pub query_timeout: Duration,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_experiences_per_collective: None,  // Unlimited by default
            max_storage_bytes: None,
            max_concurrent_reads: 100,
            transaction_timeout: Duration::from_secs(30),
            query_timeout: Duration::from_secs(10),
        }
    }
}

impl PulseDB {
    fn check_resource_limits(&self, collective_id: CollectiveId) -> Result<()> {
        if let Some(max) = self.limits.max_experiences_per_collective {
            let stats = self.get_collective_stats(collective_id)?;
            if stats.experience_count >= max {
                return Err(PulseDBError::ResourceLimit(
                    "Maximum experiences per collective exceeded".into()
                ));
            }
        }
        
        if let Some(max) = self.limits.max_storage_bytes {
            let total = self.get_total_storage_bytes()?;
            if total >= max {
                return Err(PulseDBError::ResourceLimit(
                    "Maximum storage limit exceeded".into()
                ));
            }
        }
        
        Ok(())
    }
}
```

### 3.5 Error Handling

```rust
impl PulseDBError {
    /// Convert to user-safe message (no internal details)
    pub fn user_message(&self) -> &'static str {
        match self {
            Self::Storage(_) => "A storage error occurred",
            Self::Validation(_) => "Invalid input provided",
            Self::NotFound(_) => "Resource not found",
            Self::Concurrency(_) => "Operation could not complete, please retry",
            Self::Embedding(_) => "Embedding generation failed",
            Self::ResourceLimit(_) => "Resource limit exceeded",
        }
    }
    
    /// Check if error should be logged with full details
    pub fn should_log_details(&self) -> bool {
        matches!(self, Self::Storage(_) | Self::Concurrency(_))
    }
}

// Safe error handling pattern
fn handle_api_error(error: PulseDBError) -> ApiResponse {
    if error.should_log_details() {
        log::error!("PulseDB error: {:?}", error);  // Full details to logs
    }
    
    ApiResponse::error(error.user_message())  // Safe message to user
}
```

---

## 4. Data Protection

### 4.1 Data at Rest

| Protection | Status | Notes |
|------------|--------|-------|
| Encryption | ❌ Not provided | Use OS disk encryption |
| Checksums | ✅ redb provides | Detect corruption |
| File permissions | ❌ Consumer responsibility | Set 0600 |

**Recommended Consumer Configuration:**
```bash
# Set restrictive permissions on database files
chmod 600 pulse.db
chmod 600 pulse.db.hnsw/*
chmod 700 pulse.db.hnsw/
```

### 4.2 Data in Memory

| Protection | Status | Notes |
|------------|--------|-------|
| Memory safety | ✅ Rust guarantees | No buffer overflows |
| Secure zeroing | ❌ Not provided | Use `zeroize` if needed |
| Core dumps | ❌ Consumer responsibility | Disable in production |

### 4.3 Data Deletion

```rust
impl PulseDB {
    /// Securely delete an experience (best-effort)
    pub fn secure_delete_experience(&self, id: ExperienceId) -> Result<()> {
        // 1. Remove from HNSW index
        self.hnsw.remove(id)?;
        
        // 2. Delete from storage
        self.storage.delete(id)?;
        
        // 3. Delete relations
        self.delete_relations_for_experience(id)?;
        
        // Note: Data may persist in:
        // - redb free pages (until compaction)
        // - HNSW tombstones (until rebuild)
        // - File system (until overwrite)
        // For true secure deletion, use encrypted storage
        // and destroy key
        
        Ok(())
    }
    
    /// Compact database to reclaim deleted space
    pub fn compact(&self) -> Result<()> {
        self.storage.compact()?;
        Ok(())
    }
}
```

---

## 5. Concurrency Security

### 5.1 Single-Writer Model

```rust
impl PulseDB {
    // Write lock prevents concurrent writes
    fn with_write_lock<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut WriteTxn) -> Result<R>,
    {
        // Acquire write lock with timeout
        let guard = self.write_lock
            .try_lock_for(self.limits.transaction_timeout)
            .ok_or(PulseDBError::Concurrency(
                ConcurrencyError::LockTimeout
            ))?;
        
        let mut txn = self.storage.begin_write()?;
        let result = f(&mut txn)?;
        txn.commit()?;
        
        drop(guard);
        Ok(result)
    }
}
```

### 5.2 Cross-Process Safety

```rust
// File-based lock for cross-process coordination
impl PulseDB {
    fn acquire_file_lock(path: &Path) -> Result<FileLock> {
        let lock_path = path.with_extension("db.lock");
        let file = File::create(&lock_path)?;
        
        // Try exclusive lock with timeout
        let start = Instant::now();
        loop {
            match file.try_lock_exclusive() {
                Ok(_) => return Ok(FileLock { file, path: lock_path }),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if start.elapsed() > Duration::from_secs(30) {
                        return Err(PulseDBError::Concurrency(
                            ConcurrencyError::FileLockTimeout
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}
```

---

## 6. Security Testing

### 6.1 Fuzz Testing

```rust
// fuzz/fuzz_targets/experience.rs
#![no_main]
use libfuzzer_sys::fuzz_target;
use arbitrary::Arbitrary;
use pulsedb::{PulseDB, NewExperience};

#[derive(Arbitrary, Debug)]
struct FuzzInput {
    content: String,
    importance: f32,
    confidence: f32,
    domain: Vec<String>,
    embedding: Vec<f32>,
}

fuzz_target!(|input: FuzzInput| {
    let db = setup_temp_db();
    let collective_id = db.create_collective("fuzz").unwrap();
    
    // Should never panic, always return Result
    let _ = db.record_experience(NewExperience {
        collective_id,
        content: input.content,
        importance: input.importance,
        confidence: input.confidence,
        domain: input.domain,
        embedding: Some(input.embedding),
        ..Default::default()
    });
});
```

**Fuzz Targets:**
1. `experience` - Experience creation with arbitrary input
2. `search` - Search with arbitrary embeddings
3. `relation` - Relation creation with arbitrary IDs
4. `deserialize` - Deserialize arbitrary bytes

### 6.2 Property-Based Testing

```rust
#[cfg(test)]
mod property_tests {
    use proptest::prelude::*;
    
    proptest! {
        #[test]
        fn experience_roundtrip(
            content in ".*",
            importance in 0.0f32..=1.0f32,
        ) {
            let db = setup_temp_db();
            let collective_id = db.create_collective("test")?;
            
            let id = db.record_experience(NewExperience {
                collective_id,
                content: content.clone(),
                importance,
                ..Default::default()
            })?;
            
            let retrieved = db.get_experience(id)?.unwrap();
            prop_assert_eq!(retrieved.content, content);
            prop_assert!((retrieved.importance - importance).abs() < 0.0001);
        }
        
        #[test]
        fn collective_isolation(
            content_a in ".*",
            content_b in ".*",
        ) {
            let db = setup_temp_db();
            let collective_a = db.create_collective("a")?;
            let collective_b = db.create_collective("b")?;
            
            db.record_experience(NewExperience {
                collective_id: collective_a,
                content: content_a.clone(),
                ..Default::default()
            })?;
            
            db.record_experience(NewExperience {
                collective_id: collective_b,
                content: content_b.clone(),
                ..Default::default()
            })?;
            
            // Search in collective_a should NOT return content_b
            let results = db.search_similar(collective_a, &random_embedding(), 100)?;
            for (exp, _) in results {
                prop_assert_eq!(exp.collective_id, collective_a);
            }
        }
    }
}
```

### 6.3 Security Test Cases

| Test ID | Description | Expected Result |
|---------|-------------|-----------------|
| SEC-001 | Content exceeding 100KB | `ValidationError::ContentTooLarge` |
| SEC-002 | Embedding dimension mismatch | `ValidationError::DimensionMismatch` |
| SEC-003 | NaN in embedding | `ValidationError::InvalidField` |
| SEC-004 | importance > 1.0 | `ValidationError::InvalidField` |
| SEC-005 | importance < 0.0 | `ValidationError::InvalidField` |
| SEC-006 | Self-referential relation | `ValidationError::InvalidField` |
| SEC-007 | Cross-collective relation | `ValidationError::InvalidField` |
| SEC-008 | Non-existent collective | `ValidationError::CollectiveNotFound` |
| SEC-009 | Non-existent experience | `NotFoundError` or `None` |
| SEC-010 | Empty content | `ValidationError::InvalidField` |
| SEC-011 | Concurrent write attempt | `ConcurrencyError::LockTimeout` |
| SEC-012 | Crash during write | Data consistent after restart |
| SEC-013 | Read during write | Read sees consistent snapshot |

---

## 7. Security Recommendations for Consumers

### 7.1 Deployment Checklist

```markdown
## PulseDB Security Deployment Checklist

### File System
- [ ] Database files have restrictive permissions (0600)
- [ ] Database directory has restrictive permissions (0700)
- [ ] Disk encryption enabled (e.g., LUKS, FileVault, BitLocker)
- [ ] Regular backups configured
- [ ] Backup encryption enabled

### Access Control
- [ ] Authentication implemented for API access
- [ ] Authorization checks before PulseDB operations
- [ ] UserId validated before passing to PulseDB
- [ ] CollectiveId access control implemented

### Monitoring
- [ ] Error logging configured (without sensitive data)
- [ ] Resource usage monitoring enabled
- [ ] Anomaly detection for unusual patterns

### Testing
- [ ] Input validation tested
- [ ] Authorization bypass tested
- [ ] Error handling tested
- [ ] Crash recovery tested
```

### 7.2 Secure Integration Pattern

```rust
// Example: Secure wrapper around PulseDB
pub struct SecurePulseDB {
    db: PulseDB,
    auth: AuthService,
}

impl SecurePulseDB {
    pub async fn record_experience(
        &self,
        user_token: &str,
        experience: NewExperience,
    ) -> Result<ExperienceId> {
        // 1. Authenticate user
        let user = self.auth.verify_token(user_token).await?;
        
        // 2. Authorize access to collective
        if !self.auth.can_write(user.id, experience.collective_id).await? {
            return Err(AuthError::Forbidden);
        }
        
        // 3. Sanitize input (application-specific)
        let sanitized = self.sanitize_experience(experience)?;
        
        // 4. Rate limit
        self.auth.check_rate_limit(user.id).await?;
        
        // 5. Record to PulseDB
        let id = self.db.record_experience(sanitized)?;
        
        // 6. Audit log
        self.audit_log(user.id, "record_experience", id).await;
        
        Ok(id)
    }
}
```

---

## 8. Security Non-Goals

These are explicitly NOT security goals for PulseDB v1:

| Non-Goal | Rationale |
|----------|-----------|
| Encryption at rest | Performance cost, OS solutions available |
| Authentication | Consumer responsibility, many valid approaches |
| Authorization | Consumer responsibility, application-specific |
| Audit logging | Consumer responsibility, application-specific |
| Network security | Embedded library, no network exposure |
| Multi-tenancy isolation | Collective isolation is logical, not security boundary |
| Tamper-evident logs | Not a security database |
| Secure multi-party computation | Out of scope |

---

## 9. References

- [OWASP Threat Modeling](https://owasp.org/www-community/Threat_Modeling)
- [STRIDE Threat Model](https://docs.microsoft.com/en-us/azure/security/develop/threat-modeling-tool-threats)
- [Rust Security Guidelines](https://rustsec.org/)

---

## Changelog

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0.0 | February 2026 | PulseDB Team | Initial security model |
