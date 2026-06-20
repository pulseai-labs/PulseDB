//! Integration tests for agent activity tracking (E3-S03).
//!
//! Tests the full stack: PulseDB facade -> validation -> StorageEngine -> redb.
//! Covers activity register, heartbeat, end, get_active_agents, stale filtering,
//! collective isolation, validation error paths, and cascade deletes.

use std::time::Duration;

use pulsedb::{ActivityConfig, CollectiveId, Config, NewActivity, PulseDB};
use tempfile::tempdir;

/// Helper to open a fresh database with default config.
fn open_db() -> (PulseDB, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let db = PulseDB::open(&path, Config::default()).unwrap();
    (db, dir)
}

/// Helper to open a fresh database with a custom activity stale threshold.
fn open_db_with_threshold(threshold: Duration) -> (PulseDB, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let config = Config {
        activity: ActivityConfig {
            stale_threshold: threshold,
        },
        ..Default::default()
    };
    let db = PulseDB::open(&path, config).unwrap();
    (db, dir)
}

/// Helper: open DB, create a collective, return both.
fn open_db_with_collective() -> (PulseDB, CollectiveId, tempfile::TempDir) {
    let (db, dir) = open_db();
    let cid = db.create_collective("test-collective").unwrap();
    (db, cid, dir)
}

// ============================================================================
// Register + Get Roundtrip
// ============================================================================

#[test]
fn test_register_activity() {
    let (db, cid, _dir) = open_db_with_collective();

    db.register_activity(NewActivity {
        agent_id: "claude-opus".to_string(),
        collective_id: cid,
        current_task: Some("Reviewing code".to_string()),
        context_summary: Some("Working on src/db.rs".to_string()),
    })
    .unwrap();

    let agents = db.get_active_agents(cid).unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0].agent_id, "claude-opus");
    assert_eq!(agents[0].collective_id, cid);
    assert_eq!(agents[0].current_task.as_deref(), Some("Reviewing code"));
    assert_eq!(
        agents[0].context_summary.as_deref(),
        Some("Working on src/db.rs")
    );
    assert_eq!(agents[0].started_at, agents[0].last_heartbeat);
}

#[test]
fn test_register_activity_upsert() {
    let (db, cid, _dir) = open_db_with_collective();

    // First registration
    db.register_activity(NewActivity {
        agent_id: "agent-1".to_string(),
        collective_id: cid,
        current_task: Some("Task A".to_string()),
        context_summary: None,
    })
    .unwrap();

    // Second registration with same agent — should replace
    db.register_activity(NewActivity {
        agent_id: "agent-1".to_string(),
        collective_id: cid,
        current_task: Some("Task B".to_string()),
        context_summary: Some("New context".to_string()),
    })
    .unwrap();

    let agents = db.get_active_agents(cid).unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0].current_task.as_deref(), Some("Task B"));
    assert_eq!(agents[0].context_summary.as_deref(), Some("New context"));
}

// ============================================================================
// Heartbeat
// ============================================================================

#[test]
fn test_heartbeat_updates_timestamp() {
    let (db, cid, _dir) = open_db_with_collective();

    db.register_activity(NewActivity {
        agent_id: "agent-1".to_string(),
        collective_id: cid,
        current_task: None,
        context_summary: None,
    })
    .unwrap();

    let before = db.get_active_agents(cid).unwrap();
    let started_at = before[0].started_at;
    let heartbeat_before = before[0].last_heartbeat;

    // Small sleep to ensure timestamp changes
    std::thread::sleep(Duration::from_millis(10));

    db.update_heartbeat("agent-1", cid).unwrap();

    let after = db.get_active_agents(cid).unwrap();
    // started_at should NOT change
    assert_eq!(after[0].started_at, started_at);
    // last_heartbeat SHOULD have advanced
    assert!(after[0].last_heartbeat > heartbeat_before);
}

#[test]
fn test_heartbeat_nonexistent_activity() {
    let (db, cid, _dir) = open_db_with_collective();

    let result = db.update_heartbeat("no-such-agent", cid);
    assert!(result.is_err());
    assert!(result.unwrap_err().is_not_found());
}

// ============================================================================
// End Activity
// ============================================================================

#[test]
fn test_end_activity() {
    let (db, cid, _dir) = open_db_with_collective();

    db.register_activity(NewActivity {
        agent_id: "agent-1".to_string(),
        collective_id: cid,
        current_task: None,
        context_summary: None,
    })
    .unwrap();

    // Verify it exists
    assert_eq!(db.get_active_agents(cid).unwrap().len(), 1);

    // End it
    db.end_activity("agent-1", cid).unwrap();

    // Verify it's gone
    assert!(db.get_active_agents(cid).unwrap().is_empty());
}

#[test]
fn test_end_nonexistent_activity() {
    let (db, cid, _dir) = open_db_with_collective();

    let result = db.end_activity("no-such-agent", cid);
    assert!(result.is_err());
    assert!(result.unwrap_err().is_not_found());
}

// ============================================================================
// Get Active Agents
// ============================================================================

#[test]
fn test_get_active_agents() {
    let (db, cid, _dir) = open_db_with_collective();

    // Register multiple agents
    for name in &["agent-a", "agent-b", "agent-c"] {
        db.register_activity(NewActivity {
            agent_id: name.to_string(),
            collective_id: cid,
            current_task: None,
            context_summary: None,
        })
        .unwrap();
        // Small sleep so heartbeats differ
        std::thread::sleep(Duration::from_millis(5));
    }

    let agents = db.get_active_agents(cid).unwrap();
    assert_eq!(agents.len(), 3);

    // Should be sorted by last_heartbeat descending (most recent first)
    assert!(agents[0].last_heartbeat >= agents[1].last_heartbeat);
    assert!(agents[1].last_heartbeat >= agents[2].last_heartbeat);
}

#[test]
fn test_stale_activity_excluded() {
    // Stale threshold wide enough that the register→check step below can never
    // lose the timing race on a loaded CI runner (a 50ms threshold flaked: the
    // first get_active_agents could land >50ms after register and see 0). The
    // staleness check still holds because the sleep below exceeds the threshold.
    let (db, dir) = open_db_with_threshold(Duration::from_millis(1000));
    let cid = db.create_collective("test").unwrap();

    db.register_activity(NewActivity {
        agent_id: "stale-agent".to_string(),
        collective_id: cid,
        current_task: None,
        context_summary: None,
    })
    .unwrap();

    // Verify it's initially active
    assert_eq!(db.get_active_agents(cid).unwrap().len(), 1);

    // Wait for it to become stale (must exceed the 1000ms threshold above).
    std::thread::sleep(Duration::from_millis(1300));

    // Should be filtered out
    assert!(db.get_active_agents(cid).unwrap().is_empty());

    drop(dir); // keep dir alive until here
}

#[test]
fn test_one_activity_per_agent() {
    let (db, cid, _dir) = open_db_with_collective();

    // Register same agent twice
    db.register_activity(NewActivity {
        agent_id: "agent-1".to_string(),
        collective_id: cid,
        current_task: Some("First".to_string()),
        context_summary: None,
    })
    .unwrap();

    db.register_activity(NewActivity {
        agent_id: "agent-1".to_string(),
        collective_id: cid,
        current_task: Some("Second".to_string()),
        context_summary: None,
    })
    .unwrap();

    let agents = db.get_active_agents(cid).unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0].current_task.as_deref(), Some("Second"));
}

// ============================================================================
// Collective Isolation
// ============================================================================

#[test]
fn test_activity_collective_isolation() {
    let (db, _dir) = open_db();

    let cid_a = db.create_collective("collective-a").unwrap();
    let cid_b = db.create_collective("collective-b").unwrap();

    db.register_activity(NewActivity {
        agent_id: "agent-1".to_string(),
        collective_id: cid_a,
        current_task: Some("Working in A".to_string()),
        context_summary: None,
    })
    .unwrap();

    db.register_activity(NewActivity {
        agent_id: "agent-2".to_string(),
        collective_id: cid_b,
        current_task: Some("Working in B".to_string()),
        context_summary: None,
    })
    .unwrap();

    // Each collective should see only its own agent
    let agents_a = db.get_active_agents(cid_a).unwrap();
    assert_eq!(agents_a.len(), 1);
    assert_eq!(agents_a[0].agent_id, "agent-1");

    let agents_b = db.get_active_agents(cid_b).unwrap();
    assert_eq!(agents_b.len(), 1);
    assert_eq!(agents_b[0].agent_id, "agent-2");
}

// ============================================================================
// Validation Error Paths
// ============================================================================

#[test]
fn test_register_activity_invalid_collective() {
    let (db, _dir) = open_db();

    let result = db.register_activity(NewActivity {
        agent_id: "agent-1".to_string(),
        collective_id: CollectiveId::new(), // Doesn't exist
        current_task: None,
        context_summary: None,
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().is_not_found());
}

#[test]
fn test_register_activity_empty_agent_id() {
    let (db, cid, _dir) = open_db_with_collective();

    let result = db.register_activity(NewActivity {
        agent_id: String::new(),
        collective_id: cid,
        current_task: None,
        context_summary: None,
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().is_validation());
}

// ============================================================================
// Cascade Delete
// ============================================================================

#[test]
fn test_cascade_delete_collective_removes_activities() {
    let (db, cid, _dir) = open_db_with_collective();

    db.register_activity(NewActivity {
        agent_id: "agent-1".to_string(),
        collective_id: cid,
        current_task: Some("Working".to_string()),
        context_summary: None,
    })
    .unwrap();

    db.register_activity(NewActivity {
        agent_id: "agent-2".to_string(),
        collective_id: cid,
        current_task: None,
        context_summary: None,
    })
    .unwrap();

    // Verify activities exist
    assert_eq!(db.get_active_agents(cid).unwrap().len(), 2);

    // Delete the collective — activities should be cascade-deleted
    db.delete_collective(cid).unwrap();

    // Collective is gone, so get_active_agents should return NotFound
    let result = db.get_active_agents(cid);
    assert!(result.is_err());
    assert!(result.unwrap_err().is_not_found());
}
