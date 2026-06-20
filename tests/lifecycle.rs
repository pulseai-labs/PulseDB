//! Integration tests for PulseDB database lifecycle operations.
//!
//! These tests verify the end-to-end behavior of:
//! - Opening new databases
//! - Opening existing databases
//! - Configuration validation
//! - Dimension mismatch detection
//! - Proper resource cleanup on close

use pulsedb::storage::SCHEMA_VERSION;
use pulsedb::{Config, EmbeddingDimension, PulseDB, PulseDBError, SyncMode, ValidationError};
use tempfile::tempdir;

// ============================================================================
// Database Creation Tests
// ============================================================================

#[test]
fn test_open_creates_new_database() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Database should not exist yet
    assert!(!path.exists(), "Database should not exist before open");

    // Open should create the database
    let db = PulseDB::open(&path, Config::default()).unwrap();

    // Database file should now exist
    assert!(path.exists(), "Database file should exist after open");

    // Clean up
    db.close().unwrap();
}

#[test]
fn test_open_with_default_config() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    let db = PulseDB::open(&path, Config::default()).unwrap();

    // Verify default configuration
    assert_eq!(db.embedding_dimension(), 384);
    assert_eq!(db.config().sync_mode, SyncMode::Normal);
    assert!(db.config().embedding_provider.is_external());

    db.close().unwrap();
}

#[test]
fn test_open_with_custom_dimension() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    let config = Config {
        embedding_dimension: EmbeddingDimension::D768,
        ..Default::default()
    };

    let db = PulseDB::open(&path, config).unwrap();

    assert_eq!(db.embedding_dimension(), 768);

    db.close().unwrap();
}

#[test]
fn test_open_with_custom_embedding_dimension() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    let config = Config {
        embedding_dimension: EmbeddingDimension::Custom(1536), // OpenAI ada-002
        ..Default::default()
    };

    let db = PulseDB::open(&path, config).unwrap();

    assert_eq!(db.embedding_dimension(), 1536);

    db.close().unwrap();
}

// ============================================================================
// Existing Database Tests
// ============================================================================

#[test]
fn test_open_existing_database() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Create database
    let db = PulseDB::open(&path, Config::default()).unwrap();
    db.close().unwrap();

    // Reopen - should succeed
    let db = PulseDB::open(&path, Config::default()).unwrap();
    assert_eq!(db.embedding_dimension(), 384);
    db.close().unwrap();
}

#[test]
fn test_metadata_preserved_across_opens() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Create database with specific settings
    let config = Config {
        embedding_dimension: EmbeddingDimension::Custom(512),
        ..Default::default()
    };

    let db = PulseDB::open(&path, config.clone()).unwrap();
    let created_at = db.metadata().created_at;
    db.close().unwrap();

    // Small delay to ensure timestamps differ
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Reopen
    let db = PulseDB::open(&path, config).unwrap();

    // Created at should be preserved
    assert_eq!(db.metadata().created_at, created_at);

    // Last opened should be updated
    assert!(db.metadata().last_opened_at > created_at);

    db.close().unwrap();
}

// ============================================================================
// Validation Tests
// ============================================================================

#[test]
fn test_invalid_config_cache_size_zero() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    let config = Config {
        cache_size_mb: 0, // Invalid
        ..Default::default()
    };

    let result = PulseDB::open(&path, config);
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert!(matches!(err, PulseDBError::Validation(_)));
}

#[test]
fn test_invalid_config_custom_dimension_zero() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    let config = Config {
        embedding_dimension: EmbeddingDimension::Custom(0), // Invalid
        ..Default::default()
    };

    let result = PulseDB::open(&path, config);
    assert!(result.is_err());
}

#[test]
fn test_invalid_config_custom_dimension_too_large() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    let config = Config {
        embedding_dimension: EmbeddingDimension::Custom(5000), // > 4096
        ..Default::default()
    };

    let result = PulseDB::open(&path, config);
    assert!(result.is_err());
}

// ============================================================================
// Dimension Mismatch Tests
// ============================================================================

#[test]
fn test_dimension_mismatch_returns_error() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Create with D384
    let db = PulseDB::open(
        &path,
        Config {
            embedding_dimension: EmbeddingDimension::D384,
            ..Default::default()
        },
    )
    .unwrap();
    db.close().unwrap();

    // Try to reopen with D768 - should fail
    let result = PulseDB::open(
        &path,
        Config {
            embedding_dimension: EmbeddingDimension::D768,
            ..Default::default()
        },
    );

    assert!(result.is_err());

    let err = result.unwrap_err();
    match err {
        PulseDBError::Validation(ValidationError::DimensionMismatch { expected, got }) => {
            // expected = what config wants (768)
            // got = what database has (384)
            assert_eq!(expected, 768);
            assert_eq!(got, 384);
        }
        other => panic!("Expected DimensionMismatch, got {:?}", other),
    }
}

#[test]
fn test_dimension_mismatch_custom_to_standard() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Create with Custom(512)
    let db = PulseDB::open(
        &path,
        Config {
            embedding_dimension: EmbeddingDimension::Custom(512),
            ..Default::default()
        },
    )
    .unwrap();
    db.close().unwrap();

    // Try to reopen with D384 - should fail
    let result = PulseDB::open(
        &path,
        Config {
            embedding_dimension: EmbeddingDimension::D384,
            ..Default::default()
        },
    );

    assert!(result.is_err());
}

// ============================================================================
// Close Behavior Tests
// ============================================================================

#[test]
fn test_close_flushes_data() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Create and close
    let db = PulseDB::open(&path, Config::default()).unwrap();
    db.close().unwrap();

    // Reopen and verify metadata was persisted
    let db = PulseDB::open(&path, Config::default()).unwrap();
    assert_eq!(db.metadata().schema_version, SCHEMA_VERSION);
    db.close().unwrap();
}

#[test]
fn test_multiple_open_close_cycles() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    for i in 0..5 {
        let db = PulseDB::open(&path, Config::default()).unwrap();
        assert_eq!(db.embedding_dimension(), 384, "Iteration {} failed", i);
        db.close().unwrap();
    }
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_error_is_validation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    let config = Config {
        cache_size_mb: 0,
        ..Default::default()
    };

    let err = PulseDB::open(&path, config).unwrap_err();
    assert!(err.is_validation());
    assert!(!err.is_not_found());
    assert!(!err.is_storage());
}

// ============================================================================
// Sync Mode Tests
// ============================================================================

#[test]
fn test_sync_mode_normal() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    let config = Config {
        sync_mode: SyncMode::Normal,
        ..Default::default()
    };

    let db = PulseDB::open(&path, config).unwrap();
    assert_eq!(db.config().sync_mode, SyncMode::Normal);
    db.close().unwrap();
}

#[test]
fn test_sync_mode_fast() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    let config = Config {
        sync_mode: SyncMode::Fast,
        ..Default::default()
    };

    let db = PulseDB::open(&path, config).unwrap();
    assert!(db.config().sync_mode.is_fast());
    db.close().unwrap();
}

#[test]
fn test_sync_mode_paranoid() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    let config = Config {
        sync_mode: SyncMode::Paranoid,
        ..Default::default()
    };

    let db = PulseDB::open(&path, config).unwrap();
    assert!(db.config().sync_mode.is_paranoid());
    db.close().unwrap();
}
