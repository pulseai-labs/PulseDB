//! Real-time watch system for experience change notifications.
//!
//! The watch module provides an in-process pub-sub mechanism that lets
//! consumers subscribe to experience mutations (create, update, archive,
//! delete) within a collective. Events are delivered via bounded crossbeam
//! channels with a [`futures_core::Stream`] adapter for async consumption.
//!
//! # Architecture
//!
//! ```text
//! record_experience() ──┐
//! update_experience() ──┤
//! delete_experience() ──┼── WatchService::emit() ──► [crossbeam channel] ──► WatchStream
//! archive_experience()──┤                           (per subscriber)
//! reinforce_experience()┘
//! ```
//!
//! Filters are applied on the sender side before channel delivery, so
//! subscribers only receive events they care about.

pub mod lock;
pub mod poll;
pub mod types;

pub use lock::WatchLock;
pub use poll::ChangePoller;
pub use types::{WatchEvent, WatchEventType, WatchFilter, WatchStream};

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use atomic_waker::AtomicWaker;
use crossbeam_channel::{bounded, Sender, TrySendError};
use tracing::{info, warn};

use crate::error::PulseDBError;
use crate::experience::Experience;
use crate::types::CollectiveId;

/// A subscriber's channel sender and optional filter.
struct Subscriber {
    sender: Sender<WatchEvent>,
    waker: Arc<AtomicWaker>,
    filter: Option<WatchFilter>,
}

/// Internal service managing watch subscriptions and event dispatch.
///
/// Held as `Arc<WatchService>` in PulseDB so that [`WatchStream`] drop
/// handlers can call back to remove their subscriber.
pub(crate) struct WatchService {
    /// Subscribers grouped by collective.
    ///
    /// Read lock for emit (common path), write lock for subscribe/unsubscribe (rare).
    subscribers: RwLock<HashMap<CollectiveId, Vec<(u64, Subscriber)>>>,

    /// Monotonic counter for subscriber IDs.
    next_id: AtomicU64,

    /// Channel buffer capacity for new subscriptions.
    buffer_size: usize,

    /// Whether in-process event dispatch is enabled.
    in_process: bool,
}

impl WatchService {
    /// Creates a new watch service with the given channel buffer size.
    ///
    /// When `in_process` is `false`, event dispatch via `emit()` is skipped
    /// and `has_subscribers()` always returns `false`.
    pub(crate) fn new(buffer_size: usize, in_process: bool) -> Self {
        Self {
            subscribers: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(0),
            buffer_size,
            in_process,
        }
    }

    /// Registers a new subscriber for a collective.
    ///
    /// Returns a [`WatchStream`] that yields events matching the optional filter.
    /// The stream automatically unregisters on drop.
    pub(crate) fn subscribe(
        self: &Arc<Self>,
        collective_id: CollectiveId,
        filter: Option<WatchFilter>,
    ) -> crate::Result<WatchStream> {
        if !self.in_process {
            info!("in-process watch disabled, stream will not receive events");
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = bounded(self.buffer_size);
        let waker = Arc::new(AtomicWaker::new());

        let subscriber = Subscriber {
            sender,
            waker: Arc::clone(&waker),
            filter,
        };

        // Write lock: only held briefly during registration
        {
            let mut subs = self
                .subscribers
                .write()
                .map_err(|_| PulseDBError::watch("Watch subscribers lock poisoned"))?;
            subs.entry(collective_id)
                .or_default()
                .push((id, subscriber));
        }

        // Capture weak reference for cleanup to avoid preventing WatchService drop
        let service = Arc::downgrade(self);
        let cleanup_collective = collective_id;

        Ok(WatchStream {
            receiver,
            waker,
            cleanup: Some(Box::new(move || {
                if let Some(service) = service.upgrade() {
                    service.remove_subscriber(cleanup_collective, id);
                }
            })),
        })
    }

    /// Emits an event to all matching subscribers for the event's collective.
    ///
    /// Filters are applied on the sender side. Disconnected subscribers are
    /// cleaned up automatically. Full channels cause the event to be dropped
    /// for that subscriber (logged as a warning).
    ///
    /// The `experience` parameter is used for filter matching (domain, type,
    /// importance) without including it in the event payload.
    pub(crate) fn emit(&self, event: WatchEvent, experience: &Experience) -> crate::Result<()> {
        if !self.in_process {
            return Ok(());
        }

        let collective_id = event.collective_id;
        let mut disconnected = Vec::new();

        // Read lock: multiple concurrent emitters are fine
        {
            let subs = self
                .subscribers
                .read()
                .map_err(|_| PulseDBError::watch("Watch subscribers lock poisoned"))?;

            if let Some(subscribers) = subs.get(&collective_id) {
                for (id, sub) in subscribers {
                    // Apply filter before sending
                    if !matches_filter(&sub.filter, experience) {
                        continue;
                    }

                    match sub.sender.try_send(event.clone()) {
                        Ok(()) => {
                            // Wake the async poller so it picks up the event
                            sub.waker.wake();
                        }
                        Err(TrySendError::Full(_)) => {
                            warn!(
                                subscriber_id = id,
                                collective_id = %collective_id,
                                "Watch buffer full, dropping event"
                            );
                            // Still wake in case consumer is stuck
                            sub.waker.wake();
                        }
                        Err(TrySendError::Disconnected(_)) => {
                            disconnected.push(*id);
                        }
                    }
                }
            }
        }

        // Clean up disconnected subscribers (write lock, rare path)
        if !disconnected.is_empty() {
            let mut subs = self
                .subscribers
                .write()
                .map_err(|_| PulseDBError::watch("Watch subscribers lock poisoned"))?;
            if let Some(subscribers) = subs.get_mut(&collective_id) {
                subscribers.retain(|(id, _)| !disconnected.contains(id));
                if subscribers.is_empty() {
                    subs.remove(&collective_id);
                }
            }
        }

        Ok(())
    }

    /// Returns `true` if any subscribers are registered.
    ///
    /// Cheap check used by mutation methods to skip the `get_experience`
    /// read when nobody is watching.
    pub(crate) fn has_subscribers(&self) -> bool {
        if !self.in_process {
            return false;
        }
        self.subscribers
            .read()
            .map(|subs| !subs.is_empty())
            .unwrap_or(false)
    }

    /// Removes a specific subscriber. Called from [`WatchStream::drop`].
    ///
    /// Silently handles lock poisoning since this runs in a drop handler
    /// where returning errors is not possible.
    fn remove_subscriber(&self, collective_id: CollectiveId, subscriber_id: u64) {
        let mut subs = match self.subscribers.write() {
            Ok(guard) => guard,
            Err(_) => {
                warn!("Watch subscribers lock poisoned during cleanup");
                return;
            }
        };
        if let Some(subscribers) = subs.get_mut(&collective_id) {
            subscribers.retain(|(id, _)| *id != subscriber_id);
            if subscribers.is_empty() {
                subs.remove(&collective_id);
            }
        }
    }
}

/// Checks whether an experience matches a subscriber's filter.
///
/// Returns `true` if the filter is `None` (no filtering) or all specified
/// criteria match. Criteria are combined with AND logic.
fn matches_filter(filter: &Option<WatchFilter>, experience: &Experience) -> bool {
    let filter = match filter {
        Some(f) => f,
        None => return true,
    };

    // Domain filter: at least one domain must overlap
    if let Some(ref domains) = filter.domains {
        let exp_domains = &experience.domain;
        let has_match = domains.iter().any(|d| exp_domains.contains(d));
        if !has_match {
            return false;
        }
    }

    // Experience type filter: discriminant must match
    if let Some(ref types) = filter.experience_types {
        let exp_type = &experience.experience_type;
        let has_match = types
            .iter()
            .any(|t| std::mem::discriminant(t) == std::mem::discriminant(exp_type));
        if !has_match {
            return false;
        }
    }

    // Importance threshold
    if let Some(min_importance) = filter.min_importance {
        if experience.importance < min_importance {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::experience::ExperienceType;
    use crate::types::{AgentId, ExperienceId, Timestamp};
    use std::sync::Arc;

    /// Helper: create a minimal experience for filter testing.
    fn test_experience(
        collective_id: CollectiveId,
        domains: Vec<String>,
        exp_type: ExperienceType,
        importance: f32,
    ) -> Experience {
        let timestamp = Timestamp::now();
        Experience {
            id: ExperienceId::new(),
            collective_id,
            content: "test".to_string(),
            embedding: vec![0.0; 384],
            experience_type: exp_type,
            importance,
            confidence: 1.0,
            applications: std::collections::BTreeMap::new(),
            domain: domains,
            related_files: vec![],
            source_agent: AgentId::new("test-agent"),
            source_task: None,
            timestamp,
            last_reinforced: timestamp,
            archived: false,
        }
    }

    /// Helper: create a watch event.
    fn test_event(collective_id: CollectiveId, event_type: WatchEventType) -> WatchEvent {
        WatchEvent {
            experience_id: ExperienceId::new(),
            collective_id,
            event_type,
            timestamp: Timestamp::now(),
            experience: None,
        }
    }

    #[test]
    fn subscribe_returns_stream() {
        let service = Arc::new(WatchService::new(100, true));
        let collective = CollectiveId::new();
        let _stream = service.subscribe(collective, None).unwrap();
        assert!(service.has_subscribers());
    }

    #[test]
    fn emit_delivers_to_subscriber() {
        let service = Arc::new(WatchService::new(100, true));
        let collective = CollectiveId::new();
        let stream = service.subscribe(collective, None).unwrap();

        let exp = test_experience(
            collective,
            vec!["test".to_string()],
            ExperienceType::Generic { category: None },
            0.5,
        );
        let event = test_event(collective, WatchEventType::Created);

        service.emit(event.clone(), &exp).unwrap();

        let received = stream.receiver.try_recv().unwrap();
        assert_eq!(received.event_type, WatchEventType::Created);
        assert_eq!(received.collective_id, collective);
    }

    #[test]
    fn emit_delivers_to_multiple_subscribers() {
        let service = Arc::new(WatchService::new(100, true));
        let collective = CollectiveId::new();
        let stream1 = service.subscribe(collective, None).unwrap();
        let stream2 = service.subscribe(collective, None).unwrap();

        let exp = test_experience(
            collective,
            vec![],
            ExperienceType::Generic { category: None },
            0.5,
        );
        service
            .emit(test_event(collective, WatchEventType::Created), &exp)
            .unwrap();

        assert!(stream1.receiver.try_recv().is_ok());
        assert!(stream2.receiver.try_recv().is_ok());
    }

    #[test]
    fn emit_isolates_collectives() {
        let service = Arc::new(WatchService::new(100, true));
        let coll_a = CollectiveId::new();
        let coll_b = CollectiveId::new();
        let stream_a = service.subscribe(coll_a, None).unwrap();
        let _stream_b = service.subscribe(coll_b, None).unwrap();

        let exp = test_experience(
            coll_a,
            vec![],
            ExperienceType::Generic { category: None },
            0.5,
        );
        service
            .emit(test_event(coll_a, WatchEventType::Created), &exp)
            .unwrap();

        // Stream A receives, Stream B does not
        assert!(stream_a.receiver.try_recv().is_ok());
        assert!(_stream_b.receiver.try_recv().is_err());
    }

    #[test]
    fn filter_by_domain() {
        let service = Arc::new(WatchService::new(100, true));
        let collective = CollectiveId::new();
        let filter = WatchFilter {
            domains: Some(vec!["security".to_string()]),
            ..Default::default()
        };
        let stream = service.subscribe(collective, Some(filter)).unwrap();

        // Matching domain
        let exp_match = test_experience(
            collective,
            vec!["security".to_string(), "auth".to_string()],
            ExperienceType::Generic { category: None },
            0.5,
        );
        service
            .emit(test_event(collective, WatchEventType::Created), &exp_match)
            .unwrap();
        assert!(stream.receiver.try_recv().is_ok());

        // Non-matching domain
        let exp_no_match = test_experience(
            collective,
            vec!["performance".to_string()],
            ExperienceType::Generic { category: None },
            0.5,
        );
        service
            .emit(
                test_event(collective, WatchEventType::Created),
                &exp_no_match,
            )
            .unwrap();
        assert!(stream.receiver.try_recv().is_err());
    }

    #[test]
    fn filter_by_experience_type() {
        let service = Arc::new(WatchService::new(100, true));
        let collective = CollectiveId::new();
        let filter = WatchFilter {
            experience_types: Some(vec![ExperienceType::Fact {
                statement: String::new(),
                source: String::new(),
            }]),
            ..Default::default()
        };
        let stream = service.subscribe(collective, Some(filter)).unwrap();

        // Matching type (Fact)
        let exp_fact = test_experience(
            collective,
            vec![],
            ExperienceType::Fact {
                statement: "test".to_string(),
                source: "test".to_string(),
            },
            0.5,
        );
        service
            .emit(test_event(collective, WatchEventType::Created), &exp_fact)
            .unwrap();
        assert!(stream.receiver.try_recv().is_ok());

        // Non-matching type (Generic)
        let exp_generic = test_experience(
            collective,
            vec![],
            ExperienceType::Generic { category: None },
            0.5,
        );
        service
            .emit(
                test_event(collective, WatchEventType::Created),
                &exp_generic,
            )
            .unwrap();
        assert!(stream.receiver.try_recv().is_err());
    }

    #[test]
    fn filter_by_min_importance() {
        let service = Arc::new(WatchService::new(100, true));
        let collective = CollectiveId::new();
        let filter = WatchFilter {
            min_importance: Some(0.7),
            ..Default::default()
        };
        let stream = service.subscribe(collective, Some(filter)).unwrap();

        // Above threshold
        let exp_high = test_experience(
            collective,
            vec![],
            ExperienceType::Generic { category: None },
            0.8,
        );
        service
            .emit(test_event(collective, WatchEventType::Created), &exp_high)
            .unwrap();
        assert!(stream.receiver.try_recv().is_ok());

        // Below threshold
        let exp_low = test_experience(
            collective,
            vec![],
            ExperienceType::Generic { category: None },
            0.3,
        );
        service
            .emit(test_event(collective, WatchEventType::Created), &exp_low)
            .unwrap();
        assert!(stream.receiver.try_recv().is_err());
    }

    #[test]
    fn subscriber_cleanup_on_drop() {
        let service = Arc::new(WatchService::new(100, true));
        let collective = CollectiveId::new();

        {
            let _stream = service.subscribe(collective, None).unwrap();
            assert!(service.has_subscribers());
        }
        // Stream dropped — subscriber should be removed
        assert!(!service.has_subscribers());
    }

    #[test]
    fn dead_subscriber_cleaned_on_emit() {
        let service = Arc::new(WatchService::new(100, true));
        let collective = CollectiveId::new();

        // Manually inject a subscriber whose receiver has been dropped,
        // simulating a dead subscriber that bypassed WatchStream::drop cleanup.
        let (sender, receiver) = crossbeam_channel::bounded::<WatchEvent>(10);
        let waker = Arc::new(AtomicWaker::new());
        {
            let mut subs = service.subscribers.write().unwrap();
            subs.entry(collective).or_default().push((
                99,
                Subscriber {
                    sender,
                    waker,
                    filter: None,
                },
            ));
        }
        assert!(service.has_subscribers());

        // Drop the receiver — now the channel is disconnected
        drop(receiver);

        // Emit should detect disconnected sender and clean up
        let exp = test_experience(
            collective,
            vec![],
            ExperienceType::Generic { category: None },
            0.5,
        );
        service
            .emit(test_event(collective, WatchEventType::Created), &exp)
            .unwrap();
        assert!(!service.has_subscribers());
    }

    #[test]
    fn buffer_full_does_not_block() {
        let service = Arc::new(WatchService::new(2, true)); // Tiny buffer
        let collective = CollectiveId::new();
        let stream = service.subscribe(collective, None).unwrap();

        let exp = test_experience(
            collective,
            vec![],
            ExperienceType::Generic { category: None },
            0.5,
        );

        // Fill the buffer
        service
            .emit(test_event(collective, WatchEventType::Created), &exp)
            .unwrap();
        service
            .emit(test_event(collective, WatchEventType::Created), &exp)
            .unwrap();

        // Third emit should not block (buffer full, event dropped)
        service
            .emit(test_event(collective, WatchEventType::Created), &exp)
            .unwrap();

        // Only 2 events received
        assert!(stream.receiver.try_recv().is_ok());
        assert!(stream.receiver.try_recv().is_ok());
        assert!(stream.receiver.try_recv().is_err());
    }

    #[test]
    fn has_subscribers_empty() {
        let service = Arc::new(WatchService::new(100, true));
        assert!(!service.has_subscribers());
    }

    #[test]
    fn combined_filter_requires_all_criteria() {
        let service = Arc::new(WatchService::new(100, true));
        let collective = CollectiveId::new();
        let filter = WatchFilter {
            domains: Some(vec!["security".to_string()]),
            min_importance: Some(0.7),
            ..Default::default()
        };
        let stream = service.subscribe(collective, Some(filter)).unwrap();

        // Matches domain but NOT importance → filtered out
        let exp = test_experience(
            collective,
            vec!["security".to_string()],
            ExperienceType::Generic { category: None },
            0.3,
        );
        service
            .emit(test_event(collective, WatchEventType::Created), &exp)
            .unwrap();
        assert!(stream.receiver.try_recv().is_err());

        // Matches importance but NOT domain → filtered out
        let exp2 = test_experience(
            collective,
            vec!["performance".to_string()],
            ExperienceType::Generic { category: None },
            0.9,
        );
        service
            .emit(test_event(collective, WatchEventType::Created), &exp2)
            .unwrap();
        assert!(stream.receiver.try_recv().is_err());

        // Matches BOTH → delivered
        let exp3 = test_experience(
            collective,
            vec!["security".to_string()],
            ExperienceType::Generic { category: None },
            0.9,
        );
        service
            .emit(test_event(collective, WatchEventType::Created), &exp3)
            .unwrap();
        assert!(stream.receiver.try_recv().is_ok());
    }

    #[test]
    fn in_process_disabled_no_events() {
        let service = Arc::new(WatchService::new(100, false));
        let collective = CollectiveId::new();
        let stream = service.subscribe(collective, None).unwrap();

        let exp = test_experience(
            collective,
            vec!["test".to_string()],
            ExperienceType::Generic { category: None },
            0.5,
        );
        service
            .emit(test_event(collective, WatchEventType::Created), &exp)
            .unwrap();

        // No events should arrive when in_process is disabled
        assert!(stream.receiver.try_recv().is_err());
    }

    #[test]
    fn in_process_disabled_has_subscribers_false() {
        let service = Arc::new(WatchService::new(100, false));
        let collective = CollectiveId::new();

        // Even after subscribing, has_subscribers returns false
        let _stream = service.subscribe(collective, None).unwrap();
        assert!(!service.has_subscribers());
    }

    #[test]
    fn in_process_enabled_receives_events() {
        let service = Arc::new(WatchService::new(100, true));
        let collective = CollectiveId::new();
        let stream = service.subscribe(collective, None).unwrap();

        let exp = test_experience(
            collective,
            vec!["test".to_string()],
            ExperienceType::Generic { category: None },
            0.5,
        );
        service
            .emit(test_event(collective, WatchEventType::Created), &exp)
            .unwrap();

        // Events should arrive when in_process is enabled
        let event = stream.receiver.try_recv().unwrap();
        assert_eq!(event.event_type, WatchEventType::Created);
    }
}
