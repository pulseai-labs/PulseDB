//! Experience management module.
//!
//! An **experience** is the core data type in PulseDB — a unit of learned knowledge
//! stored in a collective. Experiences have content, a semantic embedding for
//! vector search, a rich type, and metadata.
//!
//! # Operations
//!
//! All experience operations are available on [`PulseDB`](crate::PulseDB):
//!
//! - [`record_experience(exp)`](crate::PulseDB::record_experience)
//! - [`get_experience(id)`](crate::PulseDB::get_experience)
//! - [`update_experience(id, update)`](crate::PulseDB::update_experience)
//! - [`archive_experience(id)`](crate::PulseDB::archive_experience)
//! - [`unarchive_experience(id)`](crate::PulseDB::unarchive_experience)
//! - [`delete_experience(id)`](crate::PulseDB::delete_experience)
//! - [`reinforce_experience(id)`](crate::PulseDB::reinforce_experience)

mod decay;
pub mod types;
mod validation;

pub use decay::energy;
pub use types::{Experience, ExperienceType, ExperienceUpdate, NewExperience, Severity};
pub(crate) use validation::{validate_experience_update, validate_new_experience};
