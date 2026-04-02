#[cfg(test)]
mod tests;

mod strict_instance_aware_counter;
pub use strict_instance_aware_counter::*;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn generate_instance_id() -> String {
    Uuid::new_v4().to_string()
}
