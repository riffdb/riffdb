#![forbid(unsafe_code)]

#[allow(dead_code)]
pub mod blog {
    include!("../../agent-blog/generated/rust/client.rs");
}

#[allow(dead_code)]
pub mod orders {
    include!("../../agent-orders/generated/rust/client.rs");
}
