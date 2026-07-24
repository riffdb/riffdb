#![forbid(unsafe_code)]

//! Loopback process fixture that exposes the real hosted MCP HTTP wrapper.

use std::{
    io::Write as _,
    net::{SocketAddr, SocketAddrV4},
};

use axum::{
    Router,
    body::Body,
    extract::{ConnectInfo, State},
    http::{Request, Response},
    routing::any,
};
use tower_service::Service;

#[allow(dead_code)]
#[path = "conformance_support/auth.rs"]
mod auth;
#[allow(dead_code)]
#[path = "conformance_support/backend.rs"]
mod backend;
#[allow(dead_code)]
#[path = "conformance_support/hosted.rs"]
mod hosted;

use backend::ConformanceBackend;
use hosted::{HostedConformanceRegistration, hosted_registration};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let listener = tokio::net::TcpListener::bind(SocketAddr::V4(SocketAddrV4::new(
        std::net::Ipv4Addr::LOCALHOST,
        0,
    )))
    .await
    .expect("bind conformance HTTP listener");
    let address = listener.local_addr().expect("conformance listener address");
    let (registration, _authenticator) =
        hosted_registration(address, ConformanceBackend::default());
    let application = Router::new()
        .fallback(any(proxy))
        .with_state(registration.clone());

    println!("RIFFDB_MCP_CONFORMANCE_URL=http://{address}/mcp");
    std::io::stdout()
        .flush()
        .expect("flush conformance endpoint");

    axum::serve(
        listener,
        application.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .expect("serve conformance HTTP listener");
    registration.shutdown();
}

async fn proxy(
    State(registration): State<HostedConformanceRegistration>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request<Body>,
) -> Response<Body> {
    let Ok(mut connection) = registration.service_for_peer(peer) else {
        return Response::builder()
            .status(403)
            .body(Body::empty())
            .expect("static rejection");
    };
    connection
        .call(request)
        .await
        .expect("hosted wrapper is infallible")
        .map(Body::new)
}
