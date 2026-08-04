# Sealed brief: reactive TicketDesk

Starting from the supplied symbolic TicketDesk application sources, build and
exercise the complete public reactive application path using only the bundled
RiffDB binaries, documentation, generated bindings, and language runtimes.

Deliver:

- an application-owned authenticated browser relay and usable queue/detail UI;
- two independent browser sessions that converge after a ticket mutation;
- reconnect/reset handling and protected-state clearing after authorization
  termination;
- a contextual TicketCreated worker that receives one-snapshot TicketPage
  hydration and reacts only through the generated CreateComment helper;
- crash-after-reaction-before-ack recovery proving the original command
  outcome is reused and business state is not duplicated; and
- an MCP wakeup check proving notifications contain no event or hydrated
  context payload.

Use the supplied Application Source V4 as author input and generate its exact
Lock V5 and Rust, TypeScript, Python, and MCP artifacts. Run the application
boundary checker and your own deterministic acceptance tests. Browser code must
never receive a RiffDB capability, credential path, lease token, causation
token, raw storage key, or kernel-shaped request.

Do not inspect RiffDB implementation source, the repository TicketDesk
implementation, prior evaluation runs, or another filesystem tree. Do not use
kernel APIs, handwritten RiffDB transports/codecs, direct storage access,
client-side semantic joins, polling, raw CDC, or unrestricted scans. Record an
honest failure if the bundled public product cannot complete a required step.
