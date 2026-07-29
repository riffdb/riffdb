# Sealed brief: Orders/Inventory in Rust

From an empty repository, build a Rust order-management application using only
bundled public RiffDB interfaces. Model customers, products with exact prices,
inventory, orders, lines, reservation, and fulfillment state. Use compiled
commands for state changes and named RiffQL operations for order detail,
customer history, open orders, and inventory dashboard. Inventory reservation
must preserve nonnegative stock under retry. Seed useful data and demonstrate a
successful write and page read.

Do not use TicketDesk, RiffDB implementation source, kernel APIs, numeric stable
IDs, masks, encoded keys, handwritten transport/codec glue, client-side
semantic joins, or unrestricted scans.
