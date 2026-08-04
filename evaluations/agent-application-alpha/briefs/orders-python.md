# Sealed brief: Orders/Inventory in Python

From an empty repository, build a real Python web order-management application
using only bundled public RiffDB interfaces. Model customers, products with
exact prices, inventory, orders, lines, reservation, and fulfillment state.
Use generated typed commands and named RiffQL operations for order detail,
customer history, open orders, and inventory dashboard. Exercise both the
generated synchronous and asynchronous Python application clients, serve a
real HTTP order page, seed useful data, and demonstrate a successful generated
write followed immediately by a page read fenced with that exact commit.

Do not use TicketDesk, RiffDB implementation source, kernel APIs, numeric stable
IDs, masks, encoded keys, handwritten transport/codec glue, client-side
semantic joins, or unrestricted scans.
