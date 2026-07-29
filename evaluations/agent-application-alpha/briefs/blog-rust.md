# Sealed brief: Blog/CMS in Rust

From an empty repository, build a Rust Blog/CMS application using only the
bundled public RiffDB interfaces. Model sites, authors, draft/published posts,
slug lookup, comments, tags, and moderation. Implement typed command writes and
one named RiffQL operation for each feed, moderation list, slug lookup, and post
detail page. The detail page must return author, bounded comments, and tags in
one request. Seed useful data and demonstrate a successful write and page read.

Do not use TicketDesk, RiffDB implementation source, kernel APIs, numeric stable
IDs, masks, encoded keys, handwritten transport/codec glue, client-side
semantic joins, or unrestricted scans.
