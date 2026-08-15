# Sealed brief: Blog/CMS in Go

From an empty repository, build a real Go web application for a Blog/CMS using
only bundled public RiffDB interfaces. Model sites, authors, draft/published
posts, slug lookup, comments, tags, and moderation. Use generated typed
commands and one named RiffQL operation per feed, moderation list, slug lookup,
and post detail page. Serve a real HTTP post page containing author, bounded
comments, and tags. Seed useful data and demonstrate a successful generated
write followed immediately by a page read fenced with that exact commit.

Do not use TicketDesk, RiffDB implementation source, kernel APIs, numeric stable
IDs, masks, encoded keys, handwritten transport/codec glue, client-side
semantic joins, or unrestricted scans.
