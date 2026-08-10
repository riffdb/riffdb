package ticketdesk

import (
	"testing"

	riffdb "riffdb.dev/application"
)

func TestGeneratedCommandCodecUsesTaggedUUIDsAndTypedOutcome(t *testing.T) {
	input := encodeCreateCommentInput(CreateCommentInput{
		Body: "hello", AuthorId: "018f0f79-7b5e-7c03-9b12-b16f57a4c991",
		TicketId: "018f0f79-7b5e-7c03-9b12-b16f57a4c992",
		CommentId: "018f0f79-7b5e-7c03-9b12-b16f57a4c993",
		IdempotencyKey: "comment-one", OrganizationId: "018f0f79-7b5e-7c03-9b12-b16f57a4c994",
	})
	for _, name := range []string{"author_id", "ticket_id", "comment_id", "organization_id"} {
		if input[name].Type != "uuid" {
			t.Fatalf("%s lost its UUID tag: %#v", name, input[name])
		}
	}
	outcome, err := decodeCreateCommentOutcome(riffdb.Record(map[string]riffdb.Value{
		"outcome": riffdb.Enum("CommentExists"),
		"comment_id": riffdb.UUID("018f0f79-7b5e-7c03-9b12-b16f57a4c993"),
	}))
	if err != nil {
		t.Fatal(err)
	}
	duplicate, ok := outcome.(CreateCommentCommentExists)
	if !ok || duplicate.CommentId != "018f0f79-7b5e-7c03-9b12-b16f57a4c993" {
		t.Fatalf("declared outcome was not decoded exactly: %#v", outcome)
	}
}

func TestGeneratedQueryOutcomeIsClosed(t *testing.T) {
	result, err := decodeGetTicketResult(riffdb.Record(map[string]riffdb.Value{
		"outcome": riffdb.Enum("NotFound"),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := result.(GetTicketNotFound); !ok {
		t.Fatalf("unexpected query outcome: %#v", result)
	}
	if _, err := decodeGetTicketResult(riffdb.Record(map[string]riffdb.Value{
		"outcome": riffdb.Enum("Undeclared"),
	})); err == nil {
		t.Fatal("undeclared query outcome was accepted")
	}
}
