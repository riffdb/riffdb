ALTER TYPE attention_origin ADD VALUE 'email';

ALTER TABLE attention_item ADD COLUMN source_channel varchar(32);
UPDATE attention_item SET source_channel = 'legacy';
ALTER TABLE attention_item ALTER COLUMN source_channel SET NOT NULL;
CREATE INDEX attention_item_by_origin
    ON attention_item (workspace_id, origin, attention_id);

CREATE TYPE email_draft_status AS ENUM ('proposed', 'approved', 'discarded', 'sent');
CREATE TABLE email_draft (
    workspace_id uuid NOT NULL,
    draft_id uuid NOT NULL,
    thread_external_id varchar(128) NOT NULL,
    body varchar(4096) NOT NULL,
    status email_draft_status NOT NULL,
    PRIMARY KEY (workspace_id, draft_id)
);
