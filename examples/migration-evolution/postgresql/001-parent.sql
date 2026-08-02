CREATE TYPE attention_origin AS ENUM ('linear', 'github', 'attention_other');

CREATE TABLE attention_item (
    workspace_id uuid NOT NULL,
    attention_id uuid NOT NULL,
    origin attention_origin NOT NULL,
    external_id varchar(128) NOT NULL,
    PRIMARY KEY (workspace_id, attention_id)
);
