ALTER TABLE generations
    DROP CONSTRAINT generations_modality_check,
    ADD CONSTRAINT generations_modality_check
    CHECK (modality IN ('llm', 'image', 'video', 'music', 'comfy')),
    DROP CONSTRAINT generations_target_kind_check,
    ADD CONSTRAINT generations_target_kind_check
    CHECK (target_kind IN ('model', 'workflow', 'comfy_prompt'));

CREATE TABLE comfy_prompts (
    tenant_id uuid NOT NULL,
    generation_id uuid NOT NULL,
    prompt_id uuid NOT NULL,
    client_id text,
    number bigint NOT NULL CHECK (number >= 0),
    outputs jsonb CHECK (outputs IS NULL OR jsonb_typeof(outputs) = 'object'),
    output_node_ids text[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (tenant_id, generation_id),
    UNIQUE (tenant_id, prompt_id),
    UNIQUE (tenant_id, number),
    FOREIGN KEY (tenant_id, generation_id)
        REFERENCES generations (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX comfy_prompts_client_number_idx
    ON comfy_prompts (tenant_id, client_id, number);

ALTER TABLE artifacts
    ADD COLUMN comfy_output_pointers text[] NOT NULL DEFAULT '{}';

ALTER TABLE comfy_prompts ENABLE ROW LEVEL SECURITY;
ALTER TABLE comfy_prompts FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON comfy_prompts
    FOR ALL TO gpq_app
    USING (tenant_id = gpq_current_tenant())
    WITH CHECK (tenant_id = gpq_current_tenant());

CREATE POLICY administration ON comfy_prompts
    FOR ALL TO gpq_admin
    USING (true)
    WITH CHECK (true);

GRANT SELECT, INSERT, UPDATE, DELETE ON comfy_prompts TO gpq_app;
GRANT ALL ON comfy_prompts TO gpq_admin;
