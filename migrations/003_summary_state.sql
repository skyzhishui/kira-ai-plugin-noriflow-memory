-- 003_summary_state.sql: add an encoding-state column to the summary table
-- (raw text from retain-encoding degradation stays out of recall)
--
-- Background: when the encoder degrades fail-open, the raw encoding input
-- (chat log lines with envelope format and history context markers) is
-- written into this table as content. Raw-text rows entering recall would
-- pollute the inject context. This migration introduces the summarized
-- state column to separate "unencoded raw text" from "encoded summaries":
-- summarized=false rows stay out of recall (filtered by the search SQL);
-- the merge agent's re-encoding pass re-encodes them (summary UPDATE on the
-- original row + facts into the fact table) and flips them back to true, so
-- no data is lost. bot_self rows write raw text by design (raw is the final
-- state), always true, unaffected.

ALTER TABLE memory_chat_summary
    ADD COLUMN IF NOT EXISTS summarized BOOLEAN NOT NULL DEFAULT true;

-- Legacy cleanup: set already-stored degraded raw-text rows to false
-- (waiting for the re-encoding pass to re-encode them). Identifying
-- features, match either:
-- a) rows containing the history context separator (HISTORY_BATCH_SEPARATOR);
-- b) leading envelope format (format_history_message output: envelope rows
--    starting with "<msg ts=" at line start) — covers degraded rows whose
--    history window was empty and carried no separator. Normal summaries
--    are natural-language paraphrases and never start with the envelope
--    pattern.
UPDATE memory_chat_summary
SET summarized = false
WHERE content LIKE '%以上为历史上下文%'
   OR content ~ '(^|\n)<msg ts="20[0-9]{2}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}"';

-- Scan index for the re-encoding pass (unsummarized rows are usually few; a partial index suffices)
CREATE INDEX IF NOT EXISTS idx_mcs_unsummarized
    ON memory_chat_summary (id) WHERE NOT summarized;
