-- Supporting indexes: /v1/stats counts dropped redeliveries without scanning
-- the whole status log, and the retention janitor deletes by age.
CREATE INDEX status_log_dropped_idx ON status_log (actor) WHERE status = 'duplicate_dropped';
CREATE INDEX status_log_at_idx ON status_log (at);
CREATE INDEX inbox_processed_idx ON inbox (processed_at);
