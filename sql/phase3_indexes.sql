-- Apply after populate_database.py has created and loaded the tables.
-- These indexes support the WebUI's exact-match filters and date ordering.

CREATE INDEX IF NOT EXISTS idx_nyc_created_at ON nyc_requests (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_nyc_source_city ON nyc_requests (source_city);
CREATE INDEX IF NOT EXISTS idx_nyc_status_group ON nyc_requests (status_group);
CREATE INDEX IF NOT EXISTS idx_nyc_responsible_unit ON nyc_requests (responsible_unit_original);
CREATE INDEX IF NOT EXISTS idx_nyc_category ON nyc_requests (category_original);
CREATE INDEX IF NOT EXISTS idx_nyc_channel ON nyc_requests (intake_channel_original);
CREATE INDEX IF NOT EXISTS idx_nyc_postal_code ON nyc_requests (postal_code);

CREATE INDEX IF NOT EXISTS idx_la_created_at ON la_requests (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_la_source_city ON la_requests (source_city);
CREATE INDEX IF NOT EXISTS idx_la_status_group ON la_requests (status_group);
CREATE INDEX IF NOT EXISTS idx_la_responsible_unit ON la_requests (responsible_unit_original);
CREATE INDEX IF NOT EXISTS idx_la_category ON la_requests (category_original);
CREATE INDEX IF NOT EXISTS idx_la_channel ON la_requests (intake_channel_original);
CREATE INDEX IF NOT EXISTS idx_la_postal_code ON la_requests (postal_code);

CREATE INDEX IF NOT EXISTS idx_merged_created_at ON merged_requests (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_merged_source_city ON merged_requests (source_city);
CREATE INDEX IF NOT EXISTS idx_merged_status ON merged_requests (status);
CREATE INDEX IF NOT EXISTS idx_merged_organization ON merged_requests (responsible_organization);
CREATE INDEX IF NOT EXISTS idx_merged_request_type ON merged_requests (request_type);
CREATE INDEX IF NOT EXISTS idx_merged_channel ON merged_requests (submission_channel);
CREATE INDEX IF NOT EXISTS idx_merged_postal_code ON merged_requests (postal_code);

