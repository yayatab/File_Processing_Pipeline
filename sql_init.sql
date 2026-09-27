CREATE DATABASE IF NOT EXISTS pipeline;
USE pipeline;

CREATE TABLE IF NOT EXISTS job (
    id CHAR(32) PRIMARY KEY,
    status VARCHAR(255) DEFAULT 'PENDING',
    current_step_index INT DEFAULT 0,
    pipeline_definition VARCHAR(2000),
    error_message VARCHAR(255),
    created_at DATETIME,
    started_at DATETIME,
    completed_at DATETIME,
    parent_job_id CHAR(32),
    pending_dependencies INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS jobstep (
    id CHAR(32) PRIMARY KEY,
    job_id CHAR(32),
    step_index INT,
    step_type VARCHAR(255),
    params VARCHAR(1000),
    status VARCHAR(255) DEFAULT 'PENDING',
    error_message VARCHAR(255),
    started_at DATETIME,
    completed_at DATETIME,
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS filereference (
    id CHAR(32) PRIMARY KEY,
    job_id CHAR(32),
    storage_path VARCHAR(255),
    original_filename VARCHAR(255),
    size_in_mb FLOAT,
    content_type VARCHAR(255),
    created_at DATETIME,
    FOREIGN KEY (job_id) REFERENCES job(id)
);
