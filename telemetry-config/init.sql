-- PostgreSQL Schema for IICPC Distributed Benchmarking Platform

-- Identity records are stored in MongoDB. PostgreSQL stores benchmark data only.
CREATE TABLE IF NOT EXISTS submissions (
    id SERIAL PRIMARY KEY,
    team_name VARCHAR(100) NOT NULL,
    docker_image_tag VARCHAR(150) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending', -- pending, building, built, failed
    build_logs TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create historical benchmark runs table
CREATE TABLE IF NOT EXISTS benchmark_runs (
    id UUID PRIMARY KEY,
    submission_id INTEGER REFERENCES submissions(id) ON DELETE CASCADE,
    status VARCHAR(50) DEFAULT 'pending', -- running, completed, failed
    total_orders_sent INTEGER DEFAULT 0,
    success_rate DOUBLE PRECISION DEFAULT 0.0, -- stored as a percentage from 0 to 100
    p50_latency_ms DOUBLE PRECISION DEFAULT 0.0,
    p90_latency_ms DOUBLE PRECISION DEFAULT 0.0,
    p99_latency_ms DOUBLE PRECISION DEFAULT 0.0,
    avg_tps DOUBLE PRECISION DEFAULT 0.0,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP WITH TIME ZONE
);

-- Seed optional mock submissions for local smoke tests.
INSERT INTO submissions (team_name, docker_image_tag, status)
SELECT seed.team_name, 'mock-contestant:latest', 'built'
FROM (VALUES ('alpha_traders'), ('beta_quant')) AS seed(team_name)
WHERE NOT EXISTS (
    SELECT 1 FROM submissions s
    WHERE s.team_name = seed.team_name AND s.docker_image_tag = 'mock-contestant:latest'
);
