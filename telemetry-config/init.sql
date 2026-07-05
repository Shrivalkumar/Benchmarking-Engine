-- PostgreSQL Schema for IICPC Distributed Benchmarking Platform

-- Create contestants table
CREATE TABLE IF NOT EXISTS contestants (
    id SERIAL PRIMARY KEY,
    team_name VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create submissions table
CREATE TABLE IF NOT EXISTS submissions (
    id SERIAL PRIMARY KEY,
    contestant_id INTEGER REFERENCES contestants(id) ON DELETE CASCADE,
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
    success_rate DOUBLE PRECISION DEFAULT 0.0,
    p50_latency_ms DOUBLE PRECISION DEFAULT 0.0,
    p90_latency_ms DOUBLE PRECISION DEFAULT 0.0,
    p99_latency_ms DOUBLE PRECISION DEFAULT 0.0,
    avg_tps DOUBLE PRECISION DEFAULT 0.0,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP WITH TIME ZONE
);

-- Seed initial contestant and submission data for testing purposes
INSERT INTO contestants (team_name) 
VALUES ('alpha_traders'), ('beta_quant') 
ON CONFLICT (team_name) DO NOTHING;

INSERT INTO submissions (contestant_id, docker_image_tag, status) 
VALUES 
(1, 'mock-contestant:latest', 'built'),
(2, 'mock-contestant:latest', 'built')
ON CONFLICT DO NOTHING;
