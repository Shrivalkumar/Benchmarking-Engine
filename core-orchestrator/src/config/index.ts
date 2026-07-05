import { Pool } from 'pg';
import { createClient } from 'redis';
import Docker from 'dockerode';
import mongoose from 'mongoose';

// Environment variables configuration
export const PORT = process.env.PORT || 8000;
export const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/benchmarking';
export const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
export const MONGODB_URI = process.env.MONGODB_URI || '';
if (!MONGODB_URI) {
  console.error('❌ CRITICAL ERROR: MONGODB_URI is not defined in environment variables.');
  process.exit(1);
}
export const KAFKA_BROKERS = process.env.KAFKA_BROKERS || 'localhost:9092';
export const BENCHMARK_NET = process.env.BENCHMARK_NET || 'benchmarking-net';
export const BOT_FLEET_URL = process.env.BOT_FLEET_URL || 'http://localhost:8081';

// PostgreSQL Pool
export const db = new Pool({
  connectionString: DATABASE_URL,
});

// Redis Client
export const redis = createClient({
  url: REDIS_URL,
});

redis.on('error', (err) => console.error('Redis Client Error', err));

// Docker Engine API client (reads default socket /var/run/docker.sock)
export const docker = new Docker();

// Initialize database tables programmatically if they do not exist
async function initializeDatabaseSchema(client: any) {
  console.log('🌱 Initializing PostgreSQL database schema programmatically...');
  
  await client.query(`
    CREATE TABLE IF NOT EXISTS contestants (
        id SERIAL PRIMARY KEY,
        team_name VARCHAR(100) UNIQUE NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
  `);
  
  await client.query(`
    CREATE TABLE IF NOT EXISTS submissions (
        id SERIAL PRIMARY KEY,
        contestant_id INTEGER REFERENCES contestants(id) ON DELETE CASCADE,
        docker_image_tag VARCHAR(150) NOT NULL,
        status VARCHAR(50) DEFAULT 'pending',
        build_logs TEXT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS benchmark_runs (
        id UUID PRIMARY KEY,
        submission_id INTEGER REFERENCES submissions(id) ON DELETE CASCADE,
        status VARCHAR(50) DEFAULT 'pending',
        total_orders_sent INTEGER DEFAULT 0,
        success_rate DOUBLE PRECISION DEFAULT 0.0,
        p50_latency_ms DOUBLE PRECISION DEFAULT 0.0,
        p90_latency_ms DOUBLE PRECISION DEFAULT 0.0,
        p99_latency_ms DOUBLE PRECISION DEFAULT 0.0,
        avg_tps DOUBLE PRECISION DEFAULT 0.0,
        started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        ended_at TIMESTAMP WITH TIME ZONE
    );
  `);

  // Seed default teams
  await client.query(`
    INSERT INTO contestants (team_name) 
    VALUES ('alpha_traders'), ('beta_quant') 
    ON CONFLICT (team_name) DO NOTHING;
  `);

  // Check if submissions exist, if not insert default ones
  const subCount = await client.query('SELECT COUNT(*) FROM submissions');
  if (parseInt(subCount.rows[0].count, 10) === 0) {
    await client.query(`
      INSERT INTO submissions (id, contestant_id, docker_image_tag, status) 
      VALUES 
      (1, 1, 'mock-contestant:latest', 'built'),
      (2, 2, 'mock-contestant:latest', 'built')
      ON CONFLICT DO NOTHING;
    `);
    // Reset serial sequence
    await client.query("SELECT setval('submissions_id_seq', (SELECT MAX(id) FROM submissions));");
  }

  console.log('✅ PostgreSQL database schema initialized successfully.');
}

// Initialize external connections
export async function initConnections() {
  await redis.connect();
  console.log('✅ Connected to Redis successfully');
  
  await mongoose.connect(MONGODB_URI);
  console.log('✅ Connected to MongoDB Atlas successfully');
  
  const client = await db.connect();
  try {
    console.log('✅ Connected to PostgreSQL successfully');
    await initializeDatabaseSchema(client);
  } finally {
    client.release();
  }
}
