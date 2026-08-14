import { Pool } from 'pg';
import { createClient } from 'redis';
import Docker from 'dockerode';
import { Db, MongoClient } from 'mongodb';

// Environment variables configuration
export const PORT = process.env.PORT || 8000;
export const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/benchmarking';
export const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
// Identity data is intentionally kept in the configured external MongoDB instance.
export const MONGO_URL = process.env.MONGO_URL || process.env.MONGODB_URI;
export const MONGO_DB_NAME = process.env.MONGO_DB_NAME || 'benchmarking';
export const KAFKA_BROKERS = process.env.KAFKA_BROKERS || 'localhost:9092';
export const BENCHMARK_NET = process.env.BENCHMARK_NET || 'benchmarking-net';
export const BOT_FLEET_URL = process.env.BOT_FLEET_URL || 'http://localhost:8081';
export const JWT_SECRET = process.env.JWT_SECRET || 'local-dev-change-me';
export const INTERNAL_API_TOKEN = process.env.INTERNAL_API_TOKEN || 'local-dev-internal-token';

// PostgreSQL Pool
export const db = new Pool({
  connectionString: DATABASE_URL,
});

export let mongoClient: MongoClient;
export let mongoDb: Db;

// Redis Client
export const redis = createClient({
  url: REDIS_URL,
});

redis.on('error', (err) => console.error('Redis Client Error', err));

// Docker Engine API client (reads default socket /var/run/docker.sock)
export const docker = new Docker();

async function initializeMongoSchema() {
  console.log('🌱 Initializing MongoDB identity schema...');

  await mongoDb.collection('users').createIndex({ username: 1 }, { unique: true });
  await mongoDb.collection('users').createIndex({ teamName: 1 }, { unique: true });
  await mongoDb.collection('users').createIndex(
    { teamId: 1 },
    { unique: true, partialFilterExpression: { teamId: { $type: 'string' } } }
  );
  await mongoDb.collection('teams').createIndex({ teamName: 1 }, { unique: true });

  console.log('✅ MongoDB identity schema initialized successfully.');
}

// Initialize database tables programmatically if they do not exist
async function initializeDatabaseSchema(client: any) {
  console.log('🌱 Initializing PostgreSQL benchmark schema programmatically...');
  
  await client.query(`
    CREATE TABLE IF NOT EXISTS submissions (
        id SERIAL PRIMARY KEY,
        team_name VARCHAR(100) NOT NULL,
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

  await client.query(`ALTER TABLE submissions ADD COLUMN IF NOT EXISTS team_name VARCHAR(100);`);
  await client.query(`CREATE INDEX IF NOT EXISTS idx_submissions_team_name ON submissions(team_name);`);
  await client.query(`
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'contestants'
      ) AND EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'submissions' AND column_name = 'contestant_id'
      ) THEN
        UPDATE submissions s
        SET team_name = c.team_name
        FROM contestants c
        WHERE s.team_name IS NULL AND s.contestant_id = c.id;
      END IF;
    END $$;
  `);

  // Check if submissions exist, if not insert default ones
  const subCount = await client.query('SELECT COUNT(*) FROM submissions');
  if (parseInt(subCount.rows[0].count, 10) === 0) {
    await client.query(`
      INSERT INTO submissions (id, team_name, docker_image_tag, status)
      VALUES 
      (1, 'alpha_traders', 'mock-contestant:latest', 'built'),
      (2, 'beta_quant', 'mock-contestant:latest', 'built')
      ON CONFLICT DO NOTHING;
    `);
    // Reset serial sequence
    await client.query("SELECT setval('submissions_id_seq', (SELECT MAX(id) FROM submissions));");
  }

  console.log('✅ PostgreSQL benchmark schema initialized successfully.');
}

// Initialize external connections
export async function initConnections() {
  await redis.connect();
  console.log('✅ Connected to Redis successfully');

  if (!MONGO_URL) {
    throw new Error('MONGO_URL or MONGODB_URI must be configured for persistent identity storage');
  }
  mongoClient = new MongoClient(MONGO_URL);
  await mongoClient.connect();
  mongoDb = mongoClient.db(MONGO_DB_NAME);
  console.log('✅ Connected to MongoDB successfully');
  await initializeMongoSchema();

  const client = await db.connect();
  try {
    console.log('✅ Connected to PostgreSQL successfully');
    await initializeDatabaseSchema(client);
  } finally {
    client.release();
  }
}
