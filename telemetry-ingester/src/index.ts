import express from 'express';
import http from 'http';
import { WebSocket, WebSocketServer } from 'ws';
import { Kafka } from 'kafkajs';
import { Pool } from 'pg';
import { createClient } from 'redis';

// Configuration
const PORT = process.env.PORT || 8001;
const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/benchmarking';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const KAFKA_BROKERS = process.env.KAFKA_BROKERS || 'localhost:9092';

const app = express();
const server = http.createServer(app);

// Databases & Redis
const db = new Pool({ connectionString: DATABASE_URL });
const redis = createClient({ url: REDIS_URL });
redis.on('error', (err) => console.error('Redis Client Error', err));

// WebSockets Server
const wss = new WebSocketServer({ noServer: true });

// Active benchmark run stats in memory
interface RunStats {
  runId: string;
  teamName: string;
  totalOrders: number;
  successCount: number;
  cumulativeLatencyNs: number;
  // Sliding 1-second window variables
  windowTotal: number;
  windowSuccess: number;
  windowLatencies: number[]; // stored in milliseconds
}

const runStatsMap = new Map<string, RunStats>();
const wsClients = new Set<WebSocket>();

// Handle Upgrade for WebSockets
server.on('upgrade', (request, socket, head) => {
  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit('connection', ws, request);
  });
});

wss.on('connection', (ws: WebSocket) => {
  wsClients.add(ws);
  console.log(`🔌 Client connected to telemetry socket. Active clients: ${wsClients.size}`);

  ws.on('close', () => {
    wsClients.delete(ws);
    console.log(`🔌 Client disconnected. Active clients: ${wsClients.size}`);
  });
});

/**
 * Broadcasts JSON metrics to all connected WebSocket clients
 */
function broadcast(payload: any) {
  const message = JSON.stringify(payload);
  for (const client of wsClients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  }
}

/**
 * Main Telemetry Calculation Pipeline (Runs every 1 second)
 */
setInterval(async () => {
  if (runStatsMap.size === 0) return;

  for (const [runId, stats] of runStatsMap.entries()) {
    // 1. Calculate TPS (transactions in this 1s window)
    const tps = stats.windowTotal;

    // 2. Sort window latencies to compute percentiles
    const latencies = [...stats.windowLatencies].sort((a, b) => a - b);
    const count = latencies.length;

    let p50 = 0;
    let p90 = 0;
    let p99 = 0;

    if (count > 0) {
      p50 = latencies[Math.floor(count * 0.50)];
      p90 = latencies[Math.floor(count * 0.90)];
      p99 = latencies[Math.floor(count * 0.99)];
    }

    const windowSuccessRate = stats.windowTotal > 0 ? stats.windowSuccess / stats.windowTotal : 0;
    const overallSuccessRate = stats.totalOrders > 0 ? stats.successCount / stats.totalOrders : 0;

    // 3. Compute Composite Score for Leaderboard
    // Formula: (TPS * WindowSuccessRate) / (p90_latency_ms + 1.0)
    const compositeScore = Number(((tps * windowSuccessRate) / (p90 + 1.0)).toFixed(2));

    // Update Redis Sorted Set for the Leaderboard
    try {
      // Only write positive scores to maintain standard sorting
      if (compositeScore > 0) {
        await redis.zAdd('leaderboard', { score: compositeScore, value: stats.teamName });
      }
    } catch (err) {
      console.error('Failed to update Redis leaderboard:', err);
    }

    // 4. Update Postgres metadata dynamically
    try {
      await db.query(
        `UPDATE benchmark_runs 
         SET total_orders_sent = $1, 
             success_rate = $2, 
             p50_latency_ms = $3, 
             p90_latency_ms = $4, 
             p99_latency_ms = $5, 
             avg_tps = $6
         WHERE id = $7`,
        [stats.totalOrders, overallSuccessRate, p50, p90, p99, tps, runId]
      );
    } catch (err) {
      console.error('Failed to update benchmark run in Postgres:', err);
    }

    // 5. Broadcast live metrics over WebSocket
    const tickData = {
      type: 'telemetry-tick',
      run_id: runId,
      team_name: stats.teamName,
      tps: tps,
      p50: Number(p50.toFixed(2)),
      p90: Number(p90.toFixed(2)),
      p99: Number(p99.toFixed(2)),
      success_rate: Number((windowSuccessRate * 100).toFixed(2)),
      total_orders: stats.totalOrders,
      composite_score: compositeScore,
    };
    broadcast(tickData);

    // 6. Reset the 1-second window stats
    stats.windowTotal = 0;
    stats.windowSuccess = 0;
    stats.windowLatencies = [];
  }
}, 1000);

/**
 * Clean up idle telemetry trackers
 * If a run has not received any order metric for 5 seconds, remove it from active memory.
 */
setInterval(() => {
  const now = Date.now();
  // We can track last update time on stats if needed, or simply clean up runs
  // that are no longer marked as "active" in Redis.
  redis.hGetAll('run:active').then((activeRun) => {
    for (const runId of runStatsMap.keys()) {
      if (!activeRun || activeRun.run_id !== runId) {
        console.log(`[Telemetry] Finalizing and cleaning memory tracker for run ${runId}`);
        runStatsMap.delete(runId);
      }
    }
  }).catch(err => console.error('Error auto-cleaning run:active stats:', err));
}, 5000);

/**
 * Initialize Kafka Telemetry Stream Consumer
 */
async function startKafkaConsumer() {
  const kafka = new Kafka({
    clientId: 'telemetry-ingester',
    brokers: KAFKA_BROKERS.split(','),
  });

  const consumer = kafka.consumer({ groupId: 'telemetry-ingester-group' });

  await consumer.connect();
  console.log('✅ Telemetry Ingester connected to Kafka');

  await consumer.subscribe({ topic: 'telemetry-stream', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if (!message.value) return;

      try {
        const payload = JSON.parse(message.value.toString());
        const { benchmark_run_id, order_id, type, latency_ns, status_code, is_success, timestamp } = payload;

        // Fetch or create memory stats object
        let stats = runStatsMap.get(benchmark_run_id);
        if (!stats) {
          // If not in memory, fetch team name from Redis active run or PG
          let teamName = 'unknown_team';
          const activeRun = await redis.hGetAll('run:active');
          if (activeRun && activeRun.run_id === benchmark_run_id) {
            teamName = activeRun.team_name;
          } else {
            const dbRun = await db.query(
              `SELECT c.team_name FROM benchmark_runs br 
               JOIN submissions s ON br.submission_id = s.id 
               JOIN contestants c ON s.contestant_id = c.id 
               WHERE br.id = $1`,
              [benchmark_run_id]
            );
            if (dbRun.rows.length > 0) {
              teamName = dbRun.rows[0].team_name;
            }
          }

          stats = {
            runId: benchmark_run_id,
            teamName,
            totalOrders: 0,
            successCount: 0,
            cumulativeLatencyNs: 0,
            windowTotal: 0,
            windowSuccess: 0,
            windowLatencies: [],
          };
          runStatsMap.set(benchmark_run_id, stats);
          console.log(`[Telemetry] Initialized telemetry listener for run ${benchmark_run_id} (${teamName})`);
        }

        // Convert latency from nanoseconds to milliseconds
        const latencyMs = latency_ns / 1_000_000;

        // Update overall aggregates
        stats.totalOrders++;
        if (is_success) stats.successCount++;
        stats.cumulativeLatencyNs += latency_ns;

        // Update current 1-second window metrics
        stats.windowTotal++;
        if (is_success) stats.windowSuccess++;
        stats.windowLatencies.push(latencyMs);

      } catch (err) {
        console.error('Error parsing Kafka telemetry message:', err);
      }
    },
  });
}

// Start Server
async function startServer() {
  await redis.connect();
  console.log('✅ Connected to Redis successfully');
  
  const client = await db.connect();
  try {
    console.log('✅ Connected to PostgreSQL successfully');
  } finally {
    client.release();
  }

  await startKafkaConsumer();

  server.listen(PORT, () => {
    console.log(`🚀 Telemetry Ingester WebSocket Server running on http://localhost:${PORT}`);
  });
}

startServer().catch((err) => {
  console.error('❌ Ingester startup failed:', err);
  process.exit(1);
});
