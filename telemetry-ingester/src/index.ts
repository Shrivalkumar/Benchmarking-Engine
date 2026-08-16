import express from 'express';
import http from 'http';
import { WebSocket, WebSocketServer } from 'ws';
import { Kafka } from 'kafkajs';
import { Pool } from 'pg';
import { createClient } from 'redis';

const PORT = process.env.PORT || 8001;
const DATABASE_URL = process.env.DATABASE_URL?.trim();
const INTERNAL_API_TOKEN = process.env.INTERNAL_API_TOKEN?.trim();
const ORCHESTRATOR_URL = process.env.ORCHESTRATOR_URL || 'http://core-orchestrator:8000';
if (!DATABASE_URL || !INTERNAL_API_TOKEN) throw new Error('DATABASE_URL and INTERNAL_API_TOKEN are required');
const redis = createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
const db = new Pool({ connectionString: DATABASE_URL });
const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });
const clients = new Set<WebSocket>();
const ACTIVE_RUNS_SET_KEY = 'runs:active';
const activeRunKey = (runId: string) => `run:active:${runId}`;

interface RunStats { runId: string; teamName: string; startedAtMs: number; totalOrders: number; successCount: number; latencySamplesMs: number[]; windowTotal: number; windowSuccess: number; windowLatencies: number[]; }
interface TelemetryRecord { event_type?: 'order_result' | 'run_complete'; benchmark_run_id: string; latency_ns?: number; is_success?: boolean; expected_orders?: number; }
const runs = new Map<string, RunStats>();

server.on('upgrade', (request, socket, head) => wss.handleUpgrade(request, socket, head, ws => wss.emit('connection', ws, request)));
wss.on('connection', ws => { clients.add(ws); ws.on('close', () => clients.delete(ws)); });
function broadcast(payload: unknown) { const message = JSON.stringify(payload); for (const client of clients) if (client.readyState === WebSocket.OPEN) client.send(message); }
function percentile(values: number[], rank: number) { if (!values.length) return 0; return values[Math.min(values.length - 1, Math.ceil(values.length * rank / 100) - 1)]; }

async function getRun(runId: string) {
  let stats = runs.get(runId);
  if (stats) return stats;
  const active = await redis.hGetAll(activeRunKey(runId));
  let teamName = active.team_name;
  if (!teamName) {
    const result = await db.query(`SELECT s.team_name FROM benchmark_runs br JOIN submissions s ON s.id = br.submission_id WHERE br.id = $1`, [runId]);
    teamName = result.rows[0]?.team_name;
  }
  if (!teamName) throw new Error(`Unknown benchmark run ${runId}`);
  stats = { runId, teamName, startedAtMs: Date.now(), totalOrders: 0, successCount: 0, latencySamplesMs: [], windowTotal: 0, windowSuccess: 0, windowLatencies: [] };
  runs.set(runId, stats);
  return stats;
}

async function flush(stats: RunStats, finalized = false) {
  const window = [...stats.windowLatencies].sort((a, b) => a - b);
  const all = [...stats.latencySamplesMs].sort((a, b) => a - b);
  const p50 = percentile(all, 50), p90 = percentile(all, 90), p99 = percentile(all, 99);
  const successRate = stats.totalOrders ? stats.successCount / stats.totalOrders : 0;
  const avgTps = stats.totalOrders / Math.max(1, (Date.now() - stats.startedAtMs) / 1000);
  const score = Number(((avgTps * successRate) / (p90 + 1)).toFixed(2));
  await db.query(`UPDATE benchmark_runs SET total_orders_sent=$1, success_rate=$2, p50_latency_ms=$3, p90_latency_ms=$4, p99_latency_ms=$5, avg_tps=$6 WHERE id=$7`, [stats.totalOrders, Number((successRate * 100).toFixed(2)), p50, p90, p99, avgTps, stats.runId]);
  broadcast({ type: finalized ? 'telemetry-finalized' : 'telemetry-tick', run_id: stats.runId, team_name: stats.teamName, tps: stats.windowTotal, p50: Number(percentile(window, 50).toFixed(2)), p90: Number(percentile(window, 90).toFixed(2)), p99: Number(percentile(window, 99).toFixed(2)), success_rate: Number(((stats.windowTotal ? stats.windowSuccess / stats.windowTotal : 0) * 100).toFixed(2)), total_orders: stats.totalOrders, composite_score: score });
  stats.windowTotal = 0; stats.windowSuccess = 0; stats.windowLatencies = [];
  if (finalized && score > 0) { const old = await redis.zScore('leaderboard', stats.teamName); if (old === null || score > old) await redis.zAdd('leaderboard', { score, value: stats.teamName }); }
}

async function notifyCompletion(runId: string, failed: boolean) {
  const response = await fetch(`${ORCHESTRATOR_URL}/benchmark/complete`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'x-internal-token': INTERNAL_API_TOKEN! }, body: JSON.stringify({ benchmark_run_id: runId, status: failed ? 'failed' : 'completed' }), signal: AbortSignal.timeout(10_000) });
  if (!response.ok) throw new Error(`orchestrator completion returned ${response.status}`);
}

async function finalize(runId: string, expectedOrders = 0) {
  const stats = await getRun(runId);
  await flush(stats, true);
  runs.delete(runId);
  await notifyCompletion(runId, stats.totalOrders === 0 || (expectedOrders > 0 && stats.totalOrders < expectedOrders));
}

async function verifySchema() {
  await db.query('SELECT team_name FROM submissions LIMIT 0');
  await db.query('SELECT id, total_orders_sent, success_rate, p50_latency_ms, p90_latency_ms, p99_latency_ms, avg_tps FROM benchmark_runs LIMIT 0');
}

async function startConsumer() {
  const consumer = new Kafka({ clientId: 'telemetry-ingester', brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(',') }).consumer({ groupId: 'telemetry-ingester-group' });
  await consumer.connect(); await consumer.subscribe({ topic: 'telemetry-stream', fromBeginning: false });
  await consumer.run({ eachMessage: async ({ message }) => {
    if (!message.value) return;
    let payload: TelemetryRecord;
    try { payload = JSON.parse(message.value.toString()); } catch { console.error('Malformed Kafka telemetry JSON'); return; }
    try {
      if (!payload.benchmark_run_id) throw new Error('benchmark_run_id is required');
      if (payload.event_type === 'run_complete') { await finalize(payload.benchmark_run_id, payload.expected_orders); return; }
      if (typeof payload.latency_ns !== 'number' || typeof payload.is_success !== 'boolean') throw new Error('invalid order telemetry');
      const stats = await getRun(payload.benchmark_run_id);
      const latency = payload.latency_ns / 1_000_000;
      stats.totalOrders++; if (payload.is_success) stats.successCount++; stats.latencySamplesMs.push(latency); stats.windowTotal++; if (payload.is_success) stats.windowSuccess++; stats.windowLatencies.push(latency);
    } catch (error) { console.error('Failed to process Kafka telemetry:', error); }
  }});
}

setInterval(() => { for (const stats of runs.values()) flush(stats).catch(error => console.error('Failed to flush telemetry:', error)); }, 1000);
app.get('/health', (_req, res) => res.json({ status: 'ok' }));
async function start() { await redis.connect(); await verifySchema(); await startConsumer(); server.listen(PORT, () => console.log(`Telemetry Ingester listening on ${PORT}`)); }
start().catch(error => { console.error('Ingester startup failed:', error); process.exit(1); });
