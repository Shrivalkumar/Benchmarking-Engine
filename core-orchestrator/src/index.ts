import express, { Request, Response } from 'express';
import cors from 'cors';
import { v4 as uuidv4 } from 'uuid';
import { initConnections, db, redis, BOT_FLEET_URL, INTERNAL_API_TOKEN, JWT_SECRET, PORT } from './config';
import { SandboxService } from './services/sandbox';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';

const app = express();
app.use(cors());
app.use(express.json());

const MAX_SOURCE_BYTES = 1024 * 1024;
const MAX_TPS = 5000;
const MAX_CONCURRENCY = 500;
const MAX_DURATION_SECONDS = 300;

interface AuthPayload {
  userId: number;
  username: string;
  contestantId: number;
  teamName: string;
}

interface AuthedRequest extends Request {
  user?: AuthPayload;
}

function normalizeHandle(value: string) {
  return value.toLowerCase().replace(/[^a-z0-9_]/g, '');
}

function authenticateToken(req: AuthedRequest, res: Response, next: express.NextFunction): any {
  const authHeader = req.headers.authorization;
  const token = authHeader?.startsWith('Bearer ') ? authHeader.slice('Bearer '.length) : undefined;

  if (!token) {
    return res.status(401).json({ error: 'Authentication token is required' });
  }

  try {
    req.user = jwt.verify(token, JWT_SECRET) as AuthPayload;
    return next();
  } catch {
    return res.status(401).json({ error: 'Invalid or expired authentication token' });
  }
}

function requireInternalToken(req: Request, res: Response, next: express.NextFunction): any {
  const token = req.headers['x-internal-token'];
  if (token !== INTERNAL_API_TOKEN) {
    return res.status(401).json({ error: 'Invalid internal token' });
  }
  return next();
}

async function waitForContestantHealth(targetUrl: string, timeoutMs = 15000) {
  const deadline = Date.now() + timeoutMs;
  let lastError = 'health check timed out';

  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${targetUrl}/health`, { signal: AbortSignal.timeout(1000) });
      if (response.ok) {
        return;
      }
      lastError = `health returned HTTP ${response.status}`;
    } catch (error: any) {
      lastError = error.message;
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }

  throw new Error(`Contestant did not become healthy: ${lastError}`);
}

/**
 * Auth: User Signup (register team and credentials in PostgreSQL)
 */
app.post('/auth/signup', async (req: Request, res: Response): Promise<any> => {
  const { username, password, team_name } = req.body;

  if (!username || !password || !team_name) {
    return res.status(400).json({ error: 'username, password, and team_name are required' });
  }

  const cleanUsername = normalizeHandle(username);
  const cleanTeamName = normalizeHandle(team_name);
  if (!cleanUsername) {
    return res.status(400).json({ error: 'username contains invalid characters. Use letters, numbers, and underscores.' });
  }
  if (!cleanTeamName) {
    return res.status(400).json({ error: 'team_name contains invalid characters. Use letters, numbers, and underscores.' });
  }

  const client = await db.connect();
  try {
    await client.query('BEGIN');

    // 1. Check if user or team already has credentials
    const existingUser = await client.query(
      'SELECT id FROM users WHERE username = $1 OR team_name = $2',
      [cleanUsername, cleanTeamName]
    );
    if (existingUser.rows.length > 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Username is already taken' });
    }

    // 2. Create contestant in PostgreSQL
    const pgResult = await client.query(
      'INSERT INTO contestants (team_name) VALUES ($1) ON CONFLICT (team_name) DO NOTHING RETURNING *',
      [cleanTeamName]
    );

    let contestantId: number;
    if (pgResult.rows.length === 0) {
      const existing = await client.query('SELECT id FROM contestants WHERE team_name = $1', [cleanTeamName]);
      contestantId = existing.rows[0].id;
    } else {
      contestantId = pgResult.rows[0].id;
      // Initialize in Redis leaderboard with a starting score of 0
      await redis.zAdd('leaderboard', { score: 0, value: cleanTeamName });
    }

    // 3. Hash password
    const passwordHash = await bcrypt.hash(password, 10);

    // 4. Create user in PostgreSQL
    const userResult = await client.query(
      `INSERT INTO users (username, password_hash, team_name, contestant_id)
       VALUES ($1, $2, $3, $4)
       RETURNING id, username, team_name, contestant_id`,
      [cleanUsername, passwordHash, cleanTeamName, contestantId]
    );
    const newUser = userResult.rows[0];

    await client.query('COMMIT');

    // 5. Generate JWT token
    const token = jwt.sign(
      { userId: newUser.id, username: newUser.username, contestantId, teamName: cleanTeamName },
      JWT_SECRET,
      { expiresIn: '7d' }
    );

    return res.status(201).json({
      message: 'Signup successful',
      token,
      username: newUser.username,
      team_name: cleanTeamName,
      contestant_id: contestantId
    });
  } catch (error: any) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('Signup error:', error);
    return res.status(500).json({ error: error.message });
  } finally {
    client.release();
  }
});

/**
 * Auth: User Login
 */
app.post('/auth/login', async (req: Request, res: Response): Promise<any> => {
  const { username, password } = req.body;

  if (!username || !password) {
    return res.status(400).json({ error: 'username and password are required' });
  }

  try {
    // 1. Find user in PostgreSQL
    const userResult = await db.query(
      'SELECT id, username, password_hash, team_name, contestant_id FROM users WHERE username = $1',
      [normalizeHandle(username)]
    );
    if (userResult.rows.length === 0) {
      return res.status(400).json({ error: 'Invalid username or password' });
    }
    const user = userResult.rows[0];

    // 2. Compare password hash
    const isValid = await bcrypt.compare(password, user.password_hash);
    if (!isValid) {
      return res.status(400).json({ error: 'Invalid username or password' });
    }

    // 3. Generate JWT token
    const token = jwt.sign(
      { userId: user.id, username: user.username, contestantId: user.contestant_id, teamName: user.team_name },
      JWT_SECRET,
      { expiresIn: '7d' }
    );

    return res.json({
      message: 'Login successful',
      token,
      username: user.username,
      team_name: user.team_name,
      contestant_id: user.contestant_id
    });
  } catch (error: any) {
    console.error('Login error:', error);
    return res.status(500).json({ error: error.message });
  }
});

// Memory map to track running container IDs for active benchmark runs
const activeContainers = new Map<string, string>();

/**
 * 1. Register a Contestant Team
 */
app.post('/contestants', async (req: Request, res: Response): Promise<any> => {
  const { team_name } = req.body;
  if (!team_name) {
    return res.status(400).json({ error: 'team_name is required' });
  }

  try {
    const result = await db.query(
      'INSERT INTO contestants (team_name) VALUES ($1) ON CONFLICT (team_name) DO NOTHING RETURNING *',
      [team_name]
    );
    
    // If team existed, fetch it
    if (result.rows.length === 0) {
      const existing = await db.query('SELECT * FROM contestants WHERE team_name = $1', [team_name]);
      return res.status(200).json(existing.rows[0]);
    }

    // Initialize in Redis leaderboard with a starting score of 0
    await redis.zAdd('leaderboard', { score: 0, value: team_name });

    return res.status(201).json(result.rows[0]);
  } catch (error: any) {
    console.error(error);
    return res.status(500).json({ error: error.message });
  }
});

/**
 * 2. Submit Source Code for Compilation & Sandboxing
 */
app.post('/submissions', authenticateToken, async (req: AuthedRequest, res: Response): Promise<any> => {
  const { contestant_id, source_code, language = 'go' } = req.body;

  if (!source_code) {
    return res.status(400).json({ error: 'source_code is required' });
  }

  if (Buffer.byteLength(source_code, 'utf8') > MAX_SOURCE_BYTES) {
    return res.status(413).json({ error: 'source_code exceeds the 1MB limit' });
  }

  if (contestant_id && Number(contestant_id) !== req.user!.contestantId) {
    return res.status(403).json({ error: 'You can only submit code for your own team' });
  }

  if (language !== 'go' && language !== 'cpp') {
    return res.status(400).json({ error: 'Unsupported language. Supported: go, cpp' });
  }

  try {
    // 1. Verify contestant exists
    const contestant = await db.query('SELECT * FROM contestants WHERE id = $1', [req.user!.contestantId]);
    if (contestant.rows.length === 0) {
      return res.status(404).json({ error: 'Contestant not found' });
    }

    const imageTag = `contestant-sub-${uuidv4()}:latest`;
    
    // 2. Insert submission metadata in PG (status: building)
    const subResult = await db.query(
      'INSERT INTO submissions (contestant_id, docker_image_tag, status) VALUES ($1, $2, $3) RETURNING *',
      [req.user!.contestantId, imageTag, 'building']
    );
    const submission = subResult.rows[0];

    // 3. Trigger build in background to avoid blocking REST response
    SandboxService.buildSubmissionImage(submission.id, imageTag, source_code, language)
      .then((buildResult) => {
        console.log(`Build completed for submission ${submission.id} (${language}). Success: ${buildResult.success}`);
      })
      .catch((err) => {
        console.error(`Build crashed for submission ${submission.id}:`, err);
      });

    return res.status(202).json({
      message: 'Submission received. Compilation and sandboxing build triggered.',
      submission_id: submission.id,
      status: 'building',
    });
  } catch (error: any) {
    console.error(error);
    return res.status(500).json({ error: error.message });
  }
});

/**
 * 3. Retrieve Submission Build Status and Logs
 */
app.get('/submissions/:id', authenticateToken, async (req: AuthedRequest, res: Response): Promise<any> => {
  const { id } = req.params;

  try {
    const result = await db.query('SELECT * FROM submissions WHERE id = $1', [id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Submission not found' });
    }
    if (result.rows[0].contestant_id !== req.user!.contestantId) {
      return res.status(403).json({ error: 'You can only view your own submissions' });
    }
    return res.json(result.rows[0]);
  } catch (error: any) {
    return res.status(500).json({ error: error.message });
  }
});

/**
 * 4. Start a Benchmark stress test
 */
app.post('/benchmark/start', authenticateToken, async (req: AuthedRequest, res: Response): Promise<any> => {
  const { submission_id, tps = 500, duration_seconds = 30, concurrency = 10 } = req.body;
  const requestedTps = Number(tps);
  const requestedDuration = Number(duration_seconds);
  const requestedConcurrency = Number(concurrency);
  let pendingRunId: string | null = null;

  if (!submission_id) {
    return res.status(400).json({ error: 'submission_id is required' });
  }

  if (
    !Number.isFinite(requestedTps) ||
    !Number.isFinite(requestedDuration) ||
    !Number.isFinite(requestedConcurrency) ||
    requestedTps < 1 ||
    requestedTps > MAX_TPS ||
    requestedConcurrency < 1 ||
    requestedConcurrency > MAX_CONCURRENCY ||
    requestedDuration < 5 ||
    requestedDuration > MAX_DURATION_SECONDS
  ) {
    return res.status(400).json({
      error: `Invalid run parameters. Limits: tps 1-${MAX_TPS}, concurrency 1-${MAX_CONCURRENCY}, duration 5-${MAX_DURATION_SECONDS}s`,
    });
  }

  try {
    // Check if there is already an active run in Redis to prevent concurrency collisions
    const activeRun = await redis.hGetAll('run:active');
    if (activeRun && activeRun.run_id) {
      return res.status(409).json({ error: 'Another benchmark test is already running. Please wait.' });
    }

    // Get submission and contestant info
    const subResult = await db.query(
      `SELECT s.*, c.team_name FROM submissions s 
       JOIN contestants c ON s.contestant_id = c.id 
       WHERE s.id = $1 AND s.contestant_id = $2`,
      [submission_id, req.user!.contestantId]
    );

    if (subResult.rows.length === 0) {
      return res.status(404).json({ error: 'Submission not found for your team' });
    }

    const submission = subResult.rows[0];
    if (submission.status !== 'built') {
      return res.status(400).json({ error: `Submission is not ready. Status: ${submission.status}` });
    }

    const runId = uuidv4();
    pendingRunId = runId;
    const targetHostname = `contestant-run-${runId}`;

    // 1. Programmatically start the sandboxed contestant container
    const { containerId } = await SandboxService.startContainer(submission.id, runId);
    activeContainers.set(runId, containerId);

    await waitForContestantHealth(`http://${targetHostname}:8080`);

    // 2. Set active run cache in Redis
    await redis.hSet('run:active', {
      run_id: runId,
      team_name: submission.team_name,
      container_id: containerId,
      started_at: new Date().toISOString(),
      expires_at: new Date(Date.now() + (requestedDuration + 30) * 1000).toISOString(),
    });
    await redis.expire('run:active', requestedDuration + 30);

    // 3. Create run entry in PostgreSQL
    await db.query(
      `INSERT INTO benchmark_runs (id, submission_id, status, total_orders_sent, success_rate, p50_latency_ms, p90_latency_ms, p99_latency_ms, avg_tps) 
       VALUES ($1, $2, $3, 0, 0, 0, 0, 0, 0)`,
      [runId, submission.id, 'running']
    );

    // 4. Trigger Go Bot Fleet load generator via HTTP
    const botFleetEndpoint = `${BOT_FLEET_URL}/start`;
    console.log(`Triggering Go Bot Fleet at ${botFleetEndpoint} targeting ${targetHostname}:8080...`);

    const triggerResponse = await fetch(botFleetEndpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        benchmark_run_id: runId,
        target_url: `http://${targetHostname}:8080`,
        duration_seconds: requestedDuration,
        tps: requestedTps,
        concurrency: requestedConcurrency,
      }),
    });

    if (!triggerResponse.ok) {
      const errorText = await triggerResponse.text();
      await cleanupRun(runId, 'bot-fleet-start-failed', 'failed');
      throw new Error(`Failed to start load generator in Bot Fleet: ${errorText}`);
    }

    // 5. Establish safety cleanup timeout (duration + 10 seconds leeway)
    setTimeout(async () => {
      await cleanupRun(runId, 'timeout');
    }, (requestedDuration + 10) * 1000);

    return res.status(202).json({
      message: 'Benchmark test triggered successfully.',
      benchmark_run_id: runId,
      team_name: submission.team_name,
      target: `http://${targetHostname}:8080`,
    });
  } catch (error: any) {
    if (pendingRunId) {
      await cleanupRun(pendingRunId, 'benchmark-start-failed', 'failed');
    }
    console.error('Error starting benchmark:', error);
    return res.status(500).json({ error: error.message });
  }
});

/**
 * 5. Complete Benchmark Run Webhook (Called by Bot Fleet or Ingester)
 */
app.post('/benchmark/complete', requireInternalToken, async (req: Request, res: Response): Promise<any> => {
  const { benchmark_run_id } = req.body;
  if (!benchmark_run_id) {
    return res.status(400).json({ error: 'benchmark_run_id is required' });
  }

  try {
    await cleanupRun(benchmark_run_id, 'completed');
    return res.json({ message: `Benchmark ${benchmark_run_id} cleaned up successfully.` });
  } catch (error: any) {
    console.error('Error completing benchmark:', error);
    return res.status(500).json({ error: error.message });
  }
});

/**
 * 6. Get Current Standings (Redis Leaderboard)
 */
app.get('/leaderboard', async (req: Request, res: Response): Promise<any> => {
  try {
    // Fetch members and their scores sorted high-to-low
    const list = await redis.zRangeWithScores('leaderboard', 0, -1, { REV: true });
    
    // Enrich leaderboard details from PG
    const enrichedList = await Promise.all(
      list.map(async (item) => {
        const teamName = item.value;
        const score = item.score;
        
        // Fetch the single best completed run for this team so displayed metrics are internally consistent.
        const stats = await db.query(
          `SELECT
             avg_tps,
             p50_latency_ms,
             p90_latency_ms,
             p99_latency_ms,
             success_rate
           FROM benchmark_runs br
           JOIN submissions s ON br.submission_id = s.id
           JOIN contestants c ON s.contestant_id = c.id
           WHERE c.team_name = $1 AND br.status = 'completed' AND br.p50_latency_ms > 0
           ORDER BY ((br.avg_tps * (br.success_rate / 100.0)) / (br.p90_latency_ms + 1.0)) DESC
           LIMIT 1`,
          [teamName]
        );
        const bestRun = stats.rows[0];

        return {
          team_name: teamName,
          score: score,
          peak_tps: Number(bestRun?.avg_tps || 0),
          p50_latency: Number(bestRun?.p50_latency_ms || 0),
          p90_latency: Number(bestRun?.p90_latency_ms || 0),
          p99_latency: Number(bestRun?.p99_latency_ms || 0),
          success_rate: Number(bestRun?.success_rate || 0),
        };
      })
    );

    return res.json(enrichedList);
  } catch (error: any) {
    console.error(error);
    return res.status(500).json({ error: error.message });
  }
});

/**
 * Helper to clean up contestant container and active Redis flags
 */
async function cleanupRun(runId: string, triggerSource: string, finalStatus: 'completed' | 'failed' = 'completed') {
  let containerId: string | undefined = activeContainers.get(runId);
  if (!containerId) {
    try {
      const active = await redis.hGetAll('run:active');
      if (active && active.run_id === runId && active.container_id) {
        containerId = active.container_id;
      }
    } catch (err) {
      console.error('Failed to read active run from Redis:', err);
    }
  }
  if (!containerId) {
    containerId = await SandboxService.findContainerIdByRun(runId) || undefined;
  }
  if (!containerId) {
    await redis.hGetAll('run:active')
      .then((active) => {
        if (active?.run_id === runId) {
          return redis.del('run:active');
        }
        return undefined;
      })
      .catch(() => {});
    await db.query(
      `UPDATE benchmark_runs
       SET status = CASE WHEN status = 'running' THEN $2::varchar ELSE status END,
           ended_at = COALESCE(ended_at, CURRENT_TIMESTAMP)
       WHERE id = $1`,
      [runId, finalStatus]
    ).catch(() => {});
    return;
  }

  console.log(`[Cleanup] Cleaning up active run ${runId} triggered by ${triggerSource}`);
  
  // 1. Remove container ID from tracking
  activeContainers.delete(runId);

  // 2. Stop and delete contestant container
  try {
    await SandboxService.stopContainer(containerId);
  } catch (err) {
    console.error(`Failed to stop container for run ${runId}:`, err);
  }

  // 3. Clear active run flag in Redis
  try {
    const active = await redis.hGetAll('run:active');
    if (active && active.run_id === runId) {
      await redis.del('run:active');
    }
  } catch (err) {
    console.error('Failed to clear active run from Redis:', err);
  }

  // 4. Update run status in PostgreSQL
  try {
    await db.query(
      `UPDATE benchmark_runs 
       SET status = CASE WHEN status = 'running' THEN $2::varchar ELSE status END, 
           ended_at = CURRENT_TIMESTAMP 
       WHERE id = $1`,
      [runId, finalStatus]
    );
  } catch (err) {
    console.error('Failed to update benchmark run status in PG:', err);
  }
}

// Start API Server
const PORT_NUM = Number(PORT);
app.listen(PORT_NUM, '0.0.0.0', async () => {
  try {
    await initConnections();
    console.log(`🚀 Core Orchestrator running on http://0.0.0.0:${PORT_NUM}`);
  } catch (err) {
    console.error('❌ Failed to start Core Orchestrator:', err);
    process.exit(1);
  }
});
