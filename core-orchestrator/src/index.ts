import express, { Request, Response } from 'express';
import cors from 'cors';
import { v4 as uuidv4 } from 'uuid';
import { initConnections, db, mongoDb, redis, BOT_FLEET_URL, INTERNAL_API_TOKEN, JWT_SECRET, PORT, validateStartupConfig } from './config';
import { SandboxService } from './services/sandbox';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import { ObjectId } from 'mongodb';

const app = express();
app.use(cors());
app.use(express.json());

const MAX_SOURCE_BYTES = 1024 * 1024;
const MAX_TPS = 5000;
const MAX_CONCURRENCY = 500;
const MAX_DURATION_SECONDS = 300;
const MIN_PASSWORD_LENGTH = 8;
const ACTIVE_RUNS_SET_KEY = 'runs:active';

function activeRunKey(runId: string) {
  return `run:active:${runId}`;
}

interface AuthPayload {
  userId: string;
  username: string;
  teamId: string;
  teamName: string;
}

interface AuthedRequest extends Request {
  user?: AuthPayload;
}

interface UserDoc {
  _id?: ObjectId;
  username: string;
  passwordHash: string;
  teamName: string;
  teamId: string;
  createdAt: Date;
}

interface TeamDoc {
  _id?: ObjectId;
  teamName: string;
  createdAt: Date;
}

function normalizeHandle(value: string) {
  return value.trim().toLowerCase().replace(/[^a-z0-9_]/g, '');
}

function parseHandle(value: unknown, fieldName: string): { value?: string; error?: string } {
  if (typeof value !== 'string' || value.trim().length === 0) {
    return { error: `${fieldName} is required` };
  }

  const normalized = normalizeHandle(value);
  if (!normalized) {
    return { error: `${fieldName} contains invalid characters. Use letters, numbers, and underscores.` };
  }

  return { value: normalized };
}

function parsePassword(value: unknown, options: { minLength?: number } = {}): { value?: string; error?: string } {
  if (typeof value !== 'string' || value.length === 0) {
    return { error: 'password is required' };
  }

  if (options.minLength && value.length < options.minLength) {
    return { error: `password must be at least ${options.minLength} characters` };
  }

  return { value };
}

function authenticateToken(req: AuthedRequest, res: Response, next: express.NextFunction): any {
  const authHeader = req.headers.authorization;
  const token = authHeader?.startsWith('Bearer ') ? authHeader.slice('Bearer '.length) : undefined;

  if (!token) {
    return res.status(401).json({ error: 'Authentication token is required' });
  }

  try {
    const payload = jwt.verify(token, JWT_SECRET) as Partial<AuthPayload>;
    if (
      typeof payload.userId !== 'string' ||
      typeof payload.username !== 'string' ||
      typeof payload.teamId !== 'string' ||
      typeof payload.teamName !== 'string'
    ) {
      return res.status(401).json({ error: 'Invalid or expired authentication token' });
    }
    req.user = payload as AuthPayload;
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
 * Auth: User Signup (register team and credentials in MongoDB)
 */
app.post('/auth/signup', async (req: Request, res: Response): Promise<any> => {
  const { username, password, team_name } = req.body;

  const parsedUsername = parseHandle(username, 'username');
  const parsedPassword = parsePassword(password, { minLength: MIN_PASSWORD_LENGTH });
  const parsedTeamName = parseHandle(team_name, 'team_name');
  const validationErrors = [parsedUsername.error, parsedPassword.error, parsedTeamName.error].filter(Boolean);
  if (validationErrors.length > 0) {
    return res.status(400).json({ error: validationErrors.join(', ') });
  }

  const cleanUsername = parsedUsername.value!;
  const cleanPassword = parsedPassword.value!;
  const cleanTeamName = parsedTeamName.value!;
  const users = mongoDb.collection<UserDoc>('users');
  const teams = mongoDb.collection<TeamDoc>('teams');

  try {
    const existingUser = await users.findOne({
      $or: [{ username: cleanUsername }, { teamName: cleanTeamName }],
    });
    if (existingUser) {
      const conflict = existingUser.username === cleanUsername
        ? 'Username is already taken'
        : 'Team name is already taken';
      return res.status(409).json({ error: conflict });
    }

    const existingTeam = await teams.findOne({ teamName: cleanTeamName });
    if (existingTeam) {
      return res.status(409).json({ error: 'Team name is already taken' });
    }

    const passwordHash = await bcrypt.hash(cleanPassword, 10);
    const now = new Date();
    const teamResult = await teams.insertOne({ teamName: cleanTeamName, createdAt: now });
    const teamId = teamResult.insertedId.toString();

    let userResult;
    try {
      userResult = await users.insertOne({
        username: cleanUsername,
        passwordHash,
        teamName: cleanTeamName,
        teamId,
        createdAt: now,
      });
    } catch (error) {
      await teams.deleteOne({ _id: teamResult.insertedId }).catch(() => {});
      throw error;
    }

    // Leaderboard setup should not invalidate a committed auth account.
    redis.zAdd('leaderboard', { score: 0, value: cleanTeamName }).catch((err) => {
      console.error(`Failed to initialize leaderboard for ${cleanTeamName}:`, err);
    });

    const token = jwt.sign(
      { userId: userResult.insertedId.toString(), username: cleanUsername, teamId, teamName: cleanTeamName },
      JWT_SECRET,
      { expiresIn: '7d' }
    );

    return res.status(201).json({
      message: 'Signup successful',
      token,
      username: cleanUsername,
      team_name: cleanTeamName,
      team_id: teamId
    });
  } catch (error: any) {
    console.error('Signup error:', error);
    if (error.code === 11000) {
      const keyPattern = error.keyPattern || {};
      const field = keyPattern.teamName || keyPattern.teamId ? 'Team name' : 'Username';
      return res.status(409).json({ error: `${field} is already taken` });
    }
    return res.status(500).json({ error: error.message });
  }
});

/**
 * Auth: User Login
 */
app.post('/auth/login', async (req: Request, res: Response): Promise<any> => {
  const { username, password } = req.body;

  const parsedUsername = parseHandle(username, 'username');
  const parsedPassword = parsePassword(password);
  if (parsedUsername.error || parsedPassword.error) {
    return res.status(400).json({ error: 'username and password are required' });
  }

  try {
    const user = await mongoDb.collection<UserDoc>('users').findOne({ username: parsedUsername.value });
    if (!user || !user._id) {
      return res.status(400).json({ error: 'Invalid username or password' });
    }

    const isValid = await bcrypt.compare(password, user.passwordHash);
    if (!isValid) {
      return res.status(400).json({ error: 'Invalid username or password' });
    }

    const token = jwt.sign(
      { userId: user._id.toString(), username: user.username, teamId: user.teamId, teamName: user.teamName },
      JWT_SECRET,
      { expiresIn: '7d' }
    );

    return res.json({
      message: 'Login successful',
      token,
      username: user.username,
      team_name: user.teamName,
      team_id: user.teamId
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
  const parsedTeamName = parseHandle(req.body.team_name, 'team_name');
  if (parsedTeamName.error) {
    return res.status(400).json({ error: parsedTeamName.error });
  }

  const teamName = parsedTeamName.value!;
  try {
    const teams = mongoDb.collection<TeamDoc>('teams');
    const existing = await teams.findOne({ teamName });
    if (existing?._id) {
      return res.status(200).json({
        id: existing._id.toString(),
        team_id: existing._id.toString(),
        team_name: existing.teamName,
        created_at: existing.createdAt,
      });
    }

    const result = await teams.insertOne({ teamName, createdAt: new Date() });
    await redis.zAdd('leaderboard', { score: 0, value: teamName });

    return res.status(201).json({
      id: result.insertedId.toString(),
      team_id: result.insertedId.toString(),
      team_name: teamName,
    });
  } catch (error: any) {
    console.error(error);
    if (error.code === 11000) {
      return res.status(409).json({ error: 'Team name is already taken' });
    }
    return res.status(500).json({ error: error.message });
  }
});

/**
 * 2. Submit Source Code for Compilation & Sandboxing
 */
app.post('/submissions', authenticateToken, async (req: AuthedRequest, res: Response): Promise<any> => {
  const { team_id, contestant_id, source_code, language = 'go' } = req.body;

  if (!source_code) {
    return res.status(400).json({ error: 'source_code is required' });
  }

  if (Buffer.byteLength(source_code, 'utf8') > MAX_SOURCE_BYTES) {
    return res.status(413).json({ error: 'source_code exceeds the 1MB limit' });
  }

  const requestedTeamId = team_id || contestant_id;
  if (requestedTeamId && String(requestedTeamId) !== req.user!.teamId) {
    return res.status(403).json({ error: 'You can only submit code for your own team' });
  }

  if (language !== 'go' && language !== 'cpp') {
    return res.status(400).json({ error: 'Unsupported language. Supported: go, cpp' });
  }

  try {
    if (!ObjectId.isValid(req.user!.userId)) {
      return res.status(401).json({ error: 'Invalid or expired authentication token' });
    }

    const user = await mongoDb.collection<UserDoc>('users').findOne({ _id: new ObjectId(req.user!.userId) });
    if (!user || user.teamId !== req.user!.teamId || user.teamName !== req.user!.teamName) {
      return res.status(404).json({ error: 'Team not found' });
    }

    const imageTag = `contestant-sub-${uuidv4()}:latest`;
    
    // 2. Insert submission metadata in PG (status: building)
    const subResult = await db.query(
      'INSERT INTO submissions (team_name, docker_image_tag, status) VALUES ($1, $2, $3) RETURNING *',
      [req.user!.teamName, imageTag, 'building']
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
    if (result.rows[0].team_name !== req.user!.teamName) {
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
    // Get submission info owned by the authenticated team
    const subResult = await db.query(
      `SELECT * FROM submissions WHERE id = $1 AND team_name = $2`,
      [submission_id, req.user!.teamName]
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
    // 1. Programmatically start the sandboxed contestant container
    const { sandboxId, endpoint } = await SandboxService.startRun(submission.id, runId);
    activeContainers.set(runId, sandboxId);

    await waitForContestantHealth(endpoint);

    // 2. Track this run independently so concurrent benchmarks do not collide.
    await redis.hSet(activeRunKey(runId), {
      run_id: runId,
      team_name: submission.team_name,
      container_id: sandboxId,
      started_at: new Date().toISOString(),
      expires_at: new Date(Date.now() + (requestedDuration + 30) * 1000).toISOString(),
    });
    await redis.expire(activeRunKey(runId), requestedDuration + 30);
    await redis.sAdd(ACTIVE_RUNS_SET_KEY, runId);

    // 3. Create run entry in PostgreSQL
    await db.query(
      `INSERT INTO benchmark_runs (id, submission_id, status, total_orders_sent, success_rate, p50_latency_ms, p90_latency_ms, p99_latency_ms, avg_tps) 
       VALUES ($1, $2, $3, 0, 0, 0, 0, 0, 0)`,
      [runId, submission.id, 'running']
    );

    // 4. Trigger Go Bot Fleet load generator via HTTP
    const botFleetEndpoint = `${BOT_FLEET_URL}/start`;
    console.log(`Triggering Go Bot Fleet at ${botFleetEndpoint} targeting sandbox endpoint...`);

    const triggerResponse = await fetch(botFleetEndpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        benchmark_run_id: runId,
        target_url: endpoint,
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
      target: endpoint,
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
           WHERE s.team_name = $1 AND br.status = 'completed' AND br.p50_latency_ms > 0
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
      const active = await redis.hGetAll(activeRunKey(runId));
      if (active && active.container_id) {
        containerId = active.container_id;
      }
    } catch (err) {
      console.error('Failed to read active run from Redis:', err);
    }
  }
  if (!containerId) {
    containerId = await SandboxService.findRunId(runId) || undefined;
  }
  if (!containerId) {
    await Promise.all([
      redis.del(activeRunKey(runId)),
      redis.sRem(ACTIVE_RUNS_SET_KEY, runId),
    ]).catch(() => {});
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
    await SandboxService.stopRun(containerId);
  } catch (err) {
    console.error(`Failed to stop container for run ${runId}:`, err);
  }

  // 3. Clear active run flag in Redis
  try {
    await redis.del(activeRunKey(runId));
    await redis.sRem(ACTIVE_RUNS_SET_KEY, runId);
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

// Validate before binding a network port. This protects against accidentally
// launching with insecure or missing authentication material.
validateStartupConfig();

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
