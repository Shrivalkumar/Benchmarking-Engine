import express, { Request, Response } from 'express';
import cors from 'cors';
import { v4 as uuidv4 } from 'uuid';
import { initConnections, db, redis, BOT_FLEET_URL, PORT } from './config';
import { SandboxService } from './services/sandbox';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import User from './models/User';

const JWT_SECRET = 'IICPC_STRESS_TESTER_SECRET_KEY_2026';

const app = express();
app.use(cors());
app.use(express.json());

/**
 * Auth: User Signup (register team and credentials in MongoDB + PG)
 */
app.post('/auth/signup', async (req: Request, res: Response): Promise<any> => {
  const { username, password, team_name } = req.body;

  if (!username || !password || !team_name) {
    return res.status(400).json({ error: 'username, password, and team_name are required' });
  }

  const cleanTeamName = team_name.toLowerCase().replace(/[^a-z0-9_]/g, '');
  if (!cleanTeamName) {
    return res.status(400).json({ error: 'team_name contains invalid characters. Use letters, numbers, and underscores.' });
  }

  try {
    // 1. Check if user already exists in MongoDB
    const existingUser = await User.findOne({ username: username.toLowerCase() });
    if (existingUser) {
      return res.status(400).json({ error: 'Username is already taken' });
    }

    // 2. Create contestant in PostgreSQL
    const pgResult = await db.query(
      'INSERT INTO contestants (team_name) VALUES ($1) ON CONFLICT (team_name) DO NOTHING RETURNING *',
      [cleanTeamName]
    );

    let contestantId: number;
    if (pgResult.rows.length === 0) {
      const existing = await db.query('SELECT id FROM contestants WHERE team_name = $1', [cleanTeamName]);
      contestantId = existing.rows[0].id;
    } else {
      contestantId = pgResult.rows[0].id;
      // Initialize in Redis leaderboard with a starting score of 0
      await redis.zAdd('leaderboard', { score: 0, value: cleanTeamName });
    }

    // 3. Hash password
    const passwordHash = await bcrypt.hash(password, 10);

    // 4. Create user in MongoDB
    const newUser = await User.create({
      username: username.toLowerCase(),
      passwordHash,
      teamName: cleanTeamName,
      contestantId
    });

    // 5. Generate JWT token
    const token = jwt.sign(
      { userId: newUser._id, username: newUser.username, contestantId, teamName: cleanTeamName },
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
    console.error('Signup error:', error);
    return res.status(500).json({ error: error.message });
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
    // 1. Find user in MongoDB
    const user = await User.findOne({ username: username.toLowerCase() });
    if (!user) {
      return res.status(400).json({ error: 'Invalid username or password' });
    }

    // 2. Compare password hash
    const isValid = await bcrypt.compare(password, user.passwordHash);
    if (!isValid) {
      return res.status(400).json({ error: 'Invalid username or password' });
    }

    // 3. Generate JWT token
    const token = jwt.sign(
      { userId: user._id, username: user.username, contestantId: user.contestantId, teamName: user.teamName },
      JWT_SECRET,
      { expiresIn: '7d' }
    );

    return res.json({
      message: 'Login successful',
      token,
      username: user.username,
      team_name: user.teamName,
      contestant_id: user.contestantId
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
app.post('/submissions', async (req: Request, res: Response): Promise<any> => {
  const { contestant_id, source_code, language = 'go' } = req.body;

  if (!contestant_id || !source_code) {
    return res.status(400).json({ error: 'contestant_id and source_code are required' });
  }

  if (language !== 'go' && language !== 'cpp') {
    return res.status(400).json({ error: 'Unsupported language. Supported: go, cpp' });
  }

  try {
    // 1. Verify contestant exists
    const contestant = await db.query('SELECT * FROM contestants WHERE id = $1', [contestant_id]);
    if (contestant.rows.length === 0) {
      return res.status(404).json({ error: 'Contestant not found' });
    }

    const imageTag = `contestant-sub-temp:${uuidv4()}`;
    
    // 2. Insert submission metadata in PG (status: building)
    const subResult = await db.query(
      'INSERT INTO submissions (contestant_id, docker_image_tag, status) VALUES ($1, $2, $3) RETURNING *',
      [contestant_id, imageTag, 'building']
    );
    const submission = subResult.rows[0];

    // 3. Trigger build in background to avoid blocking REST response
    SandboxService.buildSubmissionImage(submission.id, source_code, language)
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
app.get('/submissions/:id', async (req: Request, res: Response): Promise<any> => {
  const { id } = req.params;

  try {
    const result = await db.query('SELECT * FROM submissions WHERE id = $1', [id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Submission not found' });
    }
    return res.json(result.rows[0]);
  } catch (error: any) {
    return res.status(500).json({ error: error.message });
  }
});

/**
 * 4. Start a Benchmark stress test
 */
app.post('/benchmark/start', async (req: Request, res: Response): Promise<any> => {
  const { submission_id, tps = 500, duration_seconds = 30, concurrency = 10 } = req.body;

  if (!submission_id) {
    return res.status(400).json({ error: 'submission_id is required' });
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
       WHERE s.id = $1`,
      [submission_id]
    );

    if (subResult.rows.length === 0) {
      return res.status(404).json({ error: 'Submission not found' });
    }

    const submission = subResult.rows[0];
    if (submission.status !== 'built') {
      return res.status(400).json({ error: `Submission is not ready. Status: ${submission.status}` });
    }

    const runId = uuidv4();
    const targetHostname = `contestant-run-${runId}`;

    // 1. Programmatically start the sandboxed contestant container
    const { containerId } = await SandboxService.startContainer(submission.id, runId);
    activeContainers.set(runId, containerId);

    // 2. Set active run cache in Redis
    await redis.hSet('run:active', {
      run_id: runId,
      team_name: submission.team_name,
      started_at: new Date().toISOString(),
    });

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
        duration_seconds: Number(duration_seconds),
        tps: Number(tps),
        concurrency: Number(concurrency),
      }),
    });

    if (!triggerResponse.ok) {
      const errorText = await triggerResponse.text();
      throw new Error(`Failed to start load generator in Bot Fleet: ${errorText}`);
    }

    // 5. Establish safety cleanup timeout (duration + 10 seconds leeway)
    setTimeout(async () => {
      await cleanupRun(runId, 'timeout');
    }, (Number(duration_seconds) + 10) * 1000);

    return res.status(202).json({
      message: 'Benchmark test triggered successfully.',
      benchmark_run_id: runId,
      team_name: submission.team_name,
      target: `http://${targetHostname}:8080`,
    });
  } catch (error: any) {
    console.error('Error starting benchmark:', error);
    return res.status(500).json({ error: error.message });
  }
});

/**
 * 5. Complete Benchmark Run Webhook (Called by Bot Fleet or Ingester)
 */
app.post('/benchmark/complete', async (req: Request, res: Response): Promise<any> => {
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
        
        // Fetch peak metrics for team (ignoring legacy 0ms corrupted runs)
        const stats = await db.query(
          `SELECT 
             MAX(avg_tps) as max_tps, 
             MIN(p50_latency_ms) as min_p50,
             MIN(p90_latency_ms) as min_p90,
             MIN(p99_latency_ms) as min_p99, 
             MAX(success_rate) as max_success
           FROM benchmark_runs br
           JOIN submissions s ON br.submission_id = s.id
           JOIN contestants c ON s.contestant_id = c.id
           WHERE c.team_name = $1 AND br.status = 'completed' AND br.p50_latency_ms > 0`,
          [teamName]
        );

        return {
          team_name: teamName,
          score: score,
          peak_tps: Number(stats.rows[0]?.max_tps || 0),
          p50_latency: Number(stats.rows[0]?.min_p50 || 0),
          p90_latency: Number(stats.rows[0]?.min_p90 || 0),
          p99_latency: Number(stats.rows[0]?.min_p99 || 0),
          success_rate: Number(stats.rows[0]?.max_success || 0),
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
async function cleanupRun(runId: string, triggerSource: string) {
  const containerId = activeContainers.get(runId);
  if (!containerId) {
    return; // Already cleaned up
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
       SET status = CASE WHEN status = 'running' THEN 'completed'::varchar ELSE status END, 
           ended_at = CURRENT_TIMESTAMP 
       WHERE id = $1`,
      [runId]
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
