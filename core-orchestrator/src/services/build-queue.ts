import { randomUUID } from 'crypto';
import type { PoolClient } from 'pg';
import { db } from '../config';
import type { BuildResult, SubmissionLanguage } from './sandbox';

export const BUILD_WORKER_CONCURRENCY = positiveInteger('BUILD_WORKER_CONCURRENCY', 2);
export const BUILD_JOB_MAX_ATTEMPTS = positiveInteger('BUILD_JOB_MAX_ATTEMPTS', 3);
export const BUILD_JOB_LEASE_SECONDS = positiveInteger('BUILD_JOB_LEASE_SECONDS', 360);
export const BUILD_JOB_POLL_MS = positiveInteger('BUILD_JOB_POLL_MS', 1000);

function positiveInteger(name: string, fallback: number) {
  const value = Number(process.env[name] || fallback);
  return Number.isInteger(value) && value > 0 ? value : fallback;
}

export interface BuildJob {
  id: number;
  submissionId: number;
  imageTag: string;
  language: SubmissionLanguage;
  sourceCode: string;
  attemptCount: number;
  maxAttempts: number;
}

export interface BuildQueue {
  workerId: string;
  claimNext(): Promise<BuildJob | null>;
  heartbeat(jobId: number): Promise<void>;
  complete(job: BuildJob, result: BuildResult): Promise<void>;
  retryOrFail(job: BuildJob, error: unknown): Promise<void>;
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

function retryDelaySeconds(attempt: number) {
  return Math.min(60, 5 * 2 ** Math.max(0, attempt - 1));
}

export class PostgresBuildQueue implements BuildQueue {
  readonly workerId = `build-worker-${randomUUID()}`;

  async claimNext(): Promise<BuildJob | null> {
    const client = await db.connect();
    try {
      await client.query('BEGIN');
      // Releasing expired leases makes worker crashes recoverable without a
      // leader-election process. Attempts are counted only when reclaimed.
      await client.query(`
        UPDATE submission_build_jobs
        SET status = 'queued', worker_id = NULL, lease_expires_at = NULL,
            available_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP,
            last_error = COALESCE(last_error, 'Build worker lease expired')
        WHERE status = 'running' AND lease_expires_at < CURRENT_TIMESTAMP
      `);
      const result = await client.query(`
        WITH candidate AS (
          SELECT id
          FROM submission_build_jobs
          WHERE status = 'queued'
            AND available_at <= CURRENT_TIMESTAMP
            AND attempt_count < max_attempts
          ORDER BY available_at, id
          FOR UPDATE SKIP LOCKED
          LIMIT 1
        )
        UPDATE submission_build_jobs job
        SET status = 'running', worker_id = $1,
            attempt_count = attempt_count + 1,
            started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
            lease_expires_at = CURRENT_TIMESTAMP + ($2 * interval '1 second'),
            updated_at = CURRENT_TIMESTAMP
        FROM candidate
        WHERE job.id = candidate.id
        RETURNING job.id, job.submission_id, job.language, job.source_code,
                  job.attempt_count, job.max_attempts
      `, [this.workerId, BUILD_JOB_LEASE_SECONDS]);
      if (result.rows.length === 0) {
        await client.query('COMMIT');
        return null;
      }
      const row = result.rows[0];
      const submission = await client.query(
        `UPDATE submissions SET status = 'building', build_logs = NULL
         WHERE id = $1 AND status IN ('queued', 'building')
         RETURNING docker_image_tag`,
        [row.submission_id]
      );
      if (submission.rows.length === 0 || !row.source_code) {
        throw new Error(`Build job ${row.id} cannot be claimed because its submission or source is unavailable`);
      }
      await client.query('COMMIT');
      return {
        id: Number(row.id), submissionId: Number(row.submission_id), imageTag: submission.rows[0].docker_image_tag,
        language: row.language, sourceCode: row.source_code,
        attemptCount: Number(row.attempt_count), maxAttempts: Number(row.max_attempts),
      };
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async heartbeat(jobId: number) {
    await db.query(`
      UPDATE submission_build_jobs
      SET lease_expires_at = CURRENT_TIMESTAMP + ($3 * interval '1 second'), updated_at = CURRENT_TIMESTAMP
      WHERE id = $1 AND worker_id = $2 AND status = 'running'
    `, [jobId, this.workerId, BUILD_JOB_LEASE_SECONDS]);
  }

  async complete(job: BuildJob, result: BuildResult) {
    await this.withTransaction(async (client) => {
      const terminalStatus = result.success ? 'built' : 'failed';
      const jobStatus = result.success ? 'succeeded' : 'failed';
      const update = await client.query(`
        UPDATE submission_build_jobs
        SET status = $3, source_code = NULL, worker_id = NULL, lease_expires_at = NULL,
            completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP, last_error = $4
        WHERE id = $1 AND worker_id = $2 AND status = 'running'
        RETURNING submission_id
      `, [job.id, this.workerId, jobStatus, result.success ? null : result.logs]);
      if (update.rows.length === 0) return;
      await client.query(`
        UPDATE submissions
        SET docker_image_tag = $2, status = $3, build_logs = $4
        WHERE id = $1
      `, [job.submissionId, result.imageTag, terminalStatus, result.logs]);
    });
  }

  async retryOrFail(job: BuildJob, error: unknown) {
    const message = errorMessage(error).slice(0, 10000);
    await this.withTransaction(async (client) => {
      const terminal = job.attemptCount >= job.maxAttempts;
      const update = await client.query(`
        UPDATE submission_build_jobs
        SET status = $3, source_code = CASE WHEN $4 THEN NULL ELSE source_code END,
            worker_id = NULL, lease_expires_at = NULL,
            available_at = CASE WHEN $4 THEN available_at ELSE CURRENT_TIMESTAMP + ($5 * interval '1 second') END,
            completed_at = CASE WHEN $4 THEN CURRENT_TIMESTAMP ELSE NULL END,
            updated_at = CURRENT_TIMESTAMP, last_error = $6
        WHERE id = $1 AND worker_id = $2 AND status = 'running'
        RETURNING submission_id
      `, [job.id, this.workerId, terminal ? 'failed' : 'queued', terminal, retryDelaySeconds(job.attemptCount), message]);
      if (update.rows.length === 0) return;
      await client.query(`
        UPDATE submissions
        SET status = $2, build_logs = $3
        WHERE id = $1
      `, [job.submissionId, terminal ? 'failed' : 'queued', terminal ? message : `Build worker error; retrying: ${message}`]);
    });
  }

  private async withTransaction(action: (client: PoolClient) => Promise<void>) {
    const client = await db.connect();
    try {
      await client.query('BEGIN');
      await action(client);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }
}
