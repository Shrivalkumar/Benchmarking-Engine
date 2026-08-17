const assert = require('node:assert/strict');
const test = require('node:test');

// The queue module imports runtime configuration. These values only make the
// compiled module loadable; the tests inject the database implementation.
process.env.DATABASE_URL = 'postgresql://test:test@localhost:5432/test';
process.env.JWT_SECRET = 'a'.repeat(32);
process.env.INTERNAL_API_TOKEN = 'b'.repeat(32);
process.env.SANDBOX_BACKEND = 'docker';
process.env.NODE_ENV = 'test';

const { PostgresBuildQueue } = require('../dist/services/build-queue');

class FakeDatabase {
  constructor(jobs) {
    this.jobs = new Map(jobs.map((job) => [job.id, {
      status: 'queued', attemptCount: 0, maxAttempts: 3, availableAt: 0,
      sourceCode: 'package main', language: 'go', leaseExpiresAt: null,
      workerId: null, lastError: null, ...job,
    }]));
    this.submissions = new Map(jobs.map((job) => [job.submissionId, {
      status: 'queued', dockerImageTag: `submission-${job.submissionId}:latest`, buildLogs: null,
    }]));
  }

  async connect() {
    return { query: this.query.bind(this), release() {} };
  }

  async query(sql, params = []) {
    const statement = sql.replace(/\s+/g, ' ').trim();
    if (['BEGIN', 'COMMIT', 'ROLLBACK'].includes(statement)) return { rows: [] };

    if (statement.includes("SET status = 'queued', worker_id = NULL") && statement.includes("WHERE status = 'running'")) {
      for (const job of this.jobs.values()) {
        if (job.status === 'running' && job.leaseExpiresAt < Date.now()) {
          job.status = 'queued';
          job.workerId = null;
          job.leaseExpiresAt = null;
          job.availableAt = 0;
          job.lastError ||= 'Build worker lease expired';
        }
      }
      return { rows: [] };
    }

    if (statement.includes('WITH candidate AS')) {
      const [workerId] = params;
      const candidate = [...this.jobs.values()]
        .filter((job) => job.status === 'queued' && job.availableAt <= Date.now() && job.attemptCount < job.maxAttempts)
        .sort((left, right) => left.id - right.id)[0];
      if (!candidate) return { rows: [] };
      candidate.status = 'running';
      candidate.workerId = workerId;
      candidate.attemptCount += 1;
      candidate.leaseExpiresAt = Date.now() + 360_000;
      return { rows: [{
        id: candidate.id, submission_id: candidate.submissionId, language: candidate.language,
        source_code: candidate.sourceCode, attempt_count: candidate.attemptCount, max_attempts: candidate.maxAttempts,
      }] };
    }

    if (statement.includes("UPDATE submissions SET status = 'building'")) {
      const submission = this.submissions.get(params[0]);
      if (!submission || !['queued', 'building'].includes(submission.status)) return { rows: [] };
      submission.status = 'building';
      submission.buildLogs = null;
      return { rows: [{ docker_image_tag: submission.dockerImageTag }] };
    }

    if (statement.includes("SET lease_expires_at = CURRENT_TIMESTAMP") && statement.includes("worker_id = $2")) {
      const job = this.jobs.get(params[0]);
      if (job && job.workerId === params[1] && job.status === 'running') job.leaseExpiresAt = Date.now() + 360_000;
      return { rows: [] };
    }

    if (statement.includes('SET status = $3, source_code = NULL')) {
      const [jobId, workerId, status, lastError] = params;
      const job = this.jobs.get(jobId);
      if (!job || job.workerId !== workerId || job.status !== 'running') return { rows: [] };
      job.status = status;
      job.sourceCode = null;
      job.workerId = null;
      job.leaseExpiresAt = null;
      job.lastError = lastError;
      return { rows: [{ submission_id: job.submissionId }] };
    }

    if (statement.includes('SET status = $3, source_code = CASE')) {
      const [jobId, workerId, status, terminal, delaySeconds, lastError] = params;
      const job = this.jobs.get(jobId);
      if (!job || job.workerId !== workerId || job.status !== 'running') return { rows: [] };
      job.status = status;
      job.sourceCode = terminal ? null : job.sourceCode;
      job.workerId = null;
      job.leaseExpiresAt = null;
      job.availableAt = terminal ? job.availableAt : Date.now() + delaySeconds * 1000;
      job.lastError = lastError;
      return { rows: [{ submission_id: job.submissionId }] };
    }

    if (statement.includes('SET docker_image_tag = $2, status = $3')) {
      const [submissionId, imageTag, status, logs] = params;
      Object.assign(this.submissions.get(submissionId), { dockerImageTag: imageTag, status, buildLogs: logs });
      return { rows: [] };
    }

    if (statement.includes('UPDATE submissions SET status = $2, build_logs = $3')) {
      const [submissionId, status, logs] = params;
      Object.assign(this.submissions.get(submissionId), { status, buildLogs: logs });
      return { rows: [] };
    }
    throw new Error(`Unexpected query in fake database: ${statement}`);
  }
}

test('two workers claim different queued submissions concurrently', async () => {
  const database = new FakeDatabase([{ id: 1, submissionId: 101 }, { id: 2, submissionId: 102 }]);
  const first = new PostgresBuildQueue(database, 'worker-a');
  const second = new PostgresBuildQueue(database, 'worker-b');

  const [firstJob, secondJob] = await Promise.all([first.claimNext(), second.claimNext()]);

  assert.deepEqual(new Set([firstJob.id, secondJob.id]), new Set([1, 2]));
  assert.equal(database.jobs.get(1).status, 'running');
  assert.equal(database.jobs.get(2).status, 'running');
  assert.equal(database.submissions.get(101).status, 'building');
  assert.equal(database.submissions.get(102).status, 'building');
});

test('a successful build completes the submission and removes queued source', async () => {
  const database = new FakeDatabase([{ id: 3, submissionId: 103, sourceCode: 'package main\nfunc main() {}' }]);
  const queue = new PostgresBuildQueue(database, 'worker-a');
  const job = await queue.claimNext();

  await queue.complete(job, { success: true, imageTag: 'contestant-103:built', logs: 'build complete' });

  assert.equal(database.jobs.get(3).status, 'succeeded');
  assert.equal(database.jobs.get(3).sourceCode, null);
  assert.equal(database.submissions.get(103).status, 'built');
  assert.equal(database.submissions.get(103).dockerImageTag, 'contestant-103:built');
});

test('an infrastructure error is requeued with source retained before the final attempt', async () => {
  const database = new FakeDatabase([{ id: 4, submissionId: 104 }]);
  const queue = new PostgresBuildQueue(database, 'worker-a');
  const job = await queue.claimNext();

  await queue.retryOrFail(job, new Error('registry temporarily unavailable'));

  assert.equal(database.jobs.get(4).status, 'queued');
  assert.equal(database.jobs.get(4).sourceCode, 'package main');
  assert.ok(database.jobs.get(4).availableAt > Date.now());
  assert.equal(database.submissions.get(104).status, 'queued');
});

test('a final infrastructure failure clears source and marks the submission failed', async () => {
  const database = new FakeDatabase([{ id: 5, submissionId: 105, attemptCount: 2, maxAttempts: 3 }]);
  const queue = new PostgresBuildQueue(database, 'worker-a');
  const job = await queue.claimNext();

  await queue.retryOrFail(job, new Error('registry remains unavailable'));

  assert.equal(database.jobs.get(5).status, 'failed');
  assert.equal(database.jobs.get(5).sourceCode, null);
  assert.equal(database.submissions.get(105).status, 'failed');
});

test('an expired lease is recovered and claimed by another worker', async () => {
  const database = new FakeDatabase([{
    id: 6, submissionId: 106, status: 'running', workerId: 'dead-worker', attemptCount: 1,
    leaseExpiresAt: Date.now() - 1_000,
  }]);
  const queue = new PostgresBuildQueue(database, 'replacement-worker');

  const job = await queue.claimNext();

  assert.equal(job.id, 6);
  assert.equal(job.attemptCount, 2);
  assert.equal(database.jobs.get(6).workerId, 'replacement-worker');
});
