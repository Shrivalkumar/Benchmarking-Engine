import { initWorkerConnections, validateStartupConfig } from './config';
import { BUILD_JOB_LEASE_SECONDS, BUILD_JOB_POLL_MS, BUILD_WORKER_CONCURRENCY, PostgresBuildQueue } from './services/build-queue';
import { SandboxService } from './services/sandbox';

let acceptingJobs = true;

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function workerLoop(queue: PostgresBuildQueue, slot: number) {
  while (acceptingJobs) {
    const job = await queue.claimNext();
    if (!job) {
      await sleep(BUILD_JOB_POLL_MS);
      continue;
    }
    console.log(`[BuildWorker:${slot}] Building submission ${job.submissionId}, attempt ${job.attemptCount}/${job.maxAttempts}`);
    const heartbeat = setInterval(() => {
      queue.heartbeat(job.id).catch((error) => console.error(`[BuildWorker:${slot}] Failed to renew lease for job ${job.id}:`, error));
    }, Math.max(1000, Math.floor((BUILD_JOB_LEASE_SECONDS * 1000) / 3)));
    try {
      const result = await SandboxService.buildSubmissionImage(job.submissionId, job.imageTag, job.sourceCode, job.language);
      await queue.complete(job, result);
      console.log(`[BuildWorker:${slot}] Submission ${job.submissionId} ${result.success ? 'built' : 'failed'}`);
    } catch (error) {
      console.error(`[BuildWorker:${slot}] Build infrastructure error for submission ${job.submissionId}:`, error);
      await queue.retryOrFail(job, error);
    } finally {
      clearInterval(heartbeat);
    }
  }
}

async function startWorker() {
  validateStartupConfig();
  await initWorkerConnections();
  const queue = new PostgresBuildQueue();
  console.log(`🚀 Build worker ${queue.workerId} running with concurrency ${BUILD_WORKER_CONCURRENCY}`);
  await Promise.all(Array.from({ length: BUILD_WORKER_CONCURRENCY }, (_, index) => workerLoop(queue, index + 1)));
}

function stop(signal: string) {
  console.log(`Received ${signal}; build worker will stop claiming new jobs.`);
  acceptingJobs = false;
}

process.on('SIGTERM', () => stop('SIGTERM'));
process.on('SIGINT', () => stop('SIGINT'));

startWorker().catch((error) => {
  console.error('❌ Build worker failed to start:', error);
  process.exit(1);
});
