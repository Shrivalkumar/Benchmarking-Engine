import fs from 'fs';
import path from 'path';
import { exec } from 'child_process';
import { randomUUID } from 'crypto';
import Docker from 'dockerode';
import { BENCHMARK_NET, db, NODE_ENV, SANDBOX_BACKEND } from '../config';
import { KubernetesSandboxBackend } from './kubernetes-sandbox';

export type SubmissionLanguage = 'go' | 'cpp';

export interface BuildResult {
  success: boolean;
  imageTag: string;
  logs: string;
}

export interface SandboxRun {
  sandboxId: string;
  endpoint: string;
}

export interface SandboxBackend {
  buildSubmissionImage(submissionId: number, imageTag: string, sourceCode: string, language: SubmissionLanguage): Promise<BuildResult>;
  startRun(submissionId: number, runId: string): Promise<SandboxRun>;
  stopRun(sandboxId: string): Promise<void>;
  findRunId(runId: string): Promise<string | null>;
}

// This client is scoped to the local-only Docker backend. Production images
// have no Docker CLI or socket mount, and production config rejects this mode.
const docker = new Docker({ socketPath: process.env.DOCKER_SOCKET || '/var/run/docker.sock' });

class DockerSandboxBackend {
  /**
   * Programmatically builds a Docker image for a specific submission.
   * Compiles source code in an isolated Docker build context.
   */
  static async buildSubmissionImage(
    submissionId: number,
    imageTag: string,
    sourceCode: string,
    language: SubmissionLanguage
  ): Promise<BuildResult> {
    const buildDir = path.join(__dirname, `../../temp_builds/sub-${submissionId}-${randomUUID()}`);
    
    // Ensure build directory exists
    fs.mkdirSync(buildDir, { recursive: true });

    let filename = 'main.go';
    let dockerfileContent = '';

    if (language === 'go') {
      filename = 'main.go';
      dockerfileContent = `
FROM golang:1.20-alpine AS builder
WORKDIR /app
COPY main.go .
RUN go env -w GOPROXY=https://goproxy.cn,direct || true
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o matching-engine main.go

FROM alpine:3.18
WORKDIR /app
COPY --from=builder /app/matching-engine .
EXPOSE 8080
CMD ["./matching-engine"]
`;
    } else {
      filename = 'main.cpp';
      dockerfileContent = `
FROM benchmarking-cpp-builder:latest AS builder
WORKDIR /app
COPY main.cpp .
RUN g++ -O3 -std=c++17 -o matching-engine main.cpp -pthread

FROM alpine:3.18
RUN apk add --no-cache libstdc++
WORKDIR /app
COPY --from=builder /app/matching-engine .
EXPOSE 8080
CMD ["./matching-engine"]
`;
    }

    // Write source code
    fs.writeFileSync(path.join(buildDir, filename), sourceCode);

    // Create a secure multi-stage Dockerfile
    fs.writeFileSync(path.join(buildDir, 'Dockerfile'), dockerfileContent);

    return new Promise((resolve) => {
      console.log(`Building Docker image ${imageTag} in ${buildDir}...`);
      
      exec(`docker build -t ${imageTag} .`, { cwd: buildDir, timeout: 5 * 60 * 1000, maxBuffer: 5 * 1024 * 1024 }, (error, stdout, stderr) => {
        const logs = stdout + '\n' + stderr;
        const success = !error;

        // Clean up build files (optional, but good practice)
        try {
          fs.rmSync(buildDir, { recursive: true, force: true });
        } catch (cleanupErr) {
          console.error('Failed to clean up build dir:', cleanupErr);
        }

        resolve({
          success,
          imageTag,
          logs,
        });
      });
    });
  }

  /**
   * Spawns a sandboxed container for a specific contestant's submission.
   * Limits memory to 512MB and CPU to 1 core, attached to benchmarking-net.
   */
  static async startRun(submissionId: number, runId: string): Promise<SandboxRun> {
    const imageResult = await db.query(
      'SELECT docker_image_tag FROM submissions WHERE id = $1 AND status = $2',
      [submissionId, 'built']
    );
    if (imageResult.rows.length === 0) {
      throw new Error(`Submission ${submissionId} is not built or does not exist`);
    }

    const imageTag = imageResult.rows[0].docker_image_tag;
    const hostname = `contestant-run-${runId}`;

    console.log(`Spawning sandboxed container ${hostname} using image ${imageTag}...`);

    // Clean up any pre-existing container with the same name
    try {
      const existingContainer = docker.getContainer(hostname);
      await existingContainer.stop();
      await existingContainer.remove();
      console.log(`Removed pre-existing container: ${hostname}`);
    } catch (e) {
      // Ignored if container doesn't exist
    }

    // Create the container with cgroup constraints
    const container = await docker.createContainer({
      Image: imageTag,
      name: hostname,
      ExposedPorts: {
        '8080/tcp': {},
      },
      Labels: {
        'benchmarking.platform': 'true',
        'benchmarking.run_id': runId,
        'benchmarking.submission_id': String(submissionId),
      },
      HostConfig: {
        // Attach to the isolated bridge network
        NetworkMode: BENCHMARK_NET,
        // Memory limit: 512MB
        Memory: 512 * 1024 * 1024,
        MemorySwap: 512 * 1024 * 1024, // No swap to disk allowed
        // CPU limit: 1 core (represented by NanoCpus: 1,000,000,000)
        NanoCpus: 1000000000,
        PidsLimit: 256,
        ReadonlyRootfs: true,
        CapDrop: ['ALL'],
        SecurityOpt: ['no-new-privileges:true'],
        // Security protections: Read-only root filesystem where possible
        // (For alpine, we mount a tmpfs on /tmp for scratch space)
        Tmpfs: {
          '/tmp': 'rw,noexec,nosuid,size=65536k',
        },
        LogConfig: {
          Type: 'json-file',
          Config: {
            'max-size': '10m',
            'max-file': '2',
          },
        },
      },
    });

    // Start the container
    await container.start();
    console.log(`Successfully started sandboxed container ${hostname} (ID: ${container.id})`);

    return {
      sandboxId: container.id,
      endpoint: `http://${hostname}:8080`,
    };
  }

  /**
   * Stops and removes a running sandboxed container.
   */
  static async stopRun(sandboxId: string): Promise<void> {
    const container = docker.getContainer(sandboxId);

    try {
      console.log(`Stopping container ${sandboxId}...`);
      // Stop the container (timeout after 5 seconds, then SIGKILL)
      await container.stop({ t: 5 });
    } catch (error: any) {
      if (error.statusCode !== 304 && error.statusCode !== 404) {
        console.error(`Error stopping container ${sandboxId}:`, error);
      }
    }

    try {
      console.log(`Removing container ${sandboxId}...`);
      await container.remove({ force: true });
      console.log(`Successfully removed container ${sandboxId}`);
    } catch (error: any) {
      if (error.statusCode !== 404) {
        console.error(`Error removing container ${sandboxId}:`, error);
        throw error;
      }
    }
  }

  static async findRunId(runId: string): Promise<string | null> {
    const containers = await docker.listContainers({
      all: true,
      filters: {
        label: [`benchmarking.run_id=${runId}`],
      },
    });

    return containers[0]?.Id || null;
  }
}

const dockerBackend: SandboxBackend = {
  buildSubmissionImage: DockerSandboxBackend.buildSubmissionImage.bind(DockerSandboxBackend),
  startRun: DockerSandboxBackend.startRun.bind(DockerSandboxBackend),
  stopRun: DockerSandboxBackend.stopRun.bind(DockerSandboxBackend),
  findRunId: DockerSandboxBackend.findRunId.bind(DockerSandboxBackend),
};

const kubernetesBackend: SandboxBackend = new KubernetesSandboxBackend();

function backend(): SandboxBackend {
  if (SANDBOX_BACKEND === 'docker') {
    if (NODE_ENV === 'production') {
      throw new Error('Docker sandbox backend is not available in production');
    }
    return dockerBackend;
  }
  return kubernetesBackend;
}

/** Backend-neutral facade used by API and lifecycle code. */
export class SandboxService {
  static buildSubmissionImage(...args: Parameters<SandboxBackend['buildSubmissionImage']>) {
    return backend().buildSubmissionImage(...args);
  }

  static startRun(...args: Parameters<SandboxBackend['startRun']>) {
    return backend().startRun(...args);
  }

  static stopRun(...args: Parameters<SandboxBackend['stopRun']>) {
    return backend().stopRun(...args);
  }

  static findRunId(...args: Parameters<SandboxBackend['findRunId']>) {
    return backend().findRunId(...args);
  }
}
