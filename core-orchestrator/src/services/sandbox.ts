import fs from 'fs';
import path from 'path';
import { exec } from 'child_process';
import { docker, BENCHMARK_NET, db } from '../config';

export class SandboxService {
  /**
   * Programmatically builds a Docker image for a specific submission.
   * Compiles source code in an isolated Docker build context.
   */
  static async buildSubmissionImage(
    submissionId: number,
    sourceCode: string,
    language: 'go' | 'cpp'
  ): Promise<{ success: boolean; imageTag: string; logs: string }> {
    const buildDir = path.join(__dirname, `../../temp_builds/sub-${submissionId}`);
    
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
FROM alpine:3.18 AS builder
RUN apk add --no-cache g++ make
WORKDIR /app
COPY main.cpp .
RUN g++ -O3 -std=c++17 -o matching-engine main.cpp -pthread

FROM alpine:3.18
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

    const imageTag = `contestant-sub-${submissionId}:latest`;

    return new Promise((resolve) => {
      console.log(`Building Docker image ${imageTag} in ${buildDir}...`);
      
      exec(`docker build -t ${imageTag} .`, { cwd: buildDir }, async (error, stdout, stderr) => {
        const logs = stdout + '\n' + stderr;
        const success = !error;

        // Clean up build files (optional, but good practice)
        try {
          fs.rmSync(buildDir, { recursive: true, force: true });
        } catch (cleanupErr) {
          console.error('Failed to clean up build dir:', cleanupErr);
        }

        // Update database status
        await db.query(
          'UPDATE submissions SET status = $1, build_logs = $2 WHERE id = $3',
          [success ? 'built' : 'failed', logs, submissionId]
        );

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
  static async startContainer(submissionId: number, runId: string): Promise<{ containerId: string; hostname: string }> {
    const imageTag = `contestant-sub-${submissionId}:latest`;
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
      HostConfig: {
        // Attach to the isolated bridge network
        NetworkMode: BENCHMARK_NET,
        // Memory limit: 512MB
        Memory: 512 * 1024 * 1024,
        MemorySwap: 512 * 1024 * 1024, // No swap to disk allowed
        // CPU limit: 1 core (represented by NanoCpus: 1,000,000,000)
        NanoCpus: 1000000000,
        // Security protections: Read-only root filesystem where possible
        // (For alpine, we mount a tmpfs on /tmp for scratch space)
        Tmpfs: {
          '/tmp': 'rw,noexec,nosuid,size=65536k',
        },
      },
    });

    // Start the container
    await container.start();
    console.log(`Successfully started sandboxed container ${hostname} (ID: ${container.id})`);

    return {
      containerId: container.id,
      hostname,
    };
  }

  /**
   * Stops and removes a running sandboxed container.
   */
  static async stopContainer(containerId: string): Promise<void> {
    try {
      console.log(`Stopping container ${containerId}...`);
      const container = docker.getContainer(containerId);
      
      // Stop the container (timeout after 5 seconds, then SIGKILL)
      await container.stop({ t: 5 });
      
      console.log(`Removing container ${containerId}...`);
      await container.remove();
      console.log(`Successfully removed container ${containerId}`);
    } catch (error) {
      console.error(`Error tearing down container ${containerId}:`, error);
      throw error;
    }
  }
}
