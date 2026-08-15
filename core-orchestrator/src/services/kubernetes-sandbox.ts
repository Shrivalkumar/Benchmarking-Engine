import fs from 'fs';
import https from 'https';
import { db, CONTESTANT_IMAGE_REPOSITORY, CPP_BUILDER_IMAGE, KUBERNETES_NAMESPACE, KUBERNETES_RUNTIME_CLASS } from '../config';
import type { BuildResult, SandboxBackend, SandboxRun, SubmissionLanguage } from './sandbox';

const SERVICE_ACCOUNT_TOKEN = '/var/run/secrets/kubernetes.io/serviceaccount/token';
const SERVICE_ACCOUNT_CA = '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt';
const BUILD_TIMEOUT_MS = 5 * 60 * 1000;

function resourceName(prefix: string, value: string, reservedSuffixLength = 0) {
  const maximumValueLength = 63 - prefix.length - 1 - reservedSuffixLength;
  return `${prefix}-${value.toLowerCase().replace(/[^a-z0-9-]/g, '').slice(0, maximumValueLength)}`;
}

function submissionDockerfile(language: SubmissionLanguage) {
  if (language === 'go') {
    return `FROM golang:1.22-alpine AS builder
WORKDIR /workspace
COPY main.go .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o matching-engine main.go

FROM alpine:3.20
RUN addgroup -S contestant && adduser -S -G contestant contestant
WORKDIR /app
COPY --from=builder /workspace/matching-engine ./matching-engine
USER contestant
EXPOSE 8080
ENTRYPOINT ["./matching-engine"]
`;
  }

  return `FROM ${CPP_BUILDER_IMAGE} AS builder
WORKDIR /workspace
COPY main.cpp .
RUN g++ -O3 -std=c++17 -o matching-engine main.cpp -pthread

FROM alpine:3.20
RUN apk add --no-cache libstdc++ && addgroup -S contestant && adduser -S -G contestant contestant
WORKDIR /app
COPY --from=builder /workspace/matching-engine ./matching-engine
USER contestant
EXPOSE 8080
ENTRYPOINT ["./matching-engine"]
`;
}

/** Minimal Kubernetes API client using the Pod's scoped service-account token. */
class KubernetesApi {
  private readonly host: string;
  private readonly port: number;
  private readonly token: string;
  private readonly ca: Buffer;

  constructor() {
    this.host = process.env.KUBERNETES_SERVICE_HOST || '';
    this.port = Number(process.env.KUBERNETES_SERVICE_PORT_HTTPS || '443');
    if (!this.host) {
      throw new Error('Kubernetes sandbox backend requires KUBERNETES_SERVICE_HOST');
    }
    this.token = fs.readFileSync(SERVICE_ACCOUNT_TOKEN, 'utf8').trim();
    this.ca = fs.readFileSync(SERVICE_ACCOUNT_CA);
  }

  request<T>(method: string, apiPath: string, payload?: unknown, allowNotFound = false): Promise<T | null> {
    const body = payload === undefined ? undefined : JSON.stringify(payload);
    return new Promise((resolve, reject) => {
      const request = https.request({
        hostname: this.host,
        port: this.port,
        path: apiPath,
        method,
        ca: this.ca,
        headers: {
          Authorization: `Bearer ${this.token}`,
          Accept: 'application/json',
          ...(body ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) } : {}),
        },
      }, (response) => {
        const chunks: Buffer[] = [];
        response.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
        response.on('end', () => {
          const raw = Buffer.concat(chunks).toString('utf8');
          if (allowNotFound && response.statusCode === 404) {
            resolve(null);
            return;
          }
          if (!response.statusCode || response.statusCode < 200 || response.statusCode >= 300) {
            reject(new Error(`Kubernetes API ${method} ${apiPath} failed (${response.statusCode}): ${raw.slice(0, 1024)}`));
            return;
          }
          resolve(raw ? JSON.parse(raw) as T : null);
        });
      });
      request.on('error', reject);
      if (body) request.write(body);
      request.end();
    });
  }
}

type JobStatus = { status?: { succeeded?: number; failed?: number; conditions?: Array<{ type?: string; message?: string; reason?: string }> } };
type PodList = { items?: Array<{ metadata?: { name?: string } }> };

export class KubernetesSandboxBackend implements SandboxBackend {
  private api() {
    return new KubernetesApi();
  }

  private async deleteIgnoringNotFound(api: KubernetesApi, path: string) {
    await api.request('DELETE', path, undefined, true);
  }

  private async waitForBuild(api: KubernetesApi, jobName: string): Promise<{ success: boolean; message: string }> {
    const path = `/apis/batch/v1/namespaces/${KUBERNETES_NAMESPACE}/jobs/${jobName}`;
    const deadline = Date.now() + BUILD_TIMEOUT_MS;
    while (Date.now() < deadline) {
      const job = await api.request<JobStatus>('GET', path);
      const status = job?.status;
      if (status?.succeeded) return { success: true, message: 'Build completed successfully' };
      if (status?.failed) {
        const condition = status.conditions?.find((item) => item.type === 'Failed');
        return { success: false, message: condition?.message || condition?.reason || 'Build Job failed' };
      }
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
    return { success: false, message: 'Build Job timed out' };
  }

  async buildSubmissionImage(submissionId: number, imageTag: string, sourceCode: string, language: SubmissionLanguage): Promise<BuildResult> {
    const api = this.api();
    const buildName = resourceName('submission-build', `${submissionId}-${imageTag}`, '-source'.length);
    const sourceName = `${buildName}-source`;
    const filename = language === 'go' ? 'main.go' : 'main.cpp';
    const fullImageTag = `${CONTESTANT_IMAGE_REPOSITORY.replace(/\/$/, '')}/${imageTag}`;
    let logs = '';

    try {
      await api.request('POST', `/api/v1/namespaces/${KUBERNETES_NAMESPACE}/configmaps`, {
        apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: sourceName, labels: { 'benchmarking.platform': 'true', 'benchmarking.submission_id': String(submissionId) } },
        data: { [filename]: sourceCode, Dockerfile: submissionDockerfile(language) },
      });
      await api.request('POST', `/apis/batch/v1/namespaces/${KUBERNETES_NAMESPACE}/jobs`, {
        apiVersion: 'batch/v1', kind: 'Job', metadata: { name: buildName, labels: { 'benchmarking.platform': 'true', 'benchmarking.role': 'build', 'benchmarking.submission_id': String(submissionId) } },
        spec: {
          backoffLimit: 0, activeDeadlineSeconds: 300, ttlSecondsAfterFinished: 600,
          template: {
            metadata: { labels: { 'benchmarking.platform': 'true', 'benchmarking.role': 'build' } },
            spec: {
              restartPolicy: 'Never', serviceAccountName: 'sandbox-builder', automountServiceAccountToken: false,
              runtimeClassName: KUBERNETES_RUNTIME_CLASS,
              securityContext: { seccompProfile: { type: 'RuntimeDefault' }, runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532, fsGroup: 65532 },
              containers: [{
                name: 'kaniko', image: 'gcr.io/kaniko-project/executor:v1.23.2-debug',
                args: [`--context=dir:///workspace`, `--dockerfile=/workspace/Dockerfile`, `--destination=${fullImageTag}`, '--snapshot-mode=redo', '--verbosity=info'],
                resources: { requests: { cpu: '500m', memory: '512Mi', 'ephemeral-storage': '1Gi' }, limits: { cpu: '1', memory: '1Gi', 'ephemeral-storage': '2Gi' } },
                securityContext: { allowPrivilegeEscalation: false, capabilities: { drop: ['ALL'] } },
                volumeMounts: [{ name: 'source', mountPath: '/workspace', readOnly: true }, { name: 'registry-credentials', mountPath: '/kaniko/.docker', readOnly: true }],
              }],
              volumes: [{ name: 'source', configMap: { name: sourceName } }, { name: 'registry-credentials', secret: { secretName: 'kaniko-registry-credentials' } }],
            },
          },
        },
      });
      const result = await this.waitForBuild(api, buildName);
      logs = result.message;
      await db.query('UPDATE submissions SET docker_image_tag = $1, status = $2, build_logs = $3 WHERE id = $4', [fullImageTag, result.success ? 'built' : 'failed', logs, submissionId]);
      return { success: result.success, imageTag: fullImageTag, logs };
    } catch (error: any) {
      logs = error.message || 'Kubernetes build failed';
      await db.query('UPDATE submissions SET status = $1, build_logs = $2 WHERE id = $3', ['failed', logs, submissionId]);
      return { success: false, imageTag: fullImageTag, logs };
    } finally {
      await this.deleteIgnoringNotFound(api, `/api/v1/namespaces/${KUBERNETES_NAMESPACE}/configmaps/${sourceName}`).catch(() => {});
    }
  }

  async startRun(submissionId: number, runId: string): Promise<SandboxRun> {
    const imageResult = await db.query('SELECT docker_image_tag FROM submissions WHERE id = $1 AND status = $2', [submissionId, 'built']);
    if (imageResult.rows.length === 0) throw new Error(`Submission ${submissionId} is not built or does not exist`);

    const api = this.api();
    const name = resourceName('contestant-run', runId);
    const labels = { 'benchmarking.platform': 'true', 'benchmarking.role': 'contestant', 'benchmarking.run_id': runId, 'benchmarking.submission_id': String(submissionId) };
    await this.deleteIgnoringNotFound(api, `/api/v1/namespaces/${KUBERNETES_NAMESPACE}/services/${name}`);
    await this.deleteIgnoringNotFound(api, `/api/v1/namespaces/${KUBERNETES_NAMESPACE}/pods/${name}`);

    await api.request('POST', `/api/v1/namespaces/${KUBERNETES_NAMESPACE}/services`, {
      apiVersion: 'v1', kind: 'Service', metadata: { name, labels },
      spec: { selector: labels, ports: [{ name: 'http', port: 8080, targetPort: 8080 }] },
    });
    try {
      await api.request('POST', `/api/v1/namespaces/${KUBERNETES_NAMESPACE}/pods`, {
        apiVersion: 'v1', kind: 'Pod', metadata: { name, labels },
        spec: {
          runtimeClassName: KUBERNETES_RUNTIME_CLASS, restartPolicy: 'Never', automountServiceAccountToken: false,
          terminationGracePeriodSeconds: 5, activeDeadlineSeconds: 330,
          nodeSelector: { 'benchmarking.platform/node-pool': 'contestant' }, tolerations: [{ key: 'benchmarking.platform/contestant', operator: 'Equal', value: 'true', effect: 'NoSchedule' }],
          securityContext: { seccompProfile: { type: 'RuntimeDefault' }, runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532, fsGroup: 65532 },
          imagePullSecrets: [{ name: 'contestant-registry-credentials' }],
          containers: [{
            name: 'contestant', image: imageResult.rows[0].docker_image_tag, imagePullPolicy: 'Always', ports: [{ containerPort: 8080, name: 'http' }],
            resources: { requests: { cpu: '500m', memory: '256Mi', 'ephemeral-storage': '128Mi' }, limits: { cpu: '1', memory: '512Mi', 'ephemeral-storage': '256Mi' } },
            securityContext: { allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: { drop: ['ALL'] } },
            volumeMounts: [{ name: 'tmp', mountPath: '/tmp' }],
          }],
          volumes: [{ name: 'tmp', emptyDir: { medium: 'Memory', sizeLimit: '64Mi' } }],
        },
      });
    } catch (error) {
      await this.deleteIgnoringNotFound(api, `/api/v1/namespaces/${KUBERNETES_NAMESPACE}/services/${name}`).catch(() => {});
      throw error;
    }
    return { sandboxId: name, endpoint: `http://${name}.${KUBERNETES_NAMESPACE}.svc.cluster.local:8080` };
  }

  async stopRun(sandboxId: string): Promise<void> {
    const api = this.api();
    await Promise.all([
      this.deleteIgnoringNotFound(api, `/api/v1/namespaces/${KUBERNETES_NAMESPACE}/pods/${sandboxId}`),
      this.deleteIgnoringNotFound(api, `/api/v1/namespaces/${KUBERNETES_NAMESPACE}/services/${sandboxId}`),
    ]);
  }

  async findRunId(runId: string): Promise<string | null> {
    const api = this.api();
    const pods = await api.request<PodList>('GET', `/api/v1/namespaces/${KUBERNETES_NAMESPACE}/pods?labelSelector=benchmarking.run_id%3D${encodeURIComponent(runId)}`);
    return pods?.items?.[0]?.metadata?.name || null;
  }
}
