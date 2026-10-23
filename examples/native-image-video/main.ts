// Generates a Qwen image, edits it through an authenticated input Artifact,
// then asks H3 to animate the generated first/last frames into a WebM.
//
// Node.js 22.18 or newer:
//   GPQ_BASE_URL=https://gpq.example.com \
//   GPQ_MASTER_KEY=<tenant-master-key> \
//   GPQ_QWEN_IMAGE_ALIAS=qwen-image \
//   GPQ_H3_VIDEO_ALIAS=h3-video \
//     node main.ts "a red panda astronaut" --size 512x512 --frames 39 \
//       --edit-prompt "Add a glass helmet" --video-prompt "The astronaut waves" \
//       --seed 7 --out-dir ./out
//
// Optional --lora accepts a SHA-256 configured as an extra image LoRA on an
// online Worker, optionally followed by :scale (for example, HASH:0.75).

import { createHash, randomUUID } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { parseArgs } from "node:util";

type LoraSelection = { sha256: string; scale: number };
type ImageParameters = {
  prompt: string;
  negativePrompt: string;
  width: number;
  height: number;
  referenceArtifactIds: string[];
  loras: LoraSelection[];
  steps?: number;
  cfgScale?: number;
};
type VideoParameters = {
  prompt: string;
  negativePrompt: string;
  width: number;
  height: number;
  frames: number;
  firstFrameArtifactId: string;
  lastFrameArtifactId: string;
};
type ArtifactRef = {
  artifactId: string;
  downloadPath: string;
  manifest?: { mimeType?: string };
};
type Generation = {
  generationId: string;
  state: string;
  outputArtifacts?: ArtifactRef[];
  failure?: { kind?: string; message?: string };
};
type GenerationResponse = { generation?: Generation };

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    size: { type: "string" },
    frames: { type: "string" },
    seed: { type: "string", default: "7" },
    steps: { type: "string" },
    "cfg-scale": { type: "string" },
    "edit-prompt": { type: "string" },
    "video-prompt": { type: "string" },
    "out-dir": { type: "string", default: "./gpq-output" },
    lora: { type: "string", multiple: true },
  },
});

if (positionals.length !== 1 || !positionals[0].trim()) {
  throw new Error("expected one non-empty positional argument: the image prompt");
}
const prompt = positionals[0].trim();
if (!values.size || !/^\d+x\d+$/.test(values.size)) {
  throw new Error("--size is required and must use WIDTHxHEIGHT format");
}
const [width, height] = values.size.split("x").map(Number);
if (
  !Number.isSafeInteger(width) ||
  !Number.isSafeInteger(height) ||
  width <= 0 ||
  height <= 0 ||
  width > 0xffff_ffff ||
  height > 0xffff_ffff ||
  width % 32 !== 0 ||
  height % 32 !== 0
) {
  throw new Error("--size dimensions must be positive uint32 multiples of 32");
}

function positiveInteger(option: string, value: string | undefined): number {
  const parsed = Number(value);
  if (
    !value ||
    !/^\d+$/.test(value) ||
    !Number.isSafeInteger(parsed) ||
    parsed <= 0 ||
    parsed > 0xffff_ffff
  ) {
    throw new Error(`${option} must be a positive uint32 integer`);
  }
  return parsed;
}


const frames = positiveInteger("--frames", values.frames);
if (frames < 5 || (frames - 5) % 17 !== 0) {
  throw new Error("--frames must be at least 5 and satisfy 17k+5");
}
const steps =
  values.steps === undefined ? undefined : positiveInteger("--steps", values.steps);
const cfgScale =
  values["cfg-scale"] === undefined ? undefined : Number(values["cfg-scale"]);
if (
  cfgScale !== undefined &&
  (!values["cfg-scale"] || !Number.isFinite(Math.fround(cfgScale)))
) {
  throw new Error("--cfg-scale must be a finite float32 value");
}

let seed: bigint;
try {
  seed = BigInt(values.seed ?? "7");
} catch {
  throw new Error("--seed must be an integer from 0 through 9223372036854775807");
}
if (seed < 0n || seed > 9_223_372_036_854_775_807n) {
  throw new Error("--seed must be an integer from 0 through 9223372036854775807");
}

const loras: LoraSelection[] = (values.lora ?? []).map((entry) => {
  const [hash, scaleText, extra] = entry.split(":");
  const scale = scaleText === undefined ? 1 : Number(scaleText);
  if (
    extra !== undefined ||
    scaleText === "" ||
    !/^[a-f\d]{64}$/i.test(hash) ||
    !Number.isFinite(Math.fround(scale))
  ) {
    throw new Error(`invalid --lora ${entry}; expected SHA256[:SCALE]`);
  }
  return { sha256: hash.toLowerCase(), scale };
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

const baseUrl = new URL(requireEnv("GPQ_BASE_URL"));
if (
  !["http:", "https:"].includes(baseUrl.protocol) ||
  baseUrl.username ||
  baseUrl.password ||
  baseUrl.search ||
  baseUrl.hash
) {
  throw new Error("GPQ_BASE_URL must be an HTTP(S) origin or base path without credentials");
}
if (!baseUrl.pathname.endsWith("/")) {
  baseUrl.pathname += "/";
}
const masterKey = requireEnv("GPQ_MASTER_KEY");
const qwenAlias = requireEnv("GPQ_QWEN_IMAGE_ALIAS");
const h3Alias = requireEnv("GPQ_H3_VIDEO_ALIAS");

async function rpc<T>(
  service: "GenerationService",
  method: string,
  request: object,
  extraHeaders: Record<string, string> = {},
): Promise<T> {
  const response = await fetch(new URL(`gpq.v1.${service}/${method}`, baseUrl), {
    method: "POST",
    cache: "no-store",
    redirect: "error",
    headers: {
      authorization: `Bearer ${masterKey}`,
      "connect-protocol-version": "1",
      "content-type": "application/json",
      ...extraHeaders,
    },
    body: JSON.stringify(request),
  });
  const text = await response.text();
  let payload: unknown;
  try {
    payload = text ? JSON.parse(text) : {};
  } catch {
    throw new Error(`${method} returned non-JSON HTTP ${response.status}: ${text.slice(0, 400)}`);
  }
  if (!response.ok) {
    const error = payload as { code?: string; message?: string };
    throw new Error(
      `${method} failed (HTTP ${response.status}${error.code ? `, ${error.code}` : ""}): ${error.message ?? text}`,
    );
  }
  return payload as T;
}

async function createInputArtifact(bytes: Buffer<ArrayBuffer>): Promise<string> {
  const response = await rpc<{
    artifactId?: string;
    uploadUrl?: string;
  }>("GenerationService", "CreateInputArtifact", {
    manifest: {
      sizeBytes: String(bytes.byteLength),
      digestSha256: createHash("sha256").update(bytes).digest("hex"),
      kind: "MEDIA_KIND_IMAGE",
      mimeType: "image/png",
    },
  });
  if (!response.artifactId || !response.uploadUrl) {
    throw new Error("CreateInputArtifact response is missing artifact_id or upload_url");
  }
  const uploadUrl = new URL(response.uploadUrl);
  const headers: Record<string, string> = {
    "content-type": "application/octet-stream",
    "content-length": String(bytes.byteLength),
  };
  // Remote-local uploads need the tenant key; never forward it to an external
  // presigned object-store URL.
  if (
    uploadUrl.origin === baseUrl.origin &&
    uploadUrl.pathname.startsWith("/v1/artifacts/")
  ) {
    headers.authorization = `Bearer ${masterKey}`;
  }
  const upload = await fetch(uploadUrl, {
    method: "PUT",
    cache: "no-store",
    redirect: "error",
    headers,
    body: bytes,
  });
  if (!upload.ok) {
    throw new Error(`input Artifact upload failed with HTTP ${upload.status}`);
  }
  return response.artifactId;
}

async function generationFrom(response: GenerationResponse, method: string): Promise<Generation> {
  if (!response.generation?.generationId || !response.generation.state) {
    throw new Error(`${method} response is missing generation or generation_id`);
  }
  return response.generation;
}

async function submitImage(imagePrompt: string, references: string[]): Promise<Generation> {
  const request: {
    modelAlias: string;
    imageParameters: ImageParameters;
    outputPlacement: string;
    seed: string;
  } = {
    modelAlias: qwenAlias,
    imageParameters: {
      prompt: imagePrompt,
      negativePrompt: "",
      width,
      height,
      referenceArtifactIds: references,
      loras,
    },
    outputPlacement: "ARTIFACT_PLACEMENT_WORKER_LOCAL",
    seed: seed.toString(),
  };
  if (steps !== undefined) request.imageParameters.steps = steps;
  if (cfgScale !== undefined) request.imageParameters.cfgScale = cfgScale;
  const response = await rpc<GenerationResponse>(
    "GenerationService",
    "Submit",
    request,
    { "idempotency-key": randomUUID() },
  );
  return generationFrom(response, "Submit");
}

async function submitVideo(firstArtifactId: string, lastArtifactId: string): Promise<Generation> {
  const response = await rpc<GenerationResponse>(
    "GenerationService",
    "Submit",
    {
      modelAlias: h3Alias,
      videoParameters: {
        prompt: values["video-prompt"]?.trim() || prompt,
        negativePrompt: "",
        width,
        height,
        frames,
        firstFrameArtifactId: firstArtifactId,
        lastFrameArtifactId: lastArtifactId,
      } satisfies VideoParameters,
      outputPlacement: "ARTIFACT_PLACEMENT_WORKER_LOCAL",
      seed: seed.toString(),
    },
    { "idempotency-key": randomUUID() },
  );
  return generationFrom(response, "Submit");
}

async function getGeneration(generationId: string): Promise<Generation> {
  const response = await rpc<GenerationResponse>(
    "GenerationService",
    "GetGeneration",
    { generationId },
  );
  return generationFrom(response, "GetGeneration");
}

async function cancelGeneration(generationId: string): Promise<void> {
  const response = await rpc<GenerationResponse>(
    "GenerationService",
    "CancelGeneration",
    { generationId },
  );
  await generationFrom(response, "CancelGeneration");
}

const activeStates: Record<string, true> = {
  GENERATION_STATE_QUEUED: true,
  GENERATION_STATE_RUNNING: true,
  GENERATION_STATE_CANCELLING: true,
};

const terminalStates: Record<string, true> = {
  GENERATION_STATE_SUCCEEDED: true,
  GENERATION_STATE_FAILED: true,
  GENERATION_STATE_CANCELLED: true,
  GENERATION_STATE_EXPIRED: true,
};
let signal: NodeJS.Signals | undefined;
let activeGenerationId: string | undefined;
let cancellation: Promise<void> | undefined;
let cancellationError: unknown;
let cancellationFailed = false;

function requestCancellation(receivedSignal: NodeJS.Signals): void {
  signal ??= receivedSignal;
  if (activeGenerationId && !cancellation) {
    cancellation = cancelGeneration(activeGenerationId).catch((error: unknown) => {
      cancellationFailed = true;
      cancellationError = error;
    });
  }
}

const onSigint = () => requestCancellation("SIGINT");
const onSigterm = () => requestCancellation("SIGTERM");
process.on("SIGINT", onSigint);
process.on("SIGTERM", onSigterm);

class Interrupted extends Error {}
function checkInterrupted(): void {
  if (signal) throw new Interrupted(`interrupted by ${signal}`);
}


async function awaitGeneration(submitted: Generation): Promise<Generation> {
  activeGenerationId = submitted.generationId;
  console.log(`Generation ${submitted.generationId}: ${submitted.state}`);
  try {
    for (;;) {
      if (signal && !cancellation) {
        cancellation = cancelGeneration(submitted.generationId).catch((error: unknown) => {
          cancellationFailed = true;
          cancellationError = error;
        });
      }
      if (cancellation) await cancellation;
      if (cancellationFailed) throw cancellationError;

      const generation = await getGeneration(submitted.generationId);
      console.log(`Generation ${generation.generationId}: ${generation.state}`);
      if (terminalStates[generation.state]) {
        if (signal) {
          throw new Interrupted(`generation cancellation completed as ${generation.state}`);
        }
        if (generation.state === "GENERATION_STATE_SUCCEEDED") {
          return generation;
        }
        throw new Error(
          `generation ${generation.generationId} ended in ${generation.state}` +
            (generation.failure?.message
              ? ` (${generation.failure.kind ?? "failure"}: ${generation.failure.message})`
              : ""),
        );
      }
      if (!activeStates[generation.state]) {
        throw new Error(
          `generation ${generation.generationId} returned unknown state ${generation.state}`,
        );
      }
      await delay(1500);
    }
  } finally {
    activeGenerationId = undefined;
    cancellation = undefined;
    cancellationError = undefined;
    cancellationFailed = false;
  }
}

function requireOutput(generation: Generation, mimeType: string): ArtifactRef {
  const artifact = generation.outputArtifacts?.find(
    (output) => output.manifest?.mimeType === mimeType,
  );
  if (!artifact?.downloadPath) {
    throw new Error(
      `generation ${generation.generationId} succeeded without a ${mimeType} output Artifact`,
    );
  }
  return artifact;
}

const PNG_SIGNATURE = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
const WEBM_SIGNATURE = Buffer.from([0x1a, 0x45, 0xdf, 0xa3]);

async function downloadArtifact(artifact: ArtifactRef, mimeType: string): Promise<Buffer<ArrayBuffer>> {
  const artifactUrl = new URL(artifact.downloadPath, baseUrl);
  if (artifactUrl.origin !== baseUrl.origin) {
    throw new Error(`Artifact ${artifact.artifactId} has a download URL on an unexpected origin`);
  }
  const response = await fetch(artifactUrl, {
    cache: "no-store",
    redirect: "error",
    headers: {
      authorization: `Bearer ${masterKey}`,
      accept: mimeType,
    },
  });
  if (!response.ok) {
    throw new Error(`Artifact ${artifact.artifactId} download failed with HTTP ${response.status}`);
  }
  const bytes = Buffer.from(await response.arrayBuffer());
  const signature = mimeType === "image/png" ? PNG_SIGNATURE : WEBM_SIGNATURE;
  if (!bytes.subarray(0, signature.length).equals(signature)) {
    throw new Error(`Artifact ${artifact.artifactId} is not a valid ${mimeType} file`);
  }
  return bytes;
}

const outputDir = values["out-dir"] ?? "./gpq-output";

async function run(): Promise<void> {
  await mkdir(outputDir, { recursive: true });

  const firstGeneration = await awaitGeneration(await submitImage(prompt, []));
  checkInterrupted();
  const firstImage = await downloadArtifact(requireOutput(firstGeneration, "image/png"), "image/png");
  checkInterrupted();
  const firstImagePath = join(outputDir, "first.png");
  await writeFile(firstImagePath, firstImage);
  console.log(`Wrote ${firstImage.byteLength} bytes to ${firstImagePath}`);

  checkInterrupted();
  const editInputId = await createInputArtifact(firstImage);
  checkInterrupted();
  const editPrompt = values["edit-prompt"]?.trim() || prompt;
  const editedGeneration = await awaitGeneration(await submitImage(editPrompt, [editInputId]));
  checkInterrupted();
  const editedImage = await downloadArtifact(
    requireOutput(editedGeneration, "image/png"),
    "image/png",
  );
  checkInterrupted();
  const editedImagePath = join(outputDir, "edit.png");
  await writeFile(editedImagePath, editedImage);
  console.log(`Wrote ${editedImage.byteLength} bytes to ${editedImagePath}`);

  // Each input Artifact can be linked to only one Generation, so upload fresh
  // copies for the video's independent first/last-frame references.
  checkInterrupted();
  const firstFrameId = await createInputArtifact(firstImage);
  checkInterrupted();
  const lastFrameId = await createInputArtifact(editedImage);
  checkInterrupted();
  const videoGeneration = await awaitGeneration(await submitVideo(firstFrameId, lastFrameId));
  checkInterrupted();
  const video = await downloadArtifact(
    requireOutput(videoGeneration, "video/webm"),
    "video/webm",
  );
  checkInterrupted();
  const videoPath = join(outputDir, "video.webm");
  await writeFile(videoPath, video);
  console.log(`Wrote ${video.byteLength} bytes to ${videoPath}`);
}

try {
  await run();
} catch (error) {
  if (error instanceof Interrupted || signal) {
    if (error instanceof Error) console.error(error.message);
    process.exitCode = signal === "SIGTERM" ? 143 : 130;
  } else {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  }
} finally {
  process.off("SIGINT", onSigint);
  process.off("SIGTERM", onSigterm);
}
