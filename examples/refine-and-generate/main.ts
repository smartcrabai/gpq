// Refines a rough image idea into a detailed prompt with a gpq chat Model
// alias, then renders it through a gpq image Workflow alias. Both calls go
// through the Vercel AI SDK against Remote's OpenAI-compatible surface, so the
// same code works against any OpenAI-compatible server.
//
// Usage (Node.js 22.18 or newer runs the file directly):
//
//   GPQ_BASE_URL=https://gpq.example.com \
//   GPQ_MASTER_KEY=<tenant-master-key> \
//   GPQ_TEXT_MODEL=<chat-model-alias> \
//   GPQ_IMAGE_WORKFLOW=<image-workflow-alias> \
//   node main.ts "a red panda astronaut" --size 1024x1024 --seed 7 --out panda.png
//
// `--size` binds `$width`/`$height` and `--seed` binds `$seed_value` in the
// Workflow graph; omit either when the graph does not declare the placeholder.
// Any other Workflow parameter can be added under `providerOptions.gpq`, which
// the SDK spreads into the request body and Remote binds as `$<field-name>`.

import { writeFile } from "node:fs/promises";
import { parseArgs } from "node:util";

import { createOpenAICompatible } from "@ai-sdk/openai-compatible";
import { generateImage, generateText } from "ai";

const REFINE_INSTRUCTIONS =
  "You rewrite short image ideas into detailed prompts for a text-to-image model. " +
  "Keep the subject and intent of the idea. Add concrete details about composition, " +
  "lighting, color palette, medium, and style. Write one English paragraph of at " +
  "most 80 words. Reply with the prompt only: no preamble, no quotes, no options.";

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    out: { type: "string", default: "out.png" },
    size: { type: "string" },
    seed: { type: "string" },
  },
});

if (positionals.length !== 1) {
  throw new Error("expected exactly one positional argument: the image idea");
}
const idea = positionals[0];

if (values.size !== undefined && !/^\d+x\d+$/.test(values.size)) {
  throw new Error(`--size must use the WIDTHxHEIGHT format, got ${values.size}`);
}
const size = values.size as `${number}x${number}` | undefined;

const seed = values.seed === undefined ? undefined : Number(values.seed);
if (seed !== undefined && (!Number.isSafeInteger(seed) || seed < 0)) {
  throw new Error(`--seed must be a non-negative integer, got ${values.seed}`);
}

const gpq = createOpenAICompatible({
  name: "gpq",
  baseURL: `${requireEnv("GPQ_BASE_URL")}/v1`,
  apiKey: requireEnv("GPQ_MASTER_KEY"),
});

const refined = await generateText({
  model: gpq.chatModel(requireEnv("GPQ_TEXT_MODEL")),
  instructions: REFINE_INSTRUCTIONS,
  prompt: idea,
});

const prompt = refined.text.trim();
if (!prompt) {
  throw new Error("the chat model returned an empty prompt");
}
console.log(`Refined prompt:\n${prompt}\n`);

// The generic image model drops `seed` with an "unsupported" warning, so it
// travels as a provider option; Remote reads it from the request body.
const generated = await generateImage({
  model: gpq.imageModel(requireEnv("GPQ_IMAGE_WORKFLOW")),
  prompt,
  size,
  providerOptions: seed === undefined ? {} : { gpq: { seed } },
});

for (const warning of generated.warnings) {
  console.warn(`warning: ${JSON.stringify(warning)}`);
}

await writeFile(values.out, generated.image.uint8Array);
console.log(
  `Wrote ${generated.image.uint8Array.byteLength} bytes (${generated.image.mediaType}) to ${values.out}`,
);
