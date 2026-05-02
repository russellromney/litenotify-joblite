// bun run examples/basic.ts
import { open } from "../src/index.ts";

const EXT_PATH =
  process.env.HONKER_EXTENSION_PATH ??
  new URL(
    "../../../target/release/libhonker_extension.dylib",
    import.meta.url,
  ).pathname;

const db = open("demo.db", EXT_PATH);
const q = db.queue("emails");

for (let i = 0; i < 3; i++) {
  q.enqueue({ to: `user-${i}@example.com` });
}

while (true) {
  const job = q.claimOne("worker-1");
  if (!job) break;
  console.log(`processing job ${job.id}: to=${(job.payload as any).to}`);
  job.ack();
}

db.close();
