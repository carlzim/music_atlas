import fs from 'fs';
import path from 'path';

function run(): void {
  const artifactsDir = path.resolve(process.cwd(), 'eval-artifacts');
  const currentPath = path.join(artifactsDir, 'enrich-evidence.json');
  const baselinePath = path.join(artifactsDir, 'last-enrich-evidence.json');
  const reasonCurrentPath = path.join(artifactsDir, 'reason-quality.json');
  const reasonBaselinePath = path.join(artifactsDir, 'last-reason-quality.json');

  if (!fs.existsSync(currentPath)) {
    console.error(`[eval:rebaseline] Missing current report: ${currentPath}`);
    process.exitCode = 1;
    return;
  }

  fs.mkdirSync(artifactsDir, { recursive: true });
  fs.copyFileSync(currentPath, baselinePath);

  if (fs.existsSync(reasonCurrentPath)) {
    fs.copyFileSync(reasonCurrentPath, reasonBaselinePath);
  }

  console.log('[eval:rebaseline] baseline updated');
  console.log(`[eval:rebaseline] source=${currentPath}`);
  console.log(`[eval:rebaseline] target=${baselinePath}`);
  if (fs.existsSync(reasonCurrentPath)) {
    console.log(`[eval:rebaseline] reason_source=${reasonCurrentPath}`);
    console.log(`[eval:rebaseline] reason_target=${reasonBaselinePath}`);
  }
}

run();
