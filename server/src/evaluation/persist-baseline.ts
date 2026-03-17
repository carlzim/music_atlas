import fs from 'fs';
import path from 'path';

function requireFile(filePath: string): void {
  if (!fs.existsSync(filePath)) {
    throw new Error(`Missing required artifact: ${filePath}`);
  }
}

function copyFile(sourcePath: string, targetPath: string): void {
  requireFile(sourcePath);
  fs.copyFileSync(sourcePath, targetPath);
}

function run(): void {
  const artifactsDir = path.resolve(process.cwd(), 'eval-artifacts');
  fs.mkdirSync(artifactsDir, { recursive: true });

  const enrichCurrent = path.join(artifactsDir, 'enrich-evidence.json');
  const enrichBaseline = path.join(artifactsDir, 'last-enrich-evidence.json');
  const profileCurrent = path.join(artifactsDir, 'profile-status.json');
  const profileBaseline = path.join(artifactsDir, 'last-profile-status.json');
  const reasonQualityCurrent = path.join(artifactsDir, 'reason-quality.json');
  const reasonQualityBaseline = path.join(artifactsDir, 'last-reason-quality.json');

  copyFile(enrichCurrent, enrichBaseline);
  copyFile(profileCurrent, profileBaseline);
  copyFile(reasonQualityCurrent, reasonQualityBaseline);

  console.log('[eval:baseline:persist] persisted');
  console.log(`[eval:baseline:persist] enrich=${enrichBaseline}`);
  console.log(`[eval:baseline:persist] profile=${profileBaseline}`);
  console.log(`[eval:baseline:persist] reason_quality=${reasonQualityBaseline}`);
}

try {
  run();
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`[eval:baseline:persist] FAIL ${message}`);
  process.exitCode = 1;
}
