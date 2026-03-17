import fs from 'fs';
import path from 'path';

interface ProfileStatus {
  sha256?: string;
}

function readJson(filePath: string): ProfileStatus | null {
  try {
    const raw = fs.readFileSync(filePath, 'utf-8');
    return JSON.parse(raw) as ProfileStatus;
  } catch {
    return null;
  }
}

function isMainlineBranch(branchName: string): boolean {
  const normalized = branchName.trim().toLowerCase();
  return normalized === 'main' || normalized === 'master';
}

function run(): void {
  const artifactsDir = path.resolve(process.cwd(), 'eval-artifacts');
  const currentPath = path.join(artifactsDir, 'profile-status.json');
  const previousPath = path.join(artifactsDir, 'last-profile-status.json');

  const current = readJson(currentPath);
  if (!current?.sha256) {
    console.error(`[eval:profile:drift] FAIL missing current profile status at ${currentPath}`);
    process.exitCode = 1;
    return;
  }

  const previous = readJson(previousPath);
  if (!previous?.sha256) {
    console.log(`[eval:profile:drift] SKIP no previous profile status at ${previousPath}`);
    return;
  }

  const branchName = process.env.GITHUB_REF_NAME || 'local';
  const mainline = isMainlineBranch(branchName);
  const allowChange = process.env.ALLOW_PROFILE_SHA_CHANGE === '1';
  const enforceMainline = process.env.ENFORCE_PROFILE_DRIFT_ON_MAIN === '1';

  const changed = previous.sha256 !== current.sha256;
  if (!changed) {
    console.log(`[eval:profile:drift] PASS unchanged sha256=${current.sha256}`);
    return;
  }

  console.log(`[eval:profile:drift] detected profile sha change ${previous.sha256} -> ${current.sha256}`);

  if (!mainline) {
    console.log('[eval:profile:drift] WARN profile changed on non-main branch');
    return;
  }

  if (allowChange) {
    console.log('[eval:profile:drift] PASS profile change allowed by ALLOW_PROFILE_SHA_CHANGE=1');
    return;
  }

  if (!enforceMainline) {
    console.log('[eval:profile:drift] WARN profile changed on mainline (set ENFORCE_PROFILE_DRIFT_ON_MAIN=1 to fail)');
    return;
  }

  console.error('[eval:profile:drift] FAIL profile changed on mainline without ALLOW_PROFILE_SHA_CHANGE=1');
  process.exitCode = 1;
}

run();
