import { spawnSync } from 'child_process';
import { CI_STRICT_PROFILE } from './ci-profile.js';

function run(): void {
  const env = { ...process.env, ...CI_STRICT_PROFILE };
  const npmCommand = process.platform === 'win32' ? 'npm.cmd' : 'npm';

  if (!env.GITHUB_REF_NAME || env.GITHUB_REF_NAME.trim().length === 0) {
    env.GITHUB_REF_NAME = 'main';
  }

  const result = spawnSync(npmCommand, ['run', 'eval:ci:strict'], {
    env,
    stdio: 'inherit',
  });

  if (typeof result.status === 'number') {
    process.exitCode = result.status;
    return;
  }

  process.exitCode = 1;
}

run();
