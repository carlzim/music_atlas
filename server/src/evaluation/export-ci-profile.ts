import { CI_STRICT_PROFILE } from './ci-profile.js';

function run(): void {
  const keys = Object.keys(CI_STRICT_PROFILE).sort();
  for (const key of keys) {
    const value = CI_STRICT_PROFILE[key];
    process.stdout.write(`${key}=${value}\n`);
  }
}

run();
