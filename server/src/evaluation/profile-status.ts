import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import { CI_STRICT_PROFILE } from './ci-profile.js';

function run(): void {
  const artifactsDir = path.resolve(process.cwd(), 'eval-artifacts');
  fs.mkdirSync(artifactsDir, { recursive: true });

  const sortedEntries = Object.entries(CI_STRICT_PROFILE)
    .sort(([a], [b]) => a.localeCompare(b));

  const canonical = sortedEntries.map(([key, value]) => `${key}=${value}`).join('\n');
  const sha256 = crypto.createHash('sha256').update(canonical).digest('hex');

  const payload = {
    generatedAt: new Date().toISOString(),
    keys: sortedEntries.length,
    sha256,
  };

  const jsonPath = path.join(artifactsDir, 'profile-status.json');
  const mdPath = path.join(artifactsDir, 'profile-status.md');

  fs.writeFileSync(jsonPath, JSON.stringify(payload, null, 2));
  fs.writeFileSync(
    mdPath,
    [
      '# CI Profile Status',
      '',
      `Generated: ${payload.generatedAt}`,
      `Keys: ${payload.keys}`,
      `SHA256: ${payload.sha256}`,
      '',
    ].join('\n')
  );

  console.log('[eval:profile:status] generated');
  console.log(`[eval:profile:status] keys=${payload.keys}`);
  console.log(`[eval:profile:status] sha256=${payload.sha256}`);
  console.log(`[eval:profile:status] json_path=${jsonPath}`);
  console.log(`[eval:profile:status] markdown_path=${mdPath}`);
}

run();
