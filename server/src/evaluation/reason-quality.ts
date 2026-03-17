import { refineCreditTrackReasonsForEval } from '../services/gemini.js';
import fs from 'fs';
import path from 'path';

interface ReasonQualityCaseResult {
  id: string;
  pass: boolean;
  details: string;
  unique: number;
  maxDup: number;
}

function buildGenericTracks(artist: string, count: number): Array<{ artist: string; song: string; reason: string }> {
  const tracks: Array<{ artist: string; song: string; reason: string }> = [];
  for (let i = 1; i <= count; i += 1) {
    tracks.push({
      artist,
      song: `Track ${i}`,
      reason: 'Verified producer credit evidence for Test Producer',
    });
  }
  return tracks;
}

function buildTruthClaimTracks(artist: string, count: number): Array<{ artist: string; song: string; reason: string }> {
  const tracks: Array<{ artist: string; song: string; reason: string }> = [];
  for (let i = 1; i <= count; i += 1) {
    tracks.push({
      artist,
      song: `Truth Track ${i}`,
      reason: 'Verified producer truth claim from discogs for Test Producer',
    });
  }
  return tracks;
}

function metrics(reasons: string[]): { unique: number; maxDup: number } {
  const counts = new Map<string, number>();
  for (const reason of reasons) {
    counts.set(reason, (counts.get(reason) || 0) + 1);
  }
  const unique = counts.size;
  let maxDup = 0;
  for (const count of counts.values()) {
    if (count > maxDup) maxDup = count;
  }
  return { unique, maxDup };
}

function runSingleArtistVariationCase(): ReasonQualityCaseResult {
  const id = 'reason_quality_single_artist_variation';
  const refined = refineCreditTrackReasonsForEval(buildGenericTracks('Pink Floyd', 12), 'engineer', 'Alan Parsons');
  const reasonMetrics = metrics(refined.map((track) => track.reason));
  const pass = reasonMetrics.unique >= 8 && reasonMetrics.maxDup <= 3;
  return {
    id,
    pass,
    details: `unique=${reasonMetrics.unique} max_dup=${reasonMetrics.maxDup}`,
    unique: reasonMetrics.unique,
    maxDup: reasonMetrics.maxDup,
  };
}

function runMultiArtistVariationCase(): ReasonQualityCaseResult {
  const id = 'reason_quality_multi_artist_variation';
  const seedTracks = [
    ...buildGenericTracks('Artist A', 4),
    ...buildGenericTracks('Artist B', 4),
    ...buildGenericTracks('Artist C', 4),
  ];
  const refined = refineCreditTrackReasonsForEval(seedTracks, 'producer', 'Nick Lowe');
  const reasonMetrics = metrics(refined.map((track) => track.reason));
  const pass = reasonMetrics.unique >= 9 && reasonMetrics.maxDup <= 2;
  return {
    id,
    pass,
    details: `unique=${reasonMetrics.unique} max_dup=${reasonMetrics.maxDup}`,
    unique: reasonMetrics.unique,
    maxDup: reasonMetrics.maxDup,
  };
}

function runTruthClaimRewriteCase(): ReasonQualityCaseResult {
  const id = 'reason_quality_truth_claim_rewrite';
  const refined = refineCreditTrackReasonsForEval(buildTruthClaimTracks('Artist Z', 8), 'producer', 'Test Producer');
  const hasTruthClaimPhrase = refined.some((track) => /truth\s+claim\s+from/i.test(track.reason));
  const reasonMetrics = metrics(refined.map((track) => track.reason));
  const pass = !hasTruthClaimPhrase && reasonMetrics.unique >= 6;
  return {
    id,
    pass,
    details: `truth_phrase_present=${hasTruthClaimPhrase} unique=${reasonMetrics.unique}`,
    unique: reasonMetrics.unique,
    maxDup: reasonMetrics.maxDup,
  };
}

function run(): void {
  const strict = process.argv.includes('--strict');
  const results: ReasonQualityCaseResult[] = [
    runSingleArtistVariationCase(),
    runMultiArtistVariationCase(),
    runTruthClaimRewriteCase(),
  ];

  const passed = results.filter((result) => result.pass).length;
  const failed = results.length - passed;
  const minUnique = results.reduce((acc, item) => Math.min(acc, item.unique), Number.POSITIVE_INFINITY);
  const maxDup = results.reduce((acc, item) => Math.max(acc, item.maxDup), 0);
  const artifactsDir = path.resolve(process.cwd(), 'eval-artifacts');
  fs.mkdirSync(artifactsDir, { recursive: true });

  const payload = {
    generatedAt: new Date().toISOString(),
    status: failed > 0 ? 'FAIL' : 'PASS',
    cases: results.length,
    passed,
    failed,
    minUnique,
    maxDup,
    results: results.map((item) => ({
      id: item.id,
      pass: item.pass,
      unique: item.unique,
      maxDup: item.maxDup,
      details: item.details,
    })),
  };
  const jsonPath = path.join(artifactsDir, 'reason-quality.json');
  const mdPath = path.join(artifactsDir, 'reason-quality.md');
  fs.writeFileSync(jsonPath, JSON.stringify(payload, null, 2));
  fs.writeFileSync(
    mdPath,
    [
      '# Reason Quality',
      '',
      `Status: ${payload.status}`,
      `Cases: ${payload.cases}, Passed: ${payload.passed}, Failed: ${payload.failed}`,
      `min_unique=${payload.minUnique}, max_dup=${payload.maxDup}`,
      '',
    ].join('\n')
  );
  console.log('[eval:reason-quality] Reason quality harness');
  console.log(`[eval:reason-quality] Cases: ${results.length}, Passed: ${passed}, Failed: ${failed}`);
  for (const result of results) {
    console.log(`[eval:reason-quality] ${result.pass ? 'PASS' : 'FAIL'} ${result.id} -> ${result.details}`);
  }
  console.log(`[eval:reason-quality] artifact_json=${jsonPath}`);
  console.log(`[eval:reason-quality] artifact_md=${mdPath}`);

  if (strict && failed > 0) {
    process.exitCode = 1;
  }
}

run();
