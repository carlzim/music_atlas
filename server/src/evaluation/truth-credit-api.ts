import 'dotenv/config';
import { creditArtistFoldForEval, creditTitleMatchForEval, generatePlaylist } from '../services/gemini.js';
import { getTracksByRecordingCreditEvidence } from '../services/db.js';
import { backfillTruthCreditsFromDiscogs, getTruthCreditCandidates } from '../services/truth-credit-layer.js';

interface TruthCreditApiCaseResult {
  id: string;
  pass: boolean;
  details: string;
}

function getExpectedUniqueDecadeTarget(mode: 'essential' | 'balanced' | 'deep_cuts', trackCount: number): number {
  const targetByMode: Record<'essential' | 'balanced' | 'deep_cuts', number> = {
    essential: 2,
    balanced: 3,
    deep_cuts: 4,
  };
  const target = targetByMode[mode] || targetByMode.balanced;
  return Math.min(Math.max(1, trackCount), target);
}

function isVariantTitle(title: string): boolean {
  return /\b(remix|mix|edit|dub|rework|version|extended|club|instrumental|remaster)\b/i.test(title || '');
}

function normalizeKey(value: string | null | undefined): string {
  return (value || '').trim().toLowerCase();
}

function maxBucketSize(values: string[]): number {
  const counts = new Map<string, number>();
  let max = 0;
  for (const value of values) {
    const key = normalizeKey(value);
    if (!key) continue;
    const next = (counts.get(key) || 0) + 1;
    counts.set(key, next);
    if (next > max) max = next;
  }
  return max;
}

async function runTruthCreditApiCase(params: {
  id: string;
  creditName: string;
  creditRole: string;
  prompt: string;
  minimumTracks?: number;
  expectNoVariants?: boolean;
  expectVariantWhenAvailable?: boolean;
  expectedCurationMode?: 'essential' | 'balanced' | 'deep_cuts';
}): Promise<TruthCreditApiCaseResult> {
  const { id, creditName, creditRole, prompt } = params;

  const truthSync = await backfillTruthCreditsFromDiscogs({
    creditName,
    creditRole,
    query: `${creditName} ${creditRole}`,
    limit: 20,
  });

  const response = await generatePlaylist(prompt);
  const tracks = response.playlist.tracks || [];
  const truthCandidates = getTruthCreditCandidates(creditName, creditRole, 500);
  const evidenceCandidates = getTracksByRecordingCreditEvidence(creditName, creditRole, 500);
  const combinedCandidates = [
    ...truthCandidates.map((row) => ({ artist: row.artist, title: row.title })),
    ...evidenceCandidates,
  ];

  const tracksOutsideEvidence = tracks.filter((track) => {
    const foldedArtist = creditArtistFoldForEval(track.artist);
    if (!foldedArtist) return true;
    return !combinedCandidates.some((candidate) => {
      if (creditArtistFoldForEval(candidate.artist) !== foldedArtist) return false;
      return creditTitleMatchForEval(track.song, candidate.title);
    });
  });

  const minimumTracks = Math.max(1, Math.floor(params.minimumTracks ?? 8));
  const hasEnoughTracks = tracks.length >= minimumTracks;
  const respectsTrackCap = tracks.length <= 25;
  const noOutsideEvidence = tracksOutsideEvidence.length === 0;
  const maxPerArtist = maxBucketSize(tracks.map((track) => track.artist));
  const maxPerAlbumProxy = maxBucketSize(tracks.map((track) => track.album_image_url || ''));
  const balancedEnough = maxPerArtist <= 4 && maxPerAlbumProxy <= 4;
  const variantTracks = tracks.filter((track) => isVariantTitle(track.song));
  const candidateVariantCount = combinedCandidates.filter((candidate) => isVariantTitle(candidate.title)).length;
  const curationMode = response.truth?.curation?.mode;
  const curationModePass = params.expectedCurationMode
    ? curationMode === params.expectedCurationMode
    : true;
  const curationUniqueArtists = response.truth?.curation?.composition?.unique_artists ?? null;
  const curationUniqueArtistTarget = response.truth?.curation?.composition?.unique_artist_target ?? null;
  const curationUniqueArtistTargetMet = response.truth?.curation?.composition?.unique_artist_target_met ?? null;
  const curationUniqueDecades = response.truth?.curation?.composition?.unique_decades ?? null;
  const curationUniqueDecadeTarget = response.truth?.curation?.composition?.unique_decade_target ?? null;
  const curationUniqueDecadeTargetMet = response.truth?.curation?.composition?.unique_decade_target_met ?? null;
  const curationTopSampleSize = Array.isArray(response.truth?.curation?.top_score_sample)
    ? response.truth!.curation!.top_score_sample!.length
    : 0;
  const expectedUniqueDecadeTarget = params.expectedCurationMode
    ? getExpectedUniqueDecadeTarget(params.expectedCurationMode, tracks.length)
    : null;
  const curationDecadeTargetPass = expectedUniqueDecadeTarget === null
    ? true
    : curationUniqueDecadeTarget === expectedUniqueDecadeTarget;
  const expectedArtistTargetMet = curationUniqueArtists !== null && curationUniqueArtistTarget !== null
    ? curationUniqueArtists >= curationUniqueArtistTarget
    : null;
  const artistTargetMetPass = expectedArtistTargetMet === null
    ? true
    : curationUniqueArtistTargetMet === expectedArtistTargetMet;
  const expectedDecadeTargetMet = curationUniqueDecades !== null && curationUniqueDecadeTarget !== null
    ? curationUniqueDecades >= curationUniqueDecadeTarget
    : null;
  const decadeTargetMetPass = expectedDecadeTargetMet === null
    ? true
    : curationUniqueDecadeTargetMet === expectedDecadeTargetMet;

  let variantExpectationPass = true;
  if (params.expectNoVariants) {
    variantExpectationPass = variantTracks.length === 0;
  } else if (params.expectVariantWhenAvailable) {
    variantExpectationPass = candidateVariantCount > 0 ? variantTracks.length > 0 : true;
  }

  const pass = hasEnoughTracks
    && respectsTrackCap
    && noOutsideEvidence
    && balancedEnough
    && variantExpectationPass
    && curationModePass
    && curationDecadeTargetPass
    && artistTargetMetPass
    && decadeTargetMetPass;

  return {
    id,
    pass,
    details: `tracks=${tracks.length} outside_evidence=${tracksOutsideEvidence.length} max_per_artist=${maxPerArtist} max_per_album_proxy=${maxPerAlbumProxy} variants=${variantTracks.length} variant_candidates=${candidateVariantCount} curation_mode=${curationMode || 'none'} expected_mode=${params.expectedCurationMode || 'any'} curation_unique_artists=${curationUniqueArtists ?? 'n/a'} curation_unique_artist_target=${curationUniqueArtistTarget ?? 'n/a'} curation_unique_artist_target_met=${curationUniqueArtistTargetMet ?? 'n/a'} expected_unique_artist_target_met=${expectedArtistTargetMet ?? 'n/a'} curation_unique_decades=${curationUniqueDecades ?? 'n/a'} curation_unique_decade_target=${curationUniqueDecadeTarget ?? 'n/a'} expected_unique_decade_target=${expectedUniqueDecadeTarget ?? 'n/a'} curation_unique_decade_target_met=${curationUniqueDecadeTargetMet ?? 'n/a'} expected_unique_decade_target_met=${expectedDecadeTargetMet ?? 'n/a'} curation_top_sample=${curationTopSampleSize} truth_attempted=${truthSync.attempted} truth_imported=${truthSync.imported} evidence_candidates=${combinedCandidates.length} used_auto_backfill=${response.verification?.used_auto_backfill === true}`,
  };
}

async function runProducerApiCase(): Promise<TruthCreditApiCaseResult> {
  return runTruthCreditApiCase({
    id: 'truth_credit_api_producer_evidence_first',
    creditName: 'Brian Eno',
    creditRole: 'producer',
    prompt: 'Songs produced by Brian Eno',
    expectNoVariants: true,
    expectedCurationMode: 'balanced',
  });
}

async function runProducerEssentialApiCase(): Promise<TruthCreditApiCaseResult> {
  return runTruthCreditApiCase({
    id: 'truth_credit_api_producer_essential_mode_detected',
    creditName: 'Brian Eno',
    creditRole: 'producer',
    prompt: 'The best and most iconic songs produced by Brian Eno',
    expectNoVariants: true,
    expectedCurationMode: 'essential',
  });
}

async function runProducerDeepCutsApiCase(): Promise<TruthCreditApiCaseResult> {
  return runTruthCreditApiCase({
    id: 'truth_credit_api_producer_deep_cuts_mode_detected',
    creditName: 'Brian Eno',
    creditRole: 'producer',
    prompt: 'Unknown obscure deep cuts produced by Brian Eno',
    minimumTracks: 5,
    expectedCurationMode: 'deep_cuts',
  });
}

async function runProducerRemixApiCase(): Promise<TruthCreditApiCaseResult> {
  return runTruthCreditApiCase({
    id: 'truth_credit_api_producer_remix_prompt_allows_variants',
    creditName: 'Brian Eno',
    creditRole: 'producer',
    prompt: 'Remixes produced by Brian Eno',
    minimumTracks: 4,
    expectVariantWhenAvailable: true,
  });
}

async function runEngineerApiCase(): Promise<TruthCreditApiCaseResult> {
  return runTruthCreditApiCase({
    id: 'truth_credit_api_engineer_evidence_first',
    creditName: 'Alan Moulder',
    creditRole: 'engineer',
    prompt: 'Songs engineered by Alan Moulder',
  });
}

async function runArrangerApiCase(): Promise<TruthCreditApiCaseResult> {
  return runTruthCreditApiCase({
    id: 'truth_credit_api_arranger_evidence_first',
    creditName: 'Claus Ogerman',
    creditRole: 'arranger',
    prompt: 'Songs arranged by Claus Ogerman',
  });
}

async function run(): Promise<void> {
  const strict = process.argv.includes('--strict');
  const networkEnabled = process.env.ENABLE_NETWORK_EVAL === 'true';

  if (!networkEnabled) {
    console.log('[eval:truth-credit-api] skipped (set ENABLE_NETWORK_EVAL=true to run network eval)');
    return;
  }

  const results: TruthCreditApiCaseResult[] = [
    await runProducerApiCase(),
    await runProducerEssentialApiCase(),
    await runProducerDeepCutsApiCase(),
    await runProducerRemixApiCase(),
    await runEngineerApiCase(),
    await runArrangerApiCase(),
  ];

  const passed = results.filter((item) => item.pass).length;
  const failed = results.length - passed;
  console.log('[eval:truth-credit-api] Truth credit API harness');
  console.log(`[eval:truth-credit-api] Cases: ${results.length}, Passed: ${passed}, Failed: ${failed}`);
  for (const result of results) {
    console.log(`[eval:truth-credit-api] ${result.pass ? 'PASS' : 'FAIL'} ${result.id} -> ${result.details}`);
  }

  if (strict && failed > 0) {
    process.exitCode = 1;
  }
}

run().catch((error) => {
  console.error('[eval:truth-credit-api] failed:', error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
