import 'dotenv/config';
import { creditArtistFoldForEval, creditTitleMatchForEval, generatePlaylist } from '../services/gemini.js';
import { getTracksByRecordingCreditEvidence } from '../services/db.js';
import { backfillTruthCreditsFromDiscogs, getTruthCreditCandidates } from '../services/truth-credit-layer.js';

interface TruthCreditApiCaseResult {
  id: string;
  pass: boolean;
  details: string;
}

function nearlyEqual(a: number, b: number): boolean {
  return Math.abs(a - b) < 1e-9;
}

function normalizedReasonList(values: string[] | null | undefined): string[] {
  if (!Array.isArray(values)) return [];
  return Array.from(new Set(values.map((value) => String(value).trim().toLowerCase()).filter(Boolean))).sort();
}

function sameStringList(a: string[] | null | undefined, b: string[] | null | undefined): boolean {
  const left = normalizedReasonList(a);
  const right = normalizedReasonList(b);
  if (left.length !== right.length) return false;
  return left.every((value, index) => value === right[index]);
}

const CREDIT_PLAYLIST_TRACK_TARGET = 25;
const CURATION_TARGET_TOTAL_COUNT = 4;

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
  const curationSelectedTrackTarget = response.truth?.curation?.composition?.selected_track_target ?? null;
  const curationSelectedTrackTargetMet = response.truth?.curation?.composition?.selected_track_target_met ?? null;
  const curationSelectedTrackGap = response.truth?.curation?.composition?.selected_track_gap ?? null;
  const curationSelectedTrackCoverage = response.truth?.curation?.composition?.selected_track_coverage ?? null;
  const curationSelectionRetentionGap = response.truth?.curation?.composition?.selection_retention_gap ?? null;
  const curationSelectionRetentionCoverage = response.truth?.curation?.composition?.selection_retention_coverage ?? null;
  const curationTargetTotalCount = response.truth?.curation?.composition?.target_total_count ?? null;
  const curationTargetMetCount = response.truth?.curation?.composition?.target_met_count ?? null;
  const curationTargetMetCoverage = response.truth?.curation?.composition?.target_met_coverage ?? null;
  const curationTargetMetReasons = response.truth?.curation?.composition?.target_met_reasons ?? null;
  const curationTargetMissCount = response.truth?.curation?.composition?.target_miss_count ?? null;
  const curationTargetMissReasons = response.truth?.curation?.composition?.target_miss_reasons ?? null;
  const curationTargetConsistencyOk = response.truth?.curation?.composition?.target_consistency_ok ?? null;
  const curationTargetReasonPartitionOk = response.truth?.curation?.composition?.target_reason_partition_ok ?? null;
  const curationTargetReasonOverlapCount = response.truth?.curation?.composition?.target_reason_overlap_count ?? null;
  const curationTargetReasonOverlapCoverage = response.truth?.curation?.composition?.target_reason_overlap_coverage ?? null;
  const curationTargetReasonOverlapGap = response.truth?.curation?.composition?.target_reason_overlap_gap ?? null;
  const curationTargetReasonUnionCount = response.truth?.curation?.composition?.target_reason_union_count ?? null;
  const curationTargetReasonUnionGap = response.truth?.curation?.composition?.target_reason_union_gap ?? null;
  const curationTargetReasonUnionCoverage = response.truth?.curation?.composition?.target_reason_union_coverage ?? null;
  const curationTargetReasonBalanceIndex = response.truth?.curation?.composition?.target_reason_balance_index ?? null;
  const curationTargetReasonErrorCount = response.truth?.curation?.composition?.target_reason_error_count ?? null;
  const curationTargetReasonErrorCoverage = response.truth?.curation?.composition?.target_reason_error_coverage ?? null;
  const curationTargetReasonQualityCount = response.truth?.curation?.composition?.target_reason_quality_count ?? null;
  const curationTargetReasonQualityIndex = response.truth?.curation?.composition?.target_reason_quality_index ?? null;
  const curationTargetReasonQualityErrorComplementOk = response.truth?.curation?.composition?.target_reason_quality_error_complement_ok ?? null;
  const curationTargetReasonIntegrityOk = response.truth?.curation?.composition?.target_reason_integrity_ok ?? null;
  const curationTargetReasonIntegrityTotalCount = response.truth?.curation?.composition?.target_reason_integrity_total_count ?? null;
  const curationTargetReasonIntegrityPassedCount = response.truth?.curation?.composition?.target_reason_integrity_passed_count ?? null;
  const curationTargetReasonIntegrityScore = response.truth?.curation?.composition?.target_reason_integrity_score ?? null;
  const curationTargetReasonIntegrityGap = response.truth?.curation?.composition?.target_reason_integrity_gap ?? null;
  const curationTargetSizeMet = response.truth?.curation?.composition?.target_size_met ?? null;
  const curationTargetRetentionMet = response.truth?.curation?.composition?.target_retention_met ?? null;
  const curationTargetArtistMet = response.truth?.curation?.composition?.target_artist_met ?? null;
  const curationTargetDecadeMet = response.truth?.curation?.composition?.target_decade_met ?? null;
  const curationRankingWindowKeptTracks = response.truth?.curation?.ranking_window?.kept_tracks ?? null;
  const curationUniqueArtistTarget = response.truth?.curation?.composition?.unique_artist_target ?? null;
  const curationUniqueArtistTargetMet = response.truth?.curation?.composition?.unique_artist_target_met ?? null;
  const curationUniqueArtistTargetGap = response.truth?.curation?.composition?.unique_artist_target_gap ?? null;
  const curationUniqueArtistTargetCoverage = response.truth?.curation?.composition?.unique_artist_target_coverage ?? null;
  const curationUniqueDecades = response.truth?.curation?.composition?.unique_decades ?? null;
  const curationUniqueDecadeTarget = response.truth?.curation?.composition?.unique_decade_target ?? null;
  const curationUniqueDecadeTargetMet = response.truth?.curation?.composition?.unique_decade_target_met ?? null;
  const curationUniqueDecadeTargetGap = response.truth?.curation?.composition?.unique_decade_target_gap ?? null;
  const curationUniqueDecadeTargetCoverage = response.truth?.curation?.composition?.unique_decade_target_coverage ?? null;
  const curationTopSampleSize = Array.isArray(response.truth?.curation?.top_score_sample)
    ? response.truth!.curation!.top_score_sample!.length
    : 0;
  const expectedUniqueDecadeTarget = params.expectedCurationMode
    ? getExpectedUniqueDecadeTarget(params.expectedCurationMode, tracks.length)
    : null;
  const curationDecadeTargetPass = expectedUniqueDecadeTarget === null
    ? true
    : curationUniqueDecadeTarget === expectedUniqueDecadeTarget;
  const expectedSelectedTrackTarget = CREDIT_PLAYLIST_TRACK_TARGET;
  const selectedTrackTargetPass = curationSelectedTrackTarget === expectedSelectedTrackTarget;
  const expectedSelectedTrackTargetMet = tracks.length >= expectedSelectedTrackTarget;
  const selectedTrackTargetMetPass = curationSelectedTrackTargetMet === expectedSelectedTrackTargetMet;
  const expectedSelectedTrackGap = Math.max(0, expectedSelectedTrackTarget - tracks.length);
  const selectedTrackGapPass = curationSelectedTrackGap === expectedSelectedTrackGap;
  const expectedSelectedTrackCoverage = expectedSelectedTrackTarget > 0
    ? Math.min(1, tracks.length / expectedSelectedTrackTarget)
    : 1;
  const selectedTrackCoveragePass = typeof curationSelectedTrackCoverage === 'number'
    && nearlyEqual(curationSelectedTrackCoverage, expectedSelectedTrackCoverage);
  const expectedSelectionRetentionGap = curationRankingWindowKeptTracks !== null
    ? Math.max(0, curationRankingWindowKeptTracks - tracks.length)
    : null;
  const selectionRetentionGapPass = expectedSelectionRetentionGap === null
    ? true
    : curationSelectionRetentionGap === expectedSelectionRetentionGap;
  const expectedSelectionRetentionCoverage = curationRankingWindowKeptTracks !== null && curationRankingWindowKeptTracks > 0
    ? Math.min(1, tracks.length / curationRankingWindowKeptTracks)
    : null;
  const selectionRetentionCoveragePass = expectedSelectionRetentionCoverage === null
    ? true
    : (typeof curationSelectionRetentionCoverage === 'number' && nearlyEqual(curationSelectionRetentionCoverage, expectedSelectionRetentionCoverage));
  const expectedTargetMissReasons: string[] = [];
  if (expectedSelectedTrackGap > 0) expectedTargetMissReasons.push('size');
  if ((expectedSelectionRetentionGap ?? 0) > 0) expectedTargetMissReasons.push('retention');
  const expectedArtistTargetMet = curationUniqueArtists !== null && curationUniqueArtistTarget !== null
    ? curationUniqueArtists >= curationUniqueArtistTarget
    : null;
  const expectedArtistTargetGap = curationUniqueArtists !== null && curationUniqueArtistTarget !== null
    ? Math.max(0, curationUniqueArtistTarget - curationUniqueArtists)
    : null;
  const artistTargetMetPass = expectedArtistTargetMet === null
    ? true
    : curationUniqueArtistTargetMet === expectedArtistTargetMet;
  const artistTargetGapPass = expectedArtistTargetGap === null
    ? true
    : curationUniqueArtistTargetGap === expectedArtistTargetGap;
  const expectedArtistTargetCoverage = curationUniqueArtists !== null && curationUniqueArtistTarget !== null && curationUniqueArtistTarget > 0
    ? Math.min(1, curationUniqueArtists / curationUniqueArtistTarget)
    : null;
  const artistTargetCoveragePass = expectedArtistTargetCoverage === null
    ? true
    : (typeof curationUniqueArtistTargetCoverage === 'number' && nearlyEqual(curationUniqueArtistTargetCoverage, expectedArtistTargetCoverage));
  const expectedDecadeTargetMet = curationUniqueDecades !== null && curationUniqueDecadeTarget !== null
    ? curationUniqueDecades >= curationUniqueDecadeTarget
    : null;
  const expectedDecadeTargetGap = curationUniqueDecades !== null && curationUniqueDecadeTarget !== null
    ? Math.max(0, curationUniqueDecadeTarget - curationUniqueDecades)
    : null;
  const decadeTargetMetPass = expectedDecadeTargetMet === null
    ? true
    : curationUniqueDecadeTargetMet === expectedDecadeTargetMet;
  const decadeTargetGapPass = expectedDecadeTargetGap === null
    ? true
    : curationUniqueDecadeTargetGap === expectedDecadeTargetGap;
  const expectedDecadeTargetCoverage = curationUniqueDecades !== null && curationUniqueDecadeTarget !== null && curationUniqueDecadeTarget > 0
    ? Math.min(1, curationUniqueDecades / curationUniqueDecadeTarget)
    : null;
  const decadeTargetCoveragePass = expectedDecadeTargetCoverage === null
    ? true
    : (typeof curationUniqueDecadeTargetCoverage === 'number' && nearlyEqual(curationUniqueDecadeTargetCoverage, expectedDecadeTargetCoverage));
  if ((expectedArtistTargetGap ?? 0) > 0) expectedTargetMissReasons.push('artist');
  if ((expectedDecadeTargetGap ?? 0) > 0) expectedTargetMissReasons.push('decade');
  const expectedTargetTotalCount = CURATION_TARGET_TOTAL_COUNT;
  const targetTotalCountPass = curationTargetTotalCount === expectedTargetTotalCount;
  const expectedTargetMetCount = expectedTargetTotalCount - expectedTargetMissReasons.length;
  const targetMetCountPass = curationTargetMetCount === expectedTargetMetCount;
  const expectedTargetMetCoverage = expectedTargetTotalCount > 0
    ? expectedTargetMetCount / expectedTargetTotalCount
    : 1;
  const targetMetCoveragePass = typeof curationTargetMetCoverage === 'number'
    && nearlyEqual(curationTargetMetCoverage, expectedTargetMetCoverage);
  const expectedTargetMetReasons = ['size', 'retention', 'artist', 'decade']
    .filter((reason) => !expectedTargetMissReasons.includes(reason));
  const targetMetReasonsPass = sameStringList(curationTargetMetReasons, expectedTargetMetReasons);
  const expectedTargetMissCount = expectedTargetMissReasons.length;
  const targetMissCountPass = curationTargetMissCount === expectedTargetMissCount;
  const targetMissReasonsPass = sameStringList(curationTargetMissReasons, expectedTargetMissReasons);
  const expectedTargetConsistencyOk = expectedTargetMetCount + expectedTargetMissCount === expectedTargetTotalCount;
  const targetConsistencyPass = curationTargetConsistencyOk === expectedTargetConsistencyOk;
  const expectedTargetReasonOverlapCount = expectedTargetMetReasons.filter((reason) => expectedTargetMissReasons.includes(reason)).length;
  const targetReasonOverlapPass = curationTargetReasonOverlapCount === expectedTargetReasonOverlapCount;
  const expectedTargetReasonOverlapCoverage = expectedTargetTotalCount > 0
    ? Math.min(1, expectedTargetReasonOverlapCount / expectedTargetTotalCount)
    : 0;
  const targetReasonOverlapCoveragePass = typeof curationTargetReasonOverlapCoverage === 'number'
    && nearlyEqual(curationTargetReasonOverlapCoverage, expectedTargetReasonOverlapCoverage);
  const expectedTargetReasonOverlapGap = Math.max(0, expectedTargetTotalCount - expectedTargetReasonOverlapCount);
  const targetReasonOverlapGapPass = curationTargetReasonOverlapGap === expectedTargetReasonOverlapGap;
  const expectedTargetReasonUnionCount = new Set<string>([...expectedTargetMetReasons, ...expectedTargetMissReasons]).size;
  const targetReasonUnionPass = curationTargetReasonUnionCount === expectedTargetReasonUnionCount;
  const expectedTargetReasonUnionGap = Math.max(0, expectedTargetTotalCount - expectedTargetReasonUnionCount);
  const targetReasonUnionGapPass = curationTargetReasonUnionGap === expectedTargetReasonUnionGap;
  const expectedTargetReasonUnionCoverage = expectedTargetTotalCount > 0
    ? Math.min(1, expectedTargetReasonUnionCount / expectedTargetTotalCount)
    : 1;
  const targetReasonUnionCoveragePass = typeof curationTargetReasonUnionCoverage === 'number'
    && nearlyEqual(curationTargetReasonUnionCoverage, expectedTargetReasonUnionCoverage);
  const expectedTargetReasonBalanceIndex = Math.max(0, expectedTargetReasonUnionCoverage - expectedTargetReasonOverlapCoverage);
  const targetReasonBalanceIndexPass = typeof curationTargetReasonBalanceIndex === 'number'
    && nearlyEqual(curationTargetReasonBalanceIndex, expectedTargetReasonBalanceIndex);
  const expectedTargetReasonErrorCount = expectedTargetReasonOverlapCount + expectedTargetReasonUnionGap;
  const targetReasonErrorCountPass = curationTargetReasonErrorCount === expectedTargetReasonErrorCount;
  const expectedTargetReasonErrorCoverage = expectedTargetTotalCount > 0
    ? Math.min(1, expectedTargetReasonErrorCount / expectedTargetTotalCount)
    : 0;
  const targetReasonErrorCoveragePass = typeof curationTargetReasonErrorCoverage === 'number'
    && nearlyEqual(curationTargetReasonErrorCoverage, expectedTargetReasonErrorCoverage);
  const expectedTargetReasonQualityCount = Math.max(0, expectedTargetTotalCount - expectedTargetReasonErrorCount);
  const targetReasonQualityCountPass = curationTargetReasonQualityCount === expectedTargetReasonQualityCount;
  const expectedTargetReasonQualityIndex = Math.max(0, 1 - expectedTargetReasonErrorCoverage);
  const targetReasonQualityIndexPass = typeof curationTargetReasonQualityIndex === 'number'
    && nearlyEqual(curationTargetReasonQualityIndex, expectedTargetReasonQualityIndex);
  const expectedTargetReasonQualityErrorComplementOk = nearlyEqual(expectedTargetReasonQualityIndex + expectedTargetReasonErrorCoverage, 1);
  const targetReasonQualityErrorComplementPass = curationTargetReasonQualityErrorComplementOk === expectedTargetReasonQualityErrorComplementOk;
  const expectedTargetReasonPartitionOk = expectedTargetMetReasons.length + expectedTargetMissReasons.length === expectedTargetTotalCount
    && expectedTargetReasonOverlapCount === 0;
  const expectedTargetReasonIntegrityOk = expectedTargetReasonPartitionOk
    && expectedTargetConsistencyOk
    && expectedTargetReasonQualityErrorComplementOk;
  const targetReasonIntegrityPass = curationTargetReasonIntegrityOk === expectedTargetReasonIntegrityOk;
  const expectedTargetReasonIntegrityChecks = [
    expectedTargetReasonPartitionOk,
    expectedTargetConsistencyOk,
    expectedTargetReasonQualityErrorComplementOk,
  ];
  const expectedTargetReasonIntegrityTotalCount = expectedTargetReasonIntegrityChecks.length;
  const targetReasonIntegrityTotalCountPass = curationTargetReasonIntegrityTotalCount === expectedTargetReasonIntegrityTotalCount;
  const expectedTargetReasonIntegrityPassedCount = expectedTargetReasonIntegrityChecks.filter(Boolean).length;
  const targetReasonIntegrityPassedCountPass = curationTargetReasonIntegrityPassedCount === expectedTargetReasonIntegrityPassedCount;
  const expectedTargetReasonIntegrityScore = expectedTargetReasonIntegrityTotalCount > 0
    ? expectedTargetReasonIntegrityPassedCount / expectedTargetReasonIntegrityTotalCount
    : 1;
  const targetReasonIntegrityScorePass = typeof curationTargetReasonIntegrityScore === 'number'
    && nearlyEqual(curationTargetReasonIntegrityScore, expectedTargetReasonIntegrityScore);
  const expectedTargetReasonIntegrityGap = expectedTargetReasonIntegrityTotalCount - expectedTargetReasonIntegrityPassedCount;
  const targetReasonIntegrityGapPass = curationTargetReasonIntegrityGap === expectedTargetReasonIntegrityGap;
  const targetReasonPartitionPass = curationTargetReasonPartitionOk === expectedTargetReasonPartitionOk;
  const targetSizeMetPass = curationTargetSizeMet === (expectedSelectedTrackGap === 0);
  const targetRetentionMetPass = curationTargetRetentionMet === ((expectedSelectionRetentionGap ?? 0) === 0);
  const targetArtistMetPass = curationTargetArtistMet === ((expectedArtistTargetGap ?? 0) === 0);
  const targetDecadeMetPass = curationTargetDecadeMet === ((expectedDecadeTargetGap ?? 0) === 0);

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
    && selectedTrackTargetPass
    && selectedTrackTargetMetPass
    && selectedTrackGapPass
    && selectedTrackCoveragePass
    && selectionRetentionGapPass
    && selectionRetentionCoveragePass
    && artistTargetMetPass
    && artistTargetGapPass
    && artistTargetCoveragePass
    && decadeTargetMetPass
    && decadeTargetGapPass
    && decadeTargetCoveragePass
    && targetTotalCountPass
    && targetMetCountPass
    && targetMetCoveragePass
    && targetMetReasonsPass
    && targetMissCountPass
    && targetMissReasonsPass
    && targetConsistencyPass
    && targetReasonOverlapPass
    && targetReasonOverlapCoveragePass
    && targetReasonOverlapGapPass
    && targetReasonUnionPass
    && targetReasonUnionGapPass
    && targetReasonUnionCoveragePass
    && targetReasonBalanceIndexPass
    && targetReasonErrorCountPass
    && targetReasonErrorCoveragePass
    && targetReasonQualityCountPass
    && targetReasonQualityIndexPass
    && targetReasonQualityErrorComplementPass
    && targetReasonIntegrityPass
    && targetReasonIntegrityTotalCountPass
    && targetReasonIntegrityPassedCountPass
    && targetReasonIntegrityScorePass
    && targetReasonIntegrityGapPass
    && targetReasonPartitionPass
    && targetSizeMetPass
    && targetRetentionMetPass
    && targetArtistMetPass
    && targetDecadeMetPass;

  return {
    id,
    pass,
    details: `tracks=${tracks.length} outside_evidence=${tracksOutsideEvidence.length} max_per_artist=${maxPerArtist} max_per_album_proxy=${maxPerAlbumProxy} variants=${variantTracks.length} variant_candidates=${candidateVariantCount} curation_mode=${curationMode || 'none'} expected_mode=${params.expectedCurationMode || 'any'} curation_selected_track_target=${curationSelectedTrackTarget ?? 'n/a'} expected_selected_track_target=${expectedSelectedTrackTarget} curation_selected_track_target_met=${curationSelectedTrackTargetMet ?? 'n/a'} expected_selected_track_target_met=${expectedSelectedTrackTargetMet} curation_selected_track_gap=${curationSelectedTrackGap ?? 'n/a'} expected_selected_track_gap=${expectedSelectedTrackGap} curation_selected_track_coverage=${curationSelectedTrackCoverage ?? 'n/a'} expected_selected_track_coverage=${expectedSelectedTrackCoverage} curation_selection_retention_gap=${curationSelectionRetentionGap ?? 'n/a'} expected_selection_retention_gap=${expectedSelectionRetentionGap ?? 'n/a'} curation_selection_retention_coverage=${curationSelectionRetentionCoverage ?? 'n/a'} expected_selection_retention_coverage=${expectedSelectionRetentionCoverage ?? 'n/a'} curation_unique_artists=${curationUniqueArtists ?? 'n/a'} curation_unique_artist_target=${curationUniqueArtistTarget ?? 'n/a'} curation_unique_artist_target_met=${curationUniqueArtistTargetMet ?? 'n/a'} expected_unique_artist_target_met=${expectedArtistTargetMet ?? 'n/a'} curation_unique_artist_target_gap=${curationUniqueArtistTargetGap ?? 'n/a'} expected_unique_artist_target_gap=${expectedArtistTargetGap ?? 'n/a'} curation_unique_artist_target_coverage=${curationUniqueArtistTargetCoverage ?? 'n/a'} expected_unique_artist_target_coverage=${expectedArtistTargetCoverage ?? 'n/a'} curation_unique_decades=${curationUniqueDecades ?? 'n/a'} curation_unique_decade_target=${curationUniqueDecadeTarget ?? 'n/a'} expected_unique_decade_target=${expectedUniqueDecadeTarget ?? 'n/a'} curation_unique_decade_target_met=${curationUniqueDecadeTargetMet ?? 'n/a'} expected_unique_decade_target_met=${expectedDecadeTargetMet ?? 'n/a'} curation_unique_decade_target_gap=${curationUniqueDecadeTargetGap ?? 'n/a'} expected_unique_decade_target_gap=${expectedDecadeTargetGap ?? 'n/a'} curation_unique_decade_target_coverage=${curationUniqueDecadeTargetCoverage ?? 'n/a'} expected_unique_decade_target_coverage=${expectedDecadeTargetCoverage ?? 'n/a'} curation_target_total_count=${curationTargetTotalCount ?? 'n/a'} expected_target_total_count=${expectedTargetTotalCount} curation_target_met_count=${curationTargetMetCount ?? 'n/a'} expected_target_met_count=${expectedTargetMetCount} curation_target_met_coverage=${curationTargetMetCoverage ?? 'n/a'} expected_target_met_coverage=${expectedTargetMetCoverage} curation_target_met_reasons=${normalizedReasonList(curationTargetMetReasons).join('|') || 'none'} expected_target_met_reasons=${normalizedReasonList(expectedTargetMetReasons).join('|') || 'none'} curation_target_miss_count=${curationTargetMissCount ?? 'n/a'} expected_target_miss_count=${expectedTargetMissCount} curation_target_miss_reasons=${normalizedReasonList(curationTargetMissReasons).join('|') || 'none'} expected_target_miss_reasons=${normalizedReasonList(expectedTargetMissReasons).join('|') || 'none'} curation_target_consistency_ok=${curationTargetConsistencyOk ?? 'n/a'} expected_target_consistency_ok=${expectedTargetConsistencyOk} curation_target_reason_overlap_count=${curationTargetReasonOverlapCount ?? 'n/a'} expected_target_reason_overlap_count=${expectedTargetReasonOverlapCount} curation_target_reason_overlap_coverage=${curationTargetReasonOverlapCoverage ?? 'n/a'} expected_target_reason_overlap_coverage=${expectedTargetReasonOverlapCoverage} curation_target_reason_overlap_gap=${curationTargetReasonOverlapGap ?? 'n/a'} expected_target_reason_overlap_gap=${expectedTargetReasonOverlapGap} curation_target_reason_union_count=${curationTargetReasonUnionCount ?? 'n/a'} expected_target_reason_union_count=${expectedTargetReasonUnionCount} curation_target_reason_union_gap=${curationTargetReasonUnionGap ?? 'n/a'} expected_target_reason_union_gap=${expectedTargetReasonUnionGap} curation_target_reason_union_coverage=${curationTargetReasonUnionCoverage ?? 'n/a'} expected_target_reason_union_coverage=${expectedTargetReasonUnionCoverage} curation_target_reason_balance_index=${curationTargetReasonBalanceIndex ?? 'n/a'} expected_target_reason_balance_index=${expectedTargetReasonBalanceIndex} curation_target_reason_error_count=${curationTargetReasonErrorCount ?? 'n/a'} expected_target_reason_error_count=${expectedTargetReasonErrorCount} curation_target_reason_error_coverage=${curationTargetReasonErrorCoverage ?? 'n/a'} expected_target_reason_error_coverage=${expectedTargetReasonErrorCoverage} curation_target_reason_quality_count=${curationTargetReasonQualityCount ?? 'n/a'} expected_target_reason_quality_count=${expectedTargetReasonQualityCount} curation_target_reason_quality_index=${curationTargetReasonQualityIndex ?? 'n/a'} expected_target_reason_quality_index=${expectedTargetReasonQualityIndex} curation_target_reason_quality_error_complement_ok=${curationTargetReasonQualityErrorComplementOk ?? 'n/a'} expected_target_reason_quality_error_complement_ok=${expectedTargetReasonQualityErrorComplementOk} curation_target_reason_integrity_ok=${curationTargetReasonIntegrityOk ?? 'n/a'} expected_target_reason_integrity_ok=${expectedTargetReasonIntegrityOk} curation_target_reason_integrity_total_count=${curationTargetReasonIntegrityTotalCount ?? 'n/a'} expected_target_reason_integrity_total_count=${expectedTargetReasonIntegrityTotalCount} curation_target_reason_integrity_passed_count=${curationTargetReasonIntegrityPassedCount ?? 'n/a'} expected_target_reason_integrity_passed_count=${expectedTargetReasonIntegrityPassedCount} curation_target_reason_integrity_score=${curationTargetReasonIntegrityScore ?? 'n/a'} expected_target_reason_integrity_score=${expectedTargetReasonIntegrityScore} curation_target_reason_integrity_gap=${curationTargetReasonIntegrityGap ?? 'n/a'} expected_target_reason_integrity_gap=${expectedTargetReasonIntegrityGap} curation_target_reason_partition_ok=${curationTargetReasonPartitionOk ?? 'n/a'} expected_target_reason_partition_ok=${expectedTargetReasonPartitionOk} curation_target_size_met=${curationTargetSizeMet ?? 'n/a'} expected_target_size_met=${expectedSelectedTrackGap === 0} curation_target_retention_met=${curationTargetRetentionMet ?? 'n/a'} expected_target_retention_met=${(expectedSelectionRetentionGap ?? 0) === 0} curation_target_artist_met=${curationTargetArtistMet ?? 'n/a'} expected_target_artist_met=${(expectedArtistTargetGap ?? 0) === 0} curation_target_decade_met=${curationTargetDecadeMet ?? 'n/a'} expected_target_decade_met=${(expectedDecadeTargetGap ?? 0) === 0} curation_top_sample=${curationTopSampleSize} truth_attempted=${truthSync.attempted} truth_imported=${truthSync.imported} evidence_candidates=${combinedCandidates.length} used_auto_backfill=${response.verification?.used_auto_backfill === true}`,
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
