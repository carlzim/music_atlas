import Database from 'better-sqlite3';
import { savePlaylist } from '../services/db.js';
import { resolveStudioIdentity, resolveStudioIdentityFromPrompt } from '../services/studio-identity.js';

interface StudioEvidenceCaseResult {
  id: string;
  pass: boolean;
  details: string;
}

function buildRecordingCanonicalKey(artist: string, title: string): string {
  const normalize = (value: string): string => value.trim().toLowerCase().replace(/\s+/g, ' ');
  return `${normalize(artist)}::${normalize(title)}`;
}

function cleanupCaseData(db: Database.Database, playlistId: number, recordingCanonicalKey: string): void {
  const recordingRow = db
    .prepare('SELECT id FROM recordings WHERE canonical_key = ?')
    .get(recordingCanonicalKey) as { id: number } | undefined;

  db.prepare('DELETE FROM recording_equipment_evidence WHERE source_playlist_id = ?').run(playlistId);
  db.prepare('DELETE FROM recording_credit_evidence WHERE source_playlist_id = ?').run(playlistId);
  db.prepare('DELETE FROM recording_studio_evidence WHERE source_playlist_id = ?').run(playlistId);
  db.prepare('DELETE FROM artist_membership_evidence WHERE source_playlist_id = ?').run(playlistId);
  db.prepare('DELETE FROM playlists WHERE id = ?').run(playlistId);

  if (recordingRow && typeof recordingRow.id === 'number') {
    const recordingId = recordingRow.id;
    const remainingEvidence = db
      .prepare(`
        SELECT COUNT(*) AS count
        FROM (
          SELECT 1 FROM recording_equipment_evidence WHERE recording_id = ?
          UNION ALL
          SELECT 1 FROM recording_credit_evidence WHERE recording_id = ?
          UNION ALL
          SELECT 1 FROM recording_studio_evidence WHERE recording_id = ?
        ) evidence
      `)
      .get(recordingId, recordingId, recordingId) as { count: number };

    if ((remainingEvidence?.count || 0) === 0) {
      db.prepare('DELETE FROM recordings WHERE id = ?').run(recordingId);
    }
  }
}

function runLabelPromptNoStudioEvidenceCase(db: Database.Database): StudioEvidenceCaseResult {
  const id = 'studio_evidence_no_label_to_studio_leak';
  const artist = 'Eval Guard Artist Label Leak';
  const song = 'Eval Guard Song Label Leak';
  const recordingCanonicalKey = buildRecordingCanonicalKey(artist, song);

  const saved = savePlaylist(
    '[eval] artists on atlantic records label (studio leak guard)',
    'Eval Label Prompt Guard',
    'Ensures label prompts do not persist studio evidence.',
    JSON.stringify([{ artist, song, reason: 'eval guard' }]),
    JSON.stringify(['eval-guard']),
    null,
    null,
    null,
    JSON.stringify([]),
    JSON.stringify(['United States']),
    JSON.stringify(['New York City']),
    JSON.stringify(['Atlantic Recording Studios']),
    JSON.stringify([]),
    JSON.stringify([]),
    JSON.stringify([]),
    JSON.stringify([])
  );

  try {
    const row = db
      .prepare('SELECT COUNT(*) AS count FROM recording_studio_evidence WHERE source_playlist_id = ?')
      .get(saved.id) as { count: number };

    const pass = (row?.count || 0) === 0;
    return {
      id,
      pass,
      details: `studio_evidence_rows=${row?.count || 0}`,
    };
  } finally {
    cleanupCaseData(db, saved.id, recordingCanonicalKey);
  }
}

function runStudioAliasDedupCase(db: Database.Database): StudioEvidenceCaseResult {
  const id = 'studio_evidence_alias_dedup_same_recording';
  const artist = 'Eval Guard Artist Studio Alias';
  const song = 'Eval Guard Song Studio Alias';
  const recordingCanonicalKey = buildRecordingCanonicalKey(artist, song);

  const saved = savePlaylist(
    '[eval] songs recorded at atlantic studios (alias dedup guard)',
    'Eval Studio Alias Guard',
    'Ensures studio aliases collapse to one canonical evidence row.',
    JSON.stringify([{ artist, song, reason: 'eval guard' }]),
    JSON.stringify(['eval-guard']),
    null,
    null,
    null,
    JSON.stringify([]),
    JSON.stringify(['United States']),
    JSON.stringify(['New York City']),
    JSON.stringify(['Atlantic Recording Studios', 'Atlantic studios']),
    JSON.stringify([]),
    JSON.stringify([]),
    JSON.stringify([]),
    JSON.stringify([])
  );

  try {
    const recordingRow = db
      .prepare('SELECT id FROM recordings WHERE canonical_key = ?')
      .get(recordingCanonicalKey) as { id: number } | undefined;

    if (!recordingRow || typeof recordingRow.id !== 'number') {
      return {
        id,
        pass: false,
        details: 'recording row missing',
      };
    }

    const rows = db
      .prepare(`
        SELECT studio_name, COALESCE(studio_name_canonical, lower(trim(studio_name))) AS studio_key
        FROM recording_studio_evidence
        WHERE source_playlist_id = ? AND recording_id = ?
      `)
      .all(saved.id, recordingRow.id) as Array<{ studio_name: string | null; studio_key: string | null }>;

    const canonicalKeys = new Set(
      rows
        .map((row) => (typeof row.studio_key === 'string' ? row.studio_key.trim() : ''))
        .filter((value) => value.length > 0)
    );

    const pass = rows.length === 1 && canonicalKeys.size === 1;
    return {
      id,
      pass,
      details: `rows=${rows.length} canonical_keys=${canonicalKeys.size}`,
    };
  } finally {
    cleanupCaseData(db, saved.id, recordingCanonicalKey);
  }
}

function runStudioIdentityEmiPromptCase(): StudioEvidenceCaseResult {
  const id = 'studio_identity_emi_prompt_resolves_era';
  const prompt = 'De basta inspelningarna fran EMI studios i Skarmarbrink, Stockholm';
  const resolved = resolveStudioIdentityFromPrompt(prompt);
  const excluded = Array.isArray(resolved?.excludedSuccessorNames) ? resolved.excludedSuccessorNames : [];
  const accepted = Array.isArray(resolved?.acceptedStudioNames) ? resolved.acceptedStudioNames : [];
  const hasCosmosExcluded = excluded.some((value) => value.toLowerCase().includes('cosmos'));
  const hasXLevelExcluded = excluded.some((value) => value.toLowerCase().includes('x-level'));
  const accidentallyAcceptsSuccessor = accepted.some((value) => value.toLowerCase().includes('cosmos') || value.toLowerCase().includes('x-level'));
  const pass = resolved?.key === 'emi_studios_stockholm'
    && hasCosmosExcluded
    && hasXLevelExcluded
    && !accidentallyAcceptsSuccessor;
  return {
    id,
    pass,
    details: `identity=${resolved?.key || 'none'} accepted=${accepted.length} excluded=${excluded.length}`,
  };
}

function runStudioIdentityAirCase(): StudioEvidenceCaseResult {
  const id = 'studio_identity_air_alias_resolution';
  const resolved = resolveStudioIdentity('Air Studios London');
  const accepted = Array.isArray(resolved?.acceptedStudioNames) ? resolved.acceptedStudioNames : [];
  const pass = resolved?.key === 'air_studios_london'
    && accepted.some((value) => value.toLowerCase() === 'air studios')
    && (resolved?.excludedSuccessorNames?.length || 0) === 0;
  return {
    id,
    pass,
    details: `identity=${resolved?.key || 'none'} accepted=${accepted.length}`,
  };
}

function runStudioIdentityGoldStarCase(): StudioEvidenceCaseResult {
  const id = 'studio_identity_gold_star_history_resolution';
  const resolved = resolveStudioIdentityFromPrompt('The history of Gold Star studios');
  const pass = resolved?.key === 'gold_star_studios_los_angeles';
  return {
    id,
    pass,
    details: `identity=${resolved?.key || 'none'}`,
  };
}

function runStudioIdentityPolarCase(): StudioEvidenceCaseResult {
  const id = 'studio_identity_polar_resolution';
  const resolved = resolveStudioIdentityFromPrompt('The recordings of Polar studios');
  const pass = resolved?.key === 'polar_studios_stockholm';
  return {
    id,
    pass,
    details: `identity=${resolved?.key || 'none'}`,
  };
}

function runStudioIdentitySunDefaultCase(): StudioEvidenceCaseResult {
  const id = 'studio_identity_sun_default_memphis';
  const resolved = resolveStudioIdentityFromPrompt('Best recordings made in Sun Studios');
  const accepted = Array.isArray(resolved?.acceptedStudioNames) ? resolved.acceptedStudioNames : [];
  const hasMemphisAlias = accepted.some((value) => value.toLowerCase().includes('memphis'));
  const pass = resolved?.key === 'sun_studio_memphis' && hasMemphisAlias;
  return {
    id,
    pass,
    details: `identity=${resolved?.key || 'none'} accepted=${accepted.length}`,
  };
}

function runStudioIdentityFameCase(): StudioEvidenceCaseResult {
  const id = 'studio_identity_fame_resolution';
  const resolved = resolveStudioIdentityFromPrompt('Best recordings made in FAME Studios');
  const pass = resolved?.key === 'fame_studios_muscle_shoals';
  return {
    id,
    pass,
    details: `identity=${resolved?.key || 'none'}`,
  };
}

function runStudioIdentityMuscleShoalsDefaultCase(): StudioEvidenceCaseResult {
  const id = 'studio_identity_muscle_shoals_default_msss';
  const resolved = resolveStudioIdentityFromPrompt('Best recordings made in Muscle Shoals studio');
  const pass = resolved?.key === 'muscle_shoals_sound_studio';
  return {
    id,
    pass,
    details: `identity=${resolved?.key || 'none'}`,
  };
}

function runStudioIdentityMuscleShoalsSeparationCase(): StudioEvidenceCaseResult {
  const id = 'studio_identity_fame_and_msss_are_separate';
  const fame = resolveStudioIdentityFromPrompt('Best recordings made in FAME Studios');
  const msss = resolveStudioIdentityFromPrompt('Best recordings made in Muscle Shoals Sound Studio');
  const fameAccepted = new Set((fame?.acceptedStudioNames || []).map((value) => value.toLowerCase()));
  const msssAccepted = new Set((msss?.acceptedStudioNames || []).map((value) => value.toLowerCase()));
  const fameLeaksMsss = Array.from(fameAccepted).some((value) => value.includes('muscle shoals sound'));
  const msssLeaksFame = Array.from(msssAccepted).some((value) => value.includes('fame'));
  const pass = fame?.key === 'fame_studios_muscle_shoals'
    && msss?.key === 'muscle_shoals_sound_studio'
    && !fameLeaksMsss
    && !msssLeaksFame;
  return {
    id,
    pass,
    details: `fame=${fame?.key || 'none'} msss=${msss?.key || 'none'} fame_leak=${fameLeaksMsss} msss_leak=${msssLeaksFame}`,
  };
}

function run(): void {
  const strict = process.argv.includes('--strict');
  const db = new Database('playlists.db');

  const results: StudioEvidenceCaseResult[] = [
    runLabelPromptNoStudioEvidenceCase(db),
    runStudioAliasDedupCase(db),
    runStudioIdentityEmiPromptCase(),
    runStudioIdentityAirCase(),
    runStudioIdentityGoldStarCase(),
    runStudioIdentityPolarCase(),
    runStudioIdentitySunDefaultCase(),
    runStudioIdentityFameCase(),
    runStudioIdentityMuscleShoalsDefaultCase(),
    runStudioIdentityMuscleShoalsSeparationCase(),
  ];

  const passed = results.filter((result) => result.pass).length;
  const failed = results.length - passed;

  console.log('[eval:studio-evidence] Studio evidence guard harness');
  console.log(`[eval:studio-evidence] Cases: ${results.length}, Passed: ${passed}, Failed: ${failed}`);
  for (const result of results) {
    console.log(`[eval:studio-evidence] ${result.pass ? 'PASS' : 'FAIL'} ${result.id} -> ${result.details}`);
  }

  if (strict && failed > 0) {
    process.exitCode = 1;
  }
}

run();
