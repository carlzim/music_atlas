import {
  completeTruthImportRun,
  createTruthImportRun,
  ensureSystemSourcePlaylist,
  saveArtistMembershipEvidence,
  upsertTruthEntity,
  upsertTruthMembership,
  linkTruthExternalId,
} from './db.js';
import { fetchMusicBrainzGroupMembers, resolveMusicBrainzArtist } from './musicbrainz.js';

interface TruthMembershipSyncResult {
  attempted: boolean;
  imported: number;
  skippedReason?: string;
}

const membershipCooldown = new Map<string, number>();
const MEMBERSHIP_SYNC_COOLDOWN_MS = Math.max(
  0,
  Number.isFinite(Number(process.env.TRUTH_MEMBERSHIP_COOLDOWN_MS || '600000'))
    ? Number(process.env.TRUTH_MEMBERSHIP_COOLDOWN_MS || '600000')
    : 600000
);

function normalizeKey(value: string): string {
  return value.trim().toLowerCase().replace(/\s+/g, ' ');
}

function shouldSyncMembershipNow(bandName: string): { allowed: boolean; reason?: string } {
  const key = normalizeKey(bandName);
  const now = Date.now();
  const last = membershipCooldown.get(key) || 0;
  if (last > 0 && now - last < MEMBERSHIP_SYNC_COOLDOWN_MS) {
    return { allowed: false, reason: 'cooldown_active' };
  }
  membershipCooldown.set(key, now);
  return { allowed: true };
}

export async function syncTruthMembershipForBandName(bandName: string): Promise<TruthMembershipSyncResult> {
  const enabled = process.env.ENABLE_TRUTH_MEMBERSHIP !== 'false';
  if (!enabled) {
    return { attempted: false, imported: 0, skippedReason: 'disabled' };
  }

  const normalizedBandName = bandName.trim();
  if (!normalizedBandName) {
    return { attempted: false, imported: 0, skippedReason: 'empty_band' };
  }

  const window = shouldSyncMembershipNow(normalizedBandName);
  if (!window.allowed) {
    return { attempted: false, imported: 0, skippedReason: window.reason || 'cooldown_active' };
  }

  const importRunId = createTruthImportRun('musicbrainz', 'band', normalizedBandName);
  try {
    const resolved = await resolveMusicBrainzArtist(normalizedBandName);
    if (!resolved) {
      completeTruthImportRun(importRunId, 'success', { imported: 0, skipped: 'not_found' });
      return { attempted: true, imported: 0, skippedReason: 'not_found' };
    }

    const resolvedType = (resolved.type || '').trim().toLowerCase();
    if (resolvedType && resolvedType !== 'group') {
      completeTruthImportRun(importRunId, 'success', { imported: 0, skipped: `not_group:${resolvedType}` });
      return { attempted: true, imported: 0, skippedReason: `not_group:${resolvedType}` };
    }

    const members = await fetchMusicBrainzGroupMembers(resolved.id, 120);
    if (members.length === 0) {
      completeTruthImportRun(importRunId, 'success', { imported: 0, skipped: 'no_members_found' });
      return { attempted: true, imported: 0, skippedReason: 'no_members_found' };
    }

    const groupEntity = upsertTruthEntity('group', resolved.name);
    if (!groupEntity) {
      completeTruthImportRun(importRunId, 'error', { error: 'group_entity_upsert_failed' });
      return { attempted: true, imported: 0, skippedReason: 'group_entity_upsert_failed' };
    }
    linkTruthExternalId(groupEntity.id, 'musicbrainz', resolved.id);

    const evidenceRows: Array<{ band: string; person: string; role?: string }> = [];
    let imported = 0;

    for (const member of members) {
      const personEntity = upsertTruthEntity('person', member.personName);
      if (!personEntity) continue;
      linkTruthExternalId(personEntity.id, 'musicbrainz', member.personMbid);

      upsertTruthMembership(
        personEntity.id,
        groupEntity.id,
        'musicbrainz',
        member.sourceRef,
        member.memberRole,
        member.begin,
        member.end,
        100
      );
      evidenceRows.push({
        band: groupEntity.canonical_name,
        person: personEntity.canonical_name,
        role: member.memberRole || undefined,
      });
      imported += 1;
    }

    if (evidenceRows.length > 0) {
      const sourcePlaylistId = ensureSystemSourcePlaylist(
        `[system] truth-membership musicbrainz :: ${groupEntity.canonical_name}`,
        'System truth membership import',
        'Synthetic system source row for MusicBrainz membership truth imports.'
      );
      saveArtistMembershipEvidence(sourcePlaylistId, evidenceRows);
    }

    completeTruthImportRun(importRunId, 'success', {
      imported,
      resolved_group: groupEntity.canonical_name,
      mbid: resolved.id,
    });

    return { attempted: true, imported };
  } catch (error) {
    completeTruthImportRun(importRunId, 'error', {
      error: error instanceof Error ? error.message : String(error),
    });
    return { attempted: true, imported: 0, skippedReason: 'request_failed' };
  }
}
