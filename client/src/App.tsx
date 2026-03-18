import { useState, useEffect } from 'react';
import { Routes, Route, Link, useParams, useSearchParams, useNavigate } from 'react-router-dom';
import type { Playlist } from '@shared/playlist';
import './App.css';

interface PlaylistListItem {
  id: number;
  title: string;
  prompt: string;
  tags: string | null;
  place?: string | null;
  scene?: string | null;
  places?: string | null;
  scenes?: string | null;
  countries?: string | null;
  cities?: string | null;
  studios?: string | null;
  venues?: string | null;
  created_at: string;
}

interface TopTagItem {
  tag: string;
  count: number;
}

interface ArtistAtlasResponse {
  playlists: PlaylistListItem[];
  scenes: string[];
  places: string[];
  memberOf?: string[];
  relatedStudios?: string[];
  relatedArtists: string[];
  relatedCredits: Array<{ name: string; roles: string[] }>;
  relatedEquipment?: string[];
  relatedArtistsEvidence?: { relationType: string; evidenceCount: number };
}

interface LocationAtlasResponse {
  playlists: PlaylistListItem[];
  scenes: string[];
  relatedArtists: string[];
  relatedCountries: string[];
  relatedCities: string[];
  relatedStudios: string[];
  relatedVenues: string[];
  relatedEquipment?: string[];
  city?: string | null;
  country?: string | null;
}

interface CreditAtlasResponse {
  name: string;
  roles: string[];
  primaryRoles?: string[];
  playlists: PlaylistListItem[];
  relatedArtists: string[];
  memberOf?: string[];
  associatedStudios?: string[];
}

interface EquipmentAtlasResponse {
  name: string;
  category: string;
  playlists: PlaylistListItem[];
  relatedArtists: string[];
  relatedScenes: string[];
  relatedStudios: string[];
  relatedArtistsEvidence?: { relationType: string; evidenceCount: number };
  relatedStudiosEvidence?: { relationType: string; evidenceCount: number };
}

type AtlasNodeType = 'artist' | 'tag' | 'scene' | 'country' | 'city' | 'studio' | 'venue' | 'equipment' | 'playlist';

interface ConnectResponse {
  nodes: Array<{ type: AtlasNodeType; value: string }>;
  relations: string[];
  paths?: Array<{
    nodes: Array<{ type: AtlasNodeType; value: string }>;
    relations: string[];
  }>;
}

interface ConnectSuggestion {
  type: AtlasNodeType;
  value: string;
}

interface VerificationSummary {
  evidence_before: number;
  evidence_after: number;
  used_auto_backfill: boolean;
  backfill_inserted: number;
  backfill_skipped_reason?: string;
}

interface TruthSummary {
  membership_sync?: {
    band: string;
    attempted: boolean;
    imported: number;
    skipped_reason?: string;
  };
}

interface PlaylistApiResponse extends Playlist {
  cached?: boolean;
  verification?: VerificationSummary;
  truth?: TruthSummary;
}

interface QualityStatusSummary {
  status?: string;
  counts?: {
    playlists?: number;
    recordings?: number;
    studioEvidence?: number;
    creditEvidence?: number;
    membershipEvidence?: number;
  };
  reasonQuality?: {
    status?: string;
    minUnique?: number;
    maxDup?: number;
    failed?: number;
  };
}

interface QualityStatusResponse {
  qualityStatus?: QualityStatusSummary;
  reasonQuality?: {
    status?: string;
    minUnique?: number;
    maxDup?: number;
    failed?: number;
  };
}

type PromptCreditRole =
  | 'producer'
  | 'engineer'
  | 'arranger'
  | 'cover_designer'
  | 'art_director'
  | 'photographer';

function humanizeBackfillSkipReason(reason: string): string {
  const trimmed = reason.trim();
  if (!trimmed) return 'Auto-backfill was skipped.';
  if (trimmed === 'disabled') return 'Auto-backfill is disabled by server configuration.';
  if (trimmed === 'cooldown_active') return 'Auto-backfill cooldown is active. Try again shortly.';
  if (trimmed === 'unsupported_role') return 'Auto-backfill is not enabled for this credit role yet.';
  if (trimmed === 'discogs_insufficient_evidence') return 'Discogs truth backfill ran, but not enough verified credit evidence was found yet.';
  if (trimmed.startsWith('error:')) return `Auto-backfill failed: ${trimmed.slice(6)}`;
  return `Auto-backfill skipped: ${trimmed}`;
}

function detectCreditRoleFromPrompt(prompt: string): PromptCreditRole | null {
  const value = prompt.trim();
  if (!value) return null;
  if (/\bsleeve\s+design\s+by\b|\bcover\s+design\s+by\b|\bcover\s+art\s+by\b|\bdesigned\s+by\b|\bcover\s+designer\b/i.test(value)) return 'cover_designer';
  if (/\bart\s+direction\s+by\b|\bart\s+director\b/i.test(value)) return 'art_director';
  if (/\bphotography\s+by\b|\bphoto\s+by\b|\bphotographer\b/i.test(value)) return 'photographer';
  if (/\bengineered\s+by\b|\bengineering\s+by\b|\bmixed\s+by\b|\bmixade\s+av\b|\bengineer\b/i.test(value)) return 'engineer';
  if (/\barranged\s+by\b|\barranger\b|\barrangerade\s+av\b/i.test(value)) return 'arranger';
  if (/\bproduced\s+by\b|\bproducer\b|\bproductions?\b|\bwork\s+as\s+(?:a\s+|an\s+)?producer\b|\bproducent\b|\bproducerade\s+av\b/i.test(value)) return 'producer';
  return null;
}

function isDiscogsBackfillRole(role: PromptCreditRole): boolean {
  return role === 'cover_designer' || role === 'art_director' || role === 'photographer';
}

function humanizeCreditRole(role: PromptCreditRole): string {
  if (role === 'cover_designer') return 'cover designer';
  if (role === 'art_director') return 'art director';
  return role;
}

const CONNECT_NODE_TYPES: AtlasNodeType[] = ['artist', 'tag', 'scene', 'country', 'city', 'studio', 'venue', 'equipment', 'playlist'];

function isAtlasNodeType(value: string): value is AtlasNodeType {
  return CONNECT_NODE_TYPES.includes(value as AtlasNodeType);
}

const FEATURED_PATHS: Array<{ label: string; to: string }> = [
  { label: 'Berlin experimental', to: '/scene/Berlin%20experimental' },
  { label: 'Hansa Studios', to: '/studio/Hansa%20Studios' },
  { label: 'Proto-punk origins', to: '/tag/proto-punk' },
  { label: 'Brian Eno', to: '/artist/Brian%20Eno' },
  { label: 'Laurel Canyon folk', to: '/tag/laurel-canyon' },
  { label: 'Session musicians of the 60s', to: '/tag/session-musicians' },
];

function parseTagList(rawTags: string | null | undefined): string[] {
  if (!rawTags) return [];
  try {
    const parsed = JSON.parse(rawTags);
    return Array.isArray(parsed)
      ? parsed
          .filter((item): item is string => typeof item === 'string')
          .map((item) => item.trim())
          .filter((item) => item.length > 0)
      : [];
  } catch {
    return [];
  }
}

function normalizeDisplayKey(value: string): string {
  return value.trim().replace(/\s+/g, ' ').toLowerCase();
}

function displayNameScore(value: string): number {
  const trimmed = value.trim();
  if (!trimmed) return 0;

  let score = 0;
  const words = trimmed.split(/\s+/);
  for (const word of words) {
    if (/^[A-Z][a-z]/.test(word)) score += 2;
    if (/^[A-Z]{2,}$/.test(word)) score += 2;
  }
  if (/\bstudios?\b/i.test(trimmed) && /\bStudios?\b/.test(trimmed)) score += 2;
  return score;
}

function dedupeDisplayValues(values: string[]): string[] {
  const selected = new Map<string, string>();

  for (const value of values) {
    const cleaned = value.trim().replace(/\s+/g, ' ');
    if (!cleaned) continue;

    const key = normalizeDisplayKey(cleaned);
    const existing = selected.get(key);
    if (!existing) {
      selected.set(key, cleaned);
      continue;
    }

    const existingScore = displayNameScore(existing);
    const nextScore = displayNameScore(cleaned);
    if (nextScore > existingScore) {
      selected.set(key, cleaned);
    }
  }

  return Array.from(selected.values());
}

function slugifyTitle(title: string): string {
  return title
    .toLowerCase()
    .replace(/['’]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function getPlaylistPath(id: number, title: string): string {
  const slug = slugifyTitle(title);
  return slug ? `/playlist/${id}/${slug}` : `/playlist/${id}`;
}

function formatCreditRole(role: string): string {
  return role.replace(/_/g, ' ');
}

type EquipmentItem = NonNullable<Playlist['equipment']>[number];

function splitPrimaryAndRelatedEquipmentWithPrompt(playlist: Playlist, promptText: string): {
  primary: EquipmentItem | null;
  related: EquipmentItem[];
} {
  const equipment = Array.isArray(playlist.equipment) ? playlist.equipment : [];
  if (equipment.length === 0) {
    return { primary: null, related: [] };
  }

  const prompt = (promptText || '').toLowerCase();
  const primaryIndex = equipment.findIndex((item) => {
    const equipmentName = item.name.toLowerCase();
    return prompt.length > 0 && prompt.includes(equipmentName);
  });

  const selectedIndex = primaryIndex >= 0 ? primaryIndex : 0;
  return {
    primary: equipment[selectedIndex] || null,
    related: equipment.filter((_, index) => index !== selectedIndex),
  };
}

function getNodeRoute(type: AtlasNodeType, value: string): string {
  const encoded = encodeURIComponent(value);

  if (type === 'artist') return `/artist/${encoded}`;
  if (type === 'tag') return `/tag/${encoded}`;
  if (type === 'scene') return `/scene/${encoded}`;
  if (type === 'country') return `/country/${encoded}`;
  if (type === 'city') return `/city/${encoded}`;
  if (type === 'studio') return `/studio/${encoded}`;
  if (type === 'venue') return `/venue/${encoded}`;
  if (type === 'equipment') return `/equipment/${encoded}`;
  return `/playlist/${encoded}`;
}

type PlaylistArrayField = 'countries' | 'cities' | 'studios' | 'venues' | 'scenes';

function collectTopFromPlaylists(
  playlists: PlaylistListItem[],
  field: PlaylistArrayField,
  limit = 8,
  exclude?: string
): string[] {
  const counts = new Map<string, { name: string; count: number }>();
  const excludeKey = exclude ? exclude.toLowerCase() : null;

  for (const playlist of playlists) {
    const values = parseTagList(playlist[field]);
    for (const value of values) {
      const key = value.toLowerCase();
      if (excludeKey && key === excludeKey) continue;
      const existing = counts.get(key);
      if (existing) {
        existing.count += 1;
      } else {
        counts.set(key, { name: value, count: 1 });
      }
    }
  }

  return Array.from(counts.values())
    .sort((a, b) => b.count - a.count)
    .slice(0, limit)
    .map((item) => item.name);
}

interface AtlasConnectionGroup {
  label: string;
  values: string[];
  to: (value: string) => string;
  subtitle?: string;
}

interface ExploreTargetGroup {
  type: AtlasNodeType;
  values: string[];
}

function pickExploreTargets(
  groups: ExploreTargetGroup[],
  currentType: AtlasNodeType,
  currentValue: string,
  max = 5
): Array<{ type: AtlasNodeType; value: string }> {
  const results: Array<{ type: AtlasNodeType; value: string }> = [];
  const seen = new Set<string>();
  const currentKey = `${currentType}:${currentValue.toLowerCase()}`;

  for (const group of groups) {
    for (const value of group.values) {
      const key = `${group.type}:${value.toLowerCase()}`;
      if (key === currentKey || seen.has(key)) continue;
      seen.add(key);
      results.push({ type: group.type, value });
      break;
    }
    if (results.length >= max) break;
  }

  return results.slice(0, max);
}

function renderVisiblePathNodes(nodes: Array<{ type: AtlasNodeType; value: string }>): Array<{ type: AtlasNodeType; value: string }> {
  const filtered = nodes.filter((node) => node.type !== 'playlist');
  return filtered.length > 0 ? filtered : nodes;
}

function getConnectionPathLabel(
  nodes: Array<{ type: AtlasNodeType; value: string }>,
  relations: string[]
): string {
  if (relations.some((relation) => relation.toLowerCase().includes('influence'))) {
    return 'Influence path';
  }

  const nodeTypes = nodes.map((node) => String(node.type));
  if (nodeTypes.includes('credit')) return 'Credit path';
  if (nodeTypes.includes('studio')) return 'Studio path';
  if (nodeTypes.includes('scene')) return 'Scene path';
  if (nodeTypes.includes('venue')) return 'Venue path';
  if (nodeTypes.includes('city') || nodeTypes.includes('country')) return 'Place path';

  return 'Connection path';
}

function getConnectionPathSubtitle(label: string): string {
  if (label === 'Influence path') return 'Shows a direct line of influence between these nodes.';
  if (label === 'Credit path') return 'Connects through shared producers, designers, engineers, or other credits.';
  if (label === 'Studio path') return 'Connects through recordings or activity tied to the same studio.';
  if (label === 'Scene path') return 'Connects through the same musical scene or movement.';
  if (label === 'Venue path') return 'Connects through the same live venue or performance context.';
  if (label === 'Place path') return 'Connects through a shared city or country context.';
  return 'Shows one possible path through the atlas.';
}

function AtlasConnections({ groups }: { groups: AtlasConnectionGroup[] }) {
  const normalizedGroups = groups
    .map((group) => ({ ...group, values: dedupeDisplayValues(group.values) }))
    .filter((group) => group.values.length > 0);

  if (normalizedGroups.length === 0) {
    return null;
  }

  return (
    <div className="meta">
      <p><strong>Atlas connections</strong></p>
      {normalizedGroups.map((group) => (
        <div key={group.label}>
          <p><strong>{group.label}:</strong></p>
          {group.subtitle && <p className="playlist-date">{group.subtitle}</p>}
          <div className="tags">
            {group.values.map((value, i) => (
              <Link key={`${group.label}-${i}`} to={group.to(value)} className="tag tag-link">{value}</Link>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

function ExplorePaths({
  fromType,
  fromValue,
  targets,
}: {
  fromType: AtlasNodeType;
  fromValue: string;
  targets: Array<{ type: AtlasNodeType; value: string }>;
}) {
  const [paths, setPaths] = useState<Array<{ target: { type: AtlasNodeType; value: string }; nodes: Array<{ type: AtlasNodeType; value: string }> }>>([]);

  useEffect(() => {
    const loadPaths = async () => {
      if (!fromValue || targets.length === 0) {
        setPaths([]);
        return;
      }

      const requests = targets.map(async (target) => {
        const params = new URLSearchParams({
          fromType,
          fromValue,
          toType: target.type,
          toValue: target.value,
        });

        try {
          const response = await fetch(`/api/connect?${params.toString()}`);
          if (!response.ok) return null;
          const data = await response.json() as ConnectResponse;
          if (!Array.isArray(data.nodes) || data.nodes.length === 0) return null;

          const visibleNodes = renderVisiblePathNodes(data.nodes);
          if (visibleNodes.length < 2) return null;

          return { target, nodes: visibleNodes };
        } catch {
          return null;
        }
      });

      const settled = await Promise.all(requests);
      setPaths(settled.filter((item): item is { target: { type: AtlasNodeType; value: string }; nodes: Array<{ type: AtlasNodeType; value: string }> } => item !== null));
    };

    loadPaths();
  }, [fromType, fromValue, targets]);

  if (paths.length === 0) {
    return null;
  }

  return (
    <div className="meta">
      <p><strong>Explore paths</strong></p>
      {paths.map((path, idx) => (
        <div key={`${path.target.type}-${path.target.value}-${idx}`}>
          <div className="tags">
            {path.nodes.map((node, i) => (
              <span key={`${node.type}-${node.value}-${i}`}>
                <Link to={getNodeRoute(node.type, node.value)} className="tag tag-link">{node.value}</Link>
                {i < path.nodes.length - 1 ? <span>{' -> '}</span> : null}
              </span>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

function HomePage() {
  const [prompt, setPrompt] = useState('');
  const [loading, setLoading] = useState(false);
  const [evidenceLoading, setEvidenceLoading] = useState(false);
  const [evidenceMessage, setEvidenceMessage] = useState<string | null>(null);
  const [playlist, setPlaylist] = useState<Playlist | null>(null);
  const [cached, setCached] = useState(false);
  const [verification, setVerification] = useState<VerificationSummary | null>(null);
  const [truthSummary, setTruthSummary] = useState<TruthSummary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [recentPlaylists, setRecentPlaylists] = useState<PlaylistListItem[]>([]);
  const [topTags, setTopTags] = useState<TopTagItem[]>([]);
  const [qualityStatus, setQualityStatus] = useState<QualityStatusSummary | null>(null);
  const [qualityLoading, setQualityLoading] = useState(false);

  const loadRecentPlaylists = async () => {
    try {
      const response = await fetch('/api/playlists');
      const data = await response.json();
      setRecentPlaylists(data);
    } catch (e) {
      console.error('Failed to load playlists:', e);
    }
  };

  const loadTopTags = async () => {
    try {
      const response = await fetch('/api/tags');
      const data = await response.json();
      setTopTags(data);
    } catch (e) {
      console.error('Failed to load top tags:', e);
      setTopTags([]);
    }
  };

  const loadQualityStatus = async () => {
    setQualityLoading(true);
    try {
      const response = await fetch('/api/quality/status');
      if (!response.ok) return;
      const data = await response.json() as QualityStatusResponse;
      const status = data.qualityStatus || null;
      if (!status) return;
      const reasonQuality = data.reasonQuality || status.reasonQuality;
      setQualityStatus({
        ...status,
        reasonQuality: reasonQuality || status.reasonQuality,
      });
    } catch {
      setQualityStatus(null);
    } finally {
      setQualityLoading(false);
    }
  };

  useEffect(() => {
    loadRecentPlaylists();
    loadTopTags();
    loadQualityStatus();
  }, []);

  const generateFromPrompt = async (promptText: string) => {
    setLoading(true);
    setError(null);
    setCached(false);
    setVerification(null);
    setTruthSummary(null);
    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => controller.abort(), 45000);

    try {
      const response = await fetch('/api/playlist', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt: promptText }),
        signal: controller.signal,
      });

      if (!response.ok) {
        let serverMessage = '';
        try {
          const payload = await response.json() as { error?: unknown; message?: unknown };
          if (typeof payload.error === 'string' && payload.error.trim().length > 0) {
            serverMessage = payload.error.trim();
          } else if (typeof payload.message === 'string' && payload.message.trim().length > 0) {
            serverMessage = payload.message.trim();
          }
        } catch {
          serverMessage = '';
        }

        throw new Error(
          serverMessage || `Playlist generation failed (${response.status}). Please try again.`
        );
      }

      const data = await response.json() as PlaylistApiResponse;
      const { cached: cachedFlag, ...playlistData } = data;
      const { verification: verificationData, truth: truthData, ...playlistCore } = playlistData;
      setPlaylist(playlistCore);
      setCached(cachedFlag || false);
      setVerification(verificationData || null);
      setTruthSummary(truthData || null);
      loadRecentPlaylists();
    } catch (err) {
      setPlaylist(null);
      setCached(false);
      setVerification(null);
      setTruthSummary(null);
      if (err instanceof DOMException && err.name === 'AbortError') {
        setError('Request timed out after 45s. Try a narrower prompt or run backfill first.');
      } else {
        setError(err instanceof Error ? err.message : 'An error occurred');
      }
    } finally {
      window.clearTimeout(timeoutId);
      setLoading(false);
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    const promptText = prompt.trim();
    if (!promptText) return;
    await generateFromPrompt(promptText);
  };

  const handleBackfillEvidence = async () => {
    const promptText = prompt.trim();
    if (!promptText) return;

    const detectedRole = detectCreditRoleFromPrompt(promptText);
    if (!detectedRole) {
      setError('Could not detect credit role from prompt. Try adding "produced by", "engineered by", or "sleeve design by".');
      return;
    }

    const useDiscogsTruthBackfill = isDiscogsBackfillRole(detectedRole);
    const endpoint = useDiscogsTruthBackfill ? '/api/evidence/backfill-credit-truth' : '/api/evidence/backfill-credit';

    setEvidenceLoading(true);
    setEvidenceMessage(null);
    try {
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt: promptText, limit: 120 }),
      });

      const payload = await response.json() as {
        success?: boolean;
        error?: unknown;
        insertedEvidence?: number;
        imported?: number;
        role?: string;
        name?: string;
      };

      if (!response.ok) {
        throw new Error(typeof payload.error === 'string' ? payload.error : 'Failed to backfill credit evidence');
      }

      if (useDiscogsTruthBackfill) {
        const imported = typeof payload.imported === 'number' ? payload.imported : 0;
        setEvidenceMessage(`Backfilled ${imported} truth credit rows via Discogs (${humanizeCreditRole(detectedRole)}). Regenerating playlist...`);
      } else {
        const inserted = typeof payload.insertedEvidence === 'number' ? payload.insertedEvidence : 0;
        const role = typeof payload.role === 'string' ? payload.role : humanizeCreditRole(detectedRole);
        const name = typeof payload.name === 'string' ? payload.name : 'the requested name';
        setEvidenceMessage(`Backfilled ${inserted} ${role} evidence rows for ${name}. Regenerating playlist...`);
      }

      await generateFromPrompt(promptText);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to backfill credit evidence');
    } finally {
      setEvidenceLoading(false);
    }
  };

  const isCreditEvidenceError = Boolean(
    error && /not enough verified credit evidence|only\s+\d+\s+strongly verified tracks|no verified\s+.+\s+evidence exists yet/i.test(error)
  );
  const detectedCreditRole = detectCreditRoleFromPrompt(prompt);
  const isLikelyCreditPrompt = Boolean(detectedCreditRole);
  const backfillSourceLabel = detectedCreditRole
    ? isDiscogsBackfillRole(detectedCreditRole)
      ? 'Discogs'
      : 'MusicBrainz'
    : 'MusicBrainz';
  const creditStatusBadge = verification
    ? verification.used_auto_backfill
      ? `Auto-backfill used (+${verification.backfill_inserted} evidence rows)`
      : verification.backfill_skipped_reason
        ? humanizeBackfillSkipReason(verification.backfill_skipped_reason)
        : `Evidence ready (${verification.evidence_after} verified recordings)`
    : null;
  const creditStatusClass = verification
    ? verification.used_auto_backfill
      ? 'credit-status auto'
      : verification.backfill_skipped_reason
        ? 'credit-status warning'
        : 'credit-status ready'
    : 'credit-status';

  return (
    <>
      <form onSubmit={handleSubmit} className="prompt-form">
        <textarea
          value={prompt}
          onChange={(e) => {
            setPrompt(e.target.value);
            setEvidenceMessage(null);
          }}
          placeholder="Describe the type of music you want..."
          rows={4}
        />
        <button type="submit" disabled={loading || !prompt.trim()}>
          {loading ? 'Generating playlist...' : 'Generate playlist'}
        </button>
        {isLikelyCreditPrompt && (
          <button type="button" onClick={handleBackfillEvidence} disabled={evidenceLoading || loading || !prompt.trim()}>
            {evidenceLoading ? 'Backfilling evidence...' : `Backfill credit evidence (${backfillSourceLabel})`}
          </button>
        )}
        {isLikelyCreditPrompt && <p className="credit-status hint">Credit prompt detected ({humanizeCreditRole(detectedCreditRole!)}). You can backfill evidence from {backfillSourceLabel} before generating.</p>}
        {isLikelyCreditPrompt && creditStatusBadge && <p className={creditStatusClass}>{creditStatusBadge}</p>}
      </form>

      {qualityStatus && (
        <div className="quality-panel">
          <div className="quality-panel-header">
            <h3>Quality diagnostics</h3>
            <button type="button" onClick={loadQualityStatus} disabled={qualityLoading}>
              {qualityLoading ? 'Refreshing...' : 'Refresh diagnostics'}
            </button>
          </div>
          <p>
            Status: <strong>{qualityStatus.status || 'n/a'}</strong>
            {' '}| Reason quality: {qualityStatus.reasonQuality?.status || 'n/a'}
          </p>
          <p>
            Reason metrics: min unique {qualityStatus.reasonQuality?.minUnique ?? 'n/a'},
            {' '}max dup {qualityStatus.reasonQuality?.maxDup ?? 'n/a'}
          </p>
          <p>
            Evidence: playlists {qualityStatus.counts?.playlists ?? 'n/a'}, recordings {qualityStatus.counts?.recordings ?? 'n/a'},
            {' '}studio {qualityStatus.counts?.studioEvidence ?? 'n/a'}, credit {qualityStatus.counts?.creditEvidence ?? 'n/a'}
          </p>
        </div>
      )}

      <div className="result">
        {error && <p className="error">{error}</p>}
        {verification && (
          <p>
            Verification: evidence {verification.evidence_before} {'->'} {verification.evidence_after}
            {verification.used_auto_backfill ? `, auto-backfill inserted ${verification.backfill_inserted}` : ''}
          </p>
        )}
        {verification && (
          <details className="verification-details">
            <summary>Verification details</summary>
            <pre>{JSON.stringify(verification, null, 2)}</pre>
          </details>
        )}
        {truthSummary && (
          <details className="verification-details">
            <summary>Truth sync details</summary>
            <pre>{JSON.stringify(truthSummary, null, 2)}</pre>
          </details>
        )}
        {verification && !verification.used_auto_backfill && verification.backfill_skipped_reason && (
          <p>{humanizeBackfillSkipReason(verification.backfill_skipped_reason)}</p>
        )}
        {isCreditEvidenceError && !isLikelyCreditPrompt && (
          <button type="button" onClick={handleBackfillEvidence} disabled={evidenceLoading || loading || !prompt.trim()}>
            {evidenceLoading ? 'Backfilling evidence...' : 'Backfill credit evidence from MusicBrainz and retry'}
          </button>
        )}
        {evidenceMessage && <p>{evidenceMessage}</p>}
        
        {playlist ? (
          <div className="playlist">
            {cached && <span className="cached-badge">Cached</span>}
            <h2>{playlist.title}</h2>
            {playlist.tags && playlist.tags.length > 0 && (
              <div className="tags">
                {playlist.tags.map((tag, i) => (
                  <Link key={i} to={`/tag/${encodeURIComponent(tag)}`} className="tag tag-link">{tag}</Link>
                ))}
              </div>
            )}
            <p className="description">{playlist.description}</p>
            <ul className="tracks">
              {playlist.tracks.map((track, index) => (
                <li key={index} className="track">
                  {track.album_image_url && (
                    <img src={track.album_image_url} alt="Album" className="album-art" />
                  )}
                  <div className="track-info">
                    <span className="track-title">{track.song}</span>
                    <span className="track-artist">
                      by <Link to={`/artist/${encodeURIComponent(track.artist)}`} className="artist-link">{track.artist}</Link>
                    </span>
                    <p className="track-reason">{track.reason}</p>
                    {track.spotify_url && (
                      <a href={track.spotify_url} target="_blank" rel="noopener noreferrer" className="spotify-link">
                        Open in Spotify
                      </a>
                    )}
                  </div>
                </li>
              ))}
            </ul>
          </div>
        ) : !error ? (
          <p className="placeholder-text">
            Enter a prompt above and click the button to generate a playlist.
          </p>
        ) : null}
      </div>

      <div className="recent-playlists">
        <h3>Featured paths</h3>
        <div className="tags">
          {FEATURED_PATHS.map((item) => (
            <Link key={item.to} to={item.to} className="tag tag-link">
              {item.label}
            </Link>
          ))}
        </div>
      </div>

      {topTags.length > 0 && (
        <div className="recent-playlists">
          <h3>Explore the Atlas</h3>
          <div className="tags">
            {topTags.map((item) => (
              <Link key={item.tag} to={`/tag/${encodeURIComponent(item.tag)}`} className="tag tag-link">
                {item.tag}
              </Link>
            ))}
          </div>
        </div>
      )}

      {recentPlaylists.length > 0 && (
        <div className="recent-playlists">
          <h3>Recent Playlists</h3>
          <ul className="playlist-list">
            {recentPlaylists.map((p) => (
              <li key={p.id} className="playlist-item">
                <Link to={getPlaylistPath(p.id, p.title)} className="playlist-link">
                  <span className="playlist-title">{p.title}</span>
                  <span className="playlist-prompt">{p.prompt}</span>
                  <span className="playlist-date">
                    {new Date(p.created_at).toLocaleDateString()}
                  </span>
                </Link>
                {parseTagList(p.tags).length > 0 && (
                  <div className="tags">
                    {parseTagList(p.tags).slice(0, 3).map((tag, i) => (
                      <Link key={i} to={`/tag/${encodeURIComponent(tag)}`} className="tag tag-link">{tag}</Link>
                    ))}
                  </div>
                )}
              </li>
            ))}
          </ul>
        </div>
      )}
    </>
  );
}

function PlaylistPage() {
  const { id, slug } = useParams<{ id: string; slug?: string }>();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [playlist, setPlaylist] = useState<Playlist | null>(null);
  const [prompt, setPrompt] = useState('');
  const [createdAt, setCreatedAt] = useState('');
  const [relatedPlaylists, setRelatedPlaylists] = useState<PlaylistListItem[]>([]);
  const [spotifyState, setSpotifyState] = useState<'idle' | 'saving' | 'success' | 'error'>('idle');
  const [spotifyMessage, setSpotifyMessage] = useState<string | null>(null);
  const [spotifyPlaylistUrl, setSpotifyPlaylistUrl] = useState<string | null>(null);
  const [spotifyMatchDetails, setSpotifyMatchDetails] = useState<{
    addedTracks: number;
    matched: number;
    skipped: number;
    skippedTracksTotal: number;
    duplicateUriMatches: number;
    uncertainMatches: number;
    uncertainTracks: Array<{ artist: string; song: string; score: number }>;
    matchedTracksSample: Array<{ artist: string; song: string; source: string; score?: number | null }>;
    skipReasonCounts: Record<string, number>;
    skippedTracks: Array<{ artist: string; song: string; reason?: string }>;
    matchSources?: {
      trackSpotifyUrl?: number;
      recordingSpotifyUrl?: number;
      isrc?: number;
      search?: number;
    };
    addTracksChunkStats?: {
      totalChunks?: number;
      totalAttempts?: number;
      retriedChunks?: number;
    };
  } | null>(null);
  const [shareMessage, setShareMessage] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const spotify = searchParams.get('spotify');
    const spotifyReason = searchParams.get('spotify_reason');
    if (spotify === 'connected') {
      setSpotifyState('success');
      setSpotifyMessage('Spotify connected. You can now save this playlist.');
    } else if (spotify === 'auth_failed') {
      setSpotifyState('error');
      if (spotifyReason === 'missing_state_cookie' || spotifyReason === 'state_mismatch') {
        setSpotifyMessage('Spotify authentication failed because callback cookies were missing. Open the app via http://localhost:5173 (not 127.0.0.1) and try again.');
      } else if (spotifyReason === 'missing_code') {
        setSpotifyMessage('Spotify callback did not return an auth code. Check that SPOTIFY_REDIRECT_URI exactly matches your Spotify app settings.');
      } else if (spotifyReason?.startsWith('spotify_error:')) {
        const spotifyError = spotifyReason.slice('spotify_error:'.length) || 'unknown_error';
        setSpotifyMessage(`Spotify authentication failed: ${spotifyError}. Check redirect URI and Spotify app settings.`);
      } else if (spotifyReason?.startsWith('token_exchange_failed')) {
        setSpotifyMessage('Spotify authentication failed during token exchange. Check SPOTIFY_REDIRECT_URI in your server .env and Spotify app settings.');
      } else {
        setSpotifyMessage('Spotify authentication failed. Please try again.');
      }
    }
  }, [searchParams]);

  useEffect(() => {
    const loadPlaylist = async () => {
      if (!id) return;
      
      setLoading(true);
      try {
        const response = await fetch(`/api/playlists/${id}`);
        if (!response.ok) {
          throw new Error('Playlist not found');
        }
        const data = await response.json();

        const canonicalSlug = slugifyTitle(data.title || '');
        if (slug && canonicalSlug && slug !== canonicalSlug) {
          navigate(`/playlist/${id}/${canonicalSlug}`, { replace: true });
          return;
        }
        if (slug && !canonicalSlug) {
          navigate(`/playlist/${id}`, { replace: true });
          return;
        }

        setPlaylist(data);
        setPrompt(data.prompt || '');
        setCreatedAt(data.created_at);

        const relatedResponse = await fetch(`/api/playlists/${id}/related`);
        if (relatedResponse.ok) {
          const relatedData = await relatedResponse.json();
          setRelatedPlaylists(relatedData);
        } else {
          setRelatedPlaylists([]);
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load playlist');
      } finally {
        setLoading(false);
      }
    };

    loadPlaylist();
  }, [id, slug, navigate]);

  if (loading) {
    return <p className="placeholder-text">Loading...</p>;
  }

  if (error) {
    return <p className="error">{error}</p>;
  }

  if (!playlist) {
    return <p className="error">Playlist not found</p>;
  }

  const { primary: primaryEquipment, related: relatedEquipment } = splitPrimaryAndRelatedEquipmentWithPrompt(playlist, prompt);

  const buildSpotifyMatchDetails = (data: Record<string, unknown> | null | undefined) => {
    const payload = data || {};
    return {
      addedTracks: typeof payload.addedTracks === 'number' ? payload.addedTracks : 0,
      matched: typeof payload.matched === 'number' ? payload.matched : 0,
      skipped: typeof payload.skipped === 'number' ? payload.skipped : 0,
      skippedTracksTotal: typeof payload.skippedTracksTotal === 'number'
        ? Math.max(0, Math.floor(payload.skippedTracksTotal))
        : (typeof payload.skipped === 'number' ? Math.max(0, Math.floor(payload.skipped)) : 0),
      duplicateUriMatches: typeof payload.duplicateUriMatches === 'number' ? payload.duplicateUriMatches : 0,
      uncertainMatches: typeof payload.uncertainMatches === 'number' ? payload.uncertainMatches : 0,
      uncertainTracks: Array.isArray(payload.uncertainTracks)
        ? payload.uncertainTracks
            .filter((item: unknown): item is { artist: string; song: string; score: number } => {
              return Boolean(
                item
                  && typeof item === 'object'
                  && typeof (item as { artist?: unknown }).artist === 'string'
                  && typeof (item as { song?: unknown }).song === 'string'
                  && typeof (item as { score?: unknown }).score === 'number'
              );
            })
            .slice(0, 20)
        : [],
      matchedTracksSample: Array.isArray(payload.matchedTracksSample)
        ? payload.matchedTracksSample
            .filter((item: unknown): item is { artist: string; song: string; source: string; score?: number | null } => {
              return Boolean(
                item
                  && typeof item === 'object'
                  && typeof (item as { artist?: unknown }).artist === 'string'
                  && typeof (item as { song?: unknown }).song === 'string'
                  && typeof (item as { source?: unknown }).source === 'string'
              );
            })
            .slice(0, 20)
        : [],
      skipReasonCounts: payload.skipReasonCounts && typeof payload.skipReasonCounts === 'object'
        ? Object.entries(payload.skipReasonCounts as Record<string, unknown>).reduce<Record<string, number>>((acc, [key, value]) => {
            if (typeof value === 'number' && Number.isFinite(value) && value > 0) {
              acc[key] = Math.floor(value);
            }
            return acc;
          }, {})
        : {},
      skippedTracks: Array.isArray(payload.skippedTracks)
        ? payload.skippedTracks
            .filter((item: unknown): item is { artist: string; song: string } => {
              return Boolean(
                item
                  && typeof item === 'object'
                  && typeof (item as { artist?: unknown }).artist === 'string'
                  && typeof (item as { song?: unknown }).song === 'string'
              );
            })
            .map((item) => ({
              artist: item.artist,
              song: item.song,
              reason: typeof (item as { reason?: unknown }).reason === 'string'
                ? String((item as { reason?: unknown }).reason)
                : undefined,
            }))
            .slice(0, 20)
        : [],
      matchSources: payload.matchSources && typeof payload.matchSources === 'object'
        ? payload.matchSources as { trackSpotifyUrl?: number; recordingSpotifyUrl?: number; isrc?: number; search?: number }
        : undefined,
      addTracksChunkStats: payload.addTracksChunkStats && typeof payload.addTracksChunkStats === 'object'
        ? payload.addTracksChunkStats as { totalChunks?: number; totalAttempts?: number; retriedChunks?: number }
        : undefined,
    };
  };

  const handleSaveToSpotify = async () => {
    console.log('Save to Spotify clicked');
    if (!id) {
      console.log('Save to Spotify aborted: missing playlist id');
      return;
    }

    setSpotifyState('saving');
    setSpotifyMessage(null);
    setSpotifyPlaylistUrl(null);
    setSpotifyMatchDetails(null);

    try {
      const response = await fetch(`/api/spotify/save-playlist/${id}`, {
        method: 'POST',
      });

      const data = await response.json();
      console.log('Save to Spotify response status:', response.status);
      console.log('Save to Spotify response body:', data);

      if (response.status === 401) {
        window.location.href = `/api/spotify/login?returnTo=${encodeURIComponent(window.location.pathname)}&returnOrigin=${encodeURIComponent(window.location.origin)}`;
        return;
      }

      if (!response.ok) {
        const details = buildSpotifyMatchDetails(data && typeof data === 'object' ? data as Record<string, unknown> : null);
        const partialSpotifyPlaylistUrl = typeof data?.spotifyPlaylistUrl === 'string' && data.spotifyPlaylistUrl.trim().length > 0
          ? data.spotifyPlaylistUrl.trim()
          : null;
        if (partialSpotifyPlaylistUrl) {
          setSpotifyPlaylistUrl(partialSpotifyPlaylistUrl);
        }
        const skipReasonCounts = data?.skipReasonCounts && typeof data.skipReasonCounts === 'object'
          ? Object.entries(data.skipReasonCounts as Record<string, unknown>)
              .filter(([, value]) => typeof value === 'number' && Number.isFinite(value) && value > 0)
              .map(([reason, value]) => `${reason.replace(/_/g, ' ')} (${Math.floor(value as number)})`)
          : [];
        const chunkFailureSummary = (
          typeof data?.addedBeforeFailure === 'number'
          && typeof data?.failedChunkIndex === 'number'
          && typeof data?.totalChunks === 'number'
        )
          ? `added ${Math.max(0, Math.floor(data.addedBeforeFailure))} tracks before failing in chunk ${Math.max(1, Math.floor(data.failedChunkIndex))}/${Math.max(1, Math.floor(data.totalChunks))}`
          : '';
        const chunkFailureDetail = (
          (typeof data?.failedChunkStatus === 'number' || typeof data?.failedChunkStatus === 'string')
          || typeof data?.failedChunkAttempt === 'number'
        )
          ? `chunk failure details: status ${String(data.failedChunkStatus ?? 'unknown')}, attempt ${typeof data?.failedChunkAttempt === 'number' ? Math.max(1, Math.floor(data.failedChunkAttempt)) : 'n/a'}`
          : '';
        const chunkFailureError = typeof data?.failedChunkError === 'string' && data.failedChunkError.trim().length > 0
          ? `chunk error: ${data.failedChunkError.trim()}`
          : '';
        const chunkRetrySummary = (
          typeof data?.addTracksAttemptsTotal === 'number'
          && typeof data?.addTracksChunksRetried === 'number'
        )
          ? `chunk attempts ${Math.max(0, Math.floor(data.addTracksAttemptsTotal))}, retries ${Math.max(0, Math.floor(data.addTracksChunksRetried))}`
          : '';
        const baseError = typeof data?.error === 'string' && data.error.trim().length > 0
          ? data.error.trim()
          : 'Failed to save playlist to Spotify';
        const detailParts = [
          partialSpotifyPlaylistUrl ? 'partial Spotify playlist was created' : '',
          chunkFailureSummary,
          chunkFailureDetail,
          chunkFailureError,
          chunkRetrySummary,
          skipReasonCounts.length > 0 ? `skip reasons: ${skipReasonCounts.join(', ')}` : '',
        ].filter((part) => part.length > 0);
        const detailedError = detailParts.length > 0
          ? `${baseError} — ${detailParts.join(' | ')}`
          : baseError;
        setSpotifyState('error');
        setSpotifyMessage(detailedError);
        setSpotifyMatchDetails(details);
        return;
      }

      setSpotifyState('success');
      if (typeof data.addedTracks === 'number' && typeof data.matched === 'number' && typeof data.skipped === 'number') {
        setSpotifyMessage(`Saved ${data.addedTracks} tracks to Spotify (${data.skipped} skipped).`);
      } else {
        setSpotifyMessage(`Saved ${data.addedTracks} tracks to Spotify.`);
      }
      setSpotifyPlaylistUrl(data.spotifyPlaylistUrl || null);
      setSpotifyMatchDetails(buildSpotifyMatchDetails(data && typeof data === 'object' ? data as Record<string, unknown> : null));
    } catch (err) {
      setSpotifyState('error');
      setSpotifyMessage(err instanceof Error ? err.message : 'Failed to save playlist to Spotify');
      setSpotifyMatchDetails(null);
    }
  };

  const handleCopyShareLink = async () => {
    if (!id || !playlist) return;

    try {
      const canonicalUrl = `${window.location.origin}${getPlaylistPath(Number(id), playlist.title)}`;
      await navigator.clipboard.writeText(canonicalUrl);
      setShareMessage('Link copied');
      window.setTimeout(() => setShareMessage(null), 2500);
    } catch {
      setShareMessage('Could not copy link');
      window.setTimeout(() => setShareMessage(null), 2500);
    }
  };

  return (
    <div className="playlist-page">
      <Link to="/" className="back-link">← Back to Generator</Link>
      
      <h2>{playlist.title}</h2>
      {playlist.tags && playlist.tags.length > 0 && (
        <div className="tags">
          {playlist.tags.map((tag, i) => (
            <Link key={i} to={`/tag/${encodeURIComponent(tag)}`} className="tag tag-link">{tag}</Link>
          ))}
        </div>
      )}
      <p className="description">{playlist.description}</p>
      
      <div className="meta">
        <p><strong>Prompt:</strong> {prompt}</p>
        <p><strong>Created:</strong> {new Date(createdAt).toLocaleString()}</p>
        {playlist.countries && playlist.countries.length > 0 && (
          <div>
            <p><strong>Countries:</strong></p>
            <div className="tags">
              {playlist.countries.map((country, i) => (
                <Link key={i} to={`/country/${encodeURIComponent(country)}`} className="tag tag-link">
                  {country}
                </Link>
              ))}
            </div>
          </div>
        )}
        {playlist.cities && playlist.cities.length > 0 && (
          <div>
            <p><strong>Cities:</strong></p>
            <div className="tags">
              {playlist.cities.map((city, i) => (
                <Link key={i} to={`/city/${encodeURIComponent(city)}`} className="tag tag-link">
                  {city}
                </Link>
              ))}
            </div>
          </div>
        )}
        {playlist.studios && playlist.studios.length > 0 && (
          <div>
            <p><strong>Studios:</strong></p>
            <div className="tags">
              {playlist.studios.map((studio, i) => (
                <Link key={i} to={`/studio/${encodeURIComponent(studio)}`} className="tag tag-link">
                  {studio}
                </Link>
              ))}
            </div>
          </div>
        )}
        {playlist.venues && playlist.venues.length > 0 && (
          <div>
            <p><strong>Venues:</strong></p>
            <div className="tags">
              {playlist.venues.map((venue, i) => (
                <Link key={i} to={`/venue/${encodeURIComponent(venue)}`} className="tag tag-link">
                  {venue}
                </Link>
              ))}
            </div>
          </div>
        )}
        {playlist.scenes && playlist.scenes.length > 0 && (
          <div>
            <p><strong>Scenes:</strong></p>
            <div className="tags">
              {playlist.scenes.map((scene, i) => (
                <Link key={i} to={`/scene/${encodeURIComponent(scene)}`} className="tag tag-link">
                  {scene}
                </Link>
              ))}
            </div>
          </div>
        )}
      </div>

      <div className="meta">
        <p><strong>Spotify</strong></p>
        <button type="button" onClick={handleSaveToSpotify} disabled={spotifyState === 'saving'}>
          {spotifyState === 'saving' ? 'Saving to Spotify...' : 'Save to Spotify'}
        </button>
        {spotifyMessage && <p>{spotifyMessage}</p>}
        {spotifyMatchDetails && (
          <>
            <p>
              Match sources: existing track URLs {spotifyMatchDetails.matchSources?.trackSpotifyUrl ?? 0},
              {' '}recording cache {spotifyMatchDetails.matchSources?.recordingSpotifyUrl ?? 0},
              {' '}ISRC {spotifyMatchDetails.matchSources?.isrc ?? 0},
              {' '}search {spotifyMatchDetails.matchSources?.search ?? 0}
            </p>
            {spotifyMatchDetails.addTracksChunkStats && (
              <p>
                Spotify add-tracks chunks: {spotifyMatchDetails.addTracksChunkStats.totalChunks ?? 0},
                {' '}attempts: {spotifyMatchDetails.addTracksChunkStats.totalAttempts ?? 0},
                {' '}retries: {spotifyMatchDetails.addTracksChunkStats.retriedChunks ?? 0}
              </p>
            )}
            {spotifyMatchDetails.matchedTracksSample.length > 0 && (
              <details className="verification-details">
                <summary>Matched tracks sample ({spotifyMatchDetails.matchedTracksSample.length})</summary>
                <ul className="playlist-list">
                  {spotifyMatchDetails.matchedTracksSample.map((track, idx) => (
                    <li key={`${track.artist}-${track.song}-matched-${idx}`} className="playlist-item">
                      {track.song} - {track.artist} ({track.source.replace(/_/g, ' ')}
                      {typeof track.score === 'number' && Number.isFinite(track.score) ? `, score ${track.score}` : ''})
                    </li>
                  ))}
                </ul>
              </details>
            )}
            {spotifyMatchDetails.duplicateUriMatches > 0 && (
              <p>{spotifyMatchDetails.duplicateUriMatches} matched tracks shared duplicate Spotify versions and were collapsed in Spotify export.</p>
            )}
            {spotifyMatchDetails.uncertainMatches > 0 && (
              <>
                <p>{spotifyMatchDetails.uncertainMatches} matches had low confidence (score &lt;= 0). Review uncertain/skip lists and rerun if needed.</p>
                {spotifyMatchDetails.uncertainTracks.length > 0 && (
                  <details className="verification-details">
                    <summary>Low-confidence tracks sample ({spotifyMatchDetails.uncertainTracks.length})</summary>
                    <ul className="playlist-list">
                      {spotifyMatchDetails.uncertainTracks.map((track, idx) => (
                        <li key={`${track.artist}-${track.song}-uncertain-${idx}`} className="playlist-item">
                          {track.song} - {track.artist} (score {track.score})
                        </li>
                      ))}
                    </ul>
                  </details>
                )}
              </>
            )}
            {Object.keys(spotifyMatchDetails.skipReasonCounts).length > 0 && (
              <p>
                Skip reasons:{' '}
                {Object.entries(spotifyMatchDetails.skipReasonCounts)
                  .map(([reason, count]) => `${reason.replace(/_/g, ' ')} (${count})`)
                  .join(', ')}
              </p>
            )}
            {spotifyMatchDetails.skippedTracks.length > 0 && (
              <details className="verification-details">
                <summary>
                  Skipped tracks sample ({spotifyMatchDetails.skippedTracks.length}
                  {spotifyMatchDetails.skippedTracksTotal > spotifyMatchDetails.skippedTracks.length
                    ? ` of ${spotifyMatchDetails.skippedTracksTotal}`
                    : ''})
                </summary>
                <ul className="playlist-list">
                  {spotifyMatchDetails.skippedTracks.map((track, idx) => (
                    <li key={`${track.artist}-${track.song}-${idx}`} className="playlist-item">
                      {track.song} - {track.artist}{track.reason ? ` (${track.reason.replace(/_/g, ' ')})` : ''}
                    </li>
                  ))}
                </ul>
              </details>
            )}
          </>
        )}
        {spotifyPlaylistUrl && (
          <p>
            <a href={spotifyPlaylistUrl} target="_blank" rel="noopener noreferrer" className="spotify-link">
              Open created Spotify playlist
            </a>
          </p>
        )}
      </div>

      <div className="meta">
        <p><strong>Share</strong></p>
        <button type="button" onClick={handleCopyShareLink}>Copy link</button>
        {shareMessage && <p>{shareMessage}</p>}
      </div>

      {playlist.influences && playlist.influences.length > 0 && (
        <div className="meta">
          <p><strong>Artist connections in this playlist</strong></p>
          <p className="playlist-date">Some artists in this playlist are linked through influence relationships in the atlas.</p>
          {playlist.influences.map((edge, index) => (
            <p key={`${edge.from}-${edge.to}-${index}`}>
              <Link to={`/artist/${encodeURIComponent(edge.from)}`} className="artist-link">{edge.from}</Link>{' '}influenced{' '}<Link to={`/artist/${encodeURIComponent(edge.to)}`} className="artist-link">{edge.to}</Link>
            </p>
          ))}
        </div>
      )}

      {playlist.credits && playlist.credits.length > 0 && (
        <div className="meta">
          <p><strong>Credits</strong></p>
          {playlist.credits.map((credit, index) => (
            <p key={`${credit.name}-${credit.role}-${index}`}>
              <Link to={`/credit/${encodeURIComponent(credit.name)}`} className="tag-link">{credit.name}</Link>{' - '}{formatCreditRole(credit.role)}
            </p>
          ))}
        </div>
      )}

      {primaryEquipment && (
        <div className="meta">
          <p><strong>Primary equipment</strong></p>
          <p>
            <Link to={`/equipment/${encodeURIComponent(primaryEquipment.name)}`} className="artist-link">{primaryEquipment.name}</Link>{' - '}{primaryEquipment.category.replace(/_/g, ' ')}
          </p>
          {relatedEquipment.length > 0 && (
            <>
              <p><strong>Related equipment</strong></p>
              {relatedEquipment.map((item, index) => (
                <p key={`${item.name}-${item.category}-${index}`}>
                  <Link to={`/equipment/${encodeURIComponent(item.name)}`} className="artist-link">{item.name}</Link>{' - '}{item.category.replace(/_/g, ' ')}
                </p>
              ))}
            </>
          )}
        </div>
      )}

      <ul className="tracks">
        {playlist.tracks.map((track, index) => (
          <li key={index} className="track">
            {track.album_image_url && (
              <img src={track.album_image_url} alt="Album" className="album-art" />
            )}
            <div className="track-info">
              <span className="track-title">{track.song}</span>
              <span className="track-artist">
                by <Link to={`/artist/${encodeURIComponent(track.artist)}`} className="artist-link">{track.artist}</Link>
              </span>
              <p className="track-reason">{track.reason}</p>
              {track.spotify_url && (
                <a href={track.spotify_url} target="_blank" rel="noopener noreferrer" className="spotify-link">
                  Open in Spotify
                </a>
              )}
            </div>
          </li>
        ))}
      </ul>

      {relatedPlaylists.length > 0 && (
        <div className="related-playlists">
          <h3>Related playlists</h3>
          <ul className="playlist-list">
            {relatedPlaylists.map((p) => (
              <li key={p.id} className="playlist-item">
                <Link to={getPlaylistPath(p.id, p.title)} className="playlist-link">
                  <span className="playlist-title">{p.title}</span>
                  <span className="playlist-prompt">{p.prompt}</span>
                  <span className="playlist-date">{new Date(p.created_at).toLocaleDateString()}</span>
                </Link>
                {parseTagList(p.tags).length > 0 && (
                  <div className="tags">
                    {parseTagList(p.tags).slice(0, 3).map((tag, i) => (
                      <Link key={i} to={`/tag/${encodeURIComponent(tag)}`} className="tag tag-link">{tag}</Link>
                    ))}
                  </div>
                )}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

function TagPage() {
  const { tag } = useParams<{ tag: string }>();
  const [playlists, setPlaylists] = useState<PlaylistListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadByTag = async () => {
      if (!tag) return;

      setLoading(true);
      setError(null);

      try {
        const response = await fetch(`/api/tags/${encodeURIComponent(tag)}`);
        if (!response.ok) {
          throw new Error('Failed to load tag page');
        }
        const data = await response.json();
        setPlaylists(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load tag page');
      } finally {
        setLoading(false);
      }
    };

    loadByTag();
  }, [tag]);

  if (loading) {
    return <p className="placeholder-text">Loading...</p>;
  }

  if (error) {
    return <p className="error">{error}</p>;
  }

  return (
    <div className="tag-page">
      <Link to="/" className="back-link">← Back to Generator</Link>
      <h2>Tag: {tag}</h2>

      {playlists.length === 0 ? (
        <p className="placeholder-text">No playlists found for this tag.</p>
      ) : (
        <ul className="playlist-list">
          {playlists.map((p) => (
            <li key={p.id} className="playlist-item">
              <Link to={getPlaylistPath(p.id, p.title)} className="playlist-link">
                <span className="playlist-title">{p.title}</span>
                <span className="playlist-prompt">{p.prompt}</span>
                <span className="playlist-date">{new Date(p.created_at).toLocaleDateString()}</span>
              </Link>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function PlacePage() {
  const { place } = useParams<{ place: string }>();
  const [playlists, setPlaylists] = useState<PlaylistListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadByPlace = async () => {
      if (!place) return;

      setLoading(true);
      setError(null);

      try {
        const response = await fetch(`/api/places/${encodeURIComponent(place)}`);
        if (!response.ok) {
          throw new Error('Failed to load place page');
        }
        const data = await response.json();
        setPlaylists(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load place page');
      } finally {
        setLoading(false);
      }
    };

    loadByPlace();
  }, [place]);

  if (loading) {
    return <p className="placeholder-text">Loading...</p>;
  }

  if (error) {
    return <p className="error">{error}</p>;
  }

  return (
    <div className="place-page">
      <Link to="/" className="back-link">← Back to Generator</Link>
      <h2>Place: {place}</h2>

      {playlists.length === 0 ? (
        <p className="placeholder-text">No playlists found for this place.</p>
      ) : (
        <ul className="playlist-list">
          {playlists.map((p) => (
            <li key={p.id} className="playlist-item">
              <Link to={getPlaylistPath(p.id, p.title)} className="playlist-link">
                <span className="playlist-title">{p.title}</span>
                <span className="playlist-prompt">{p.prompt}</span>
                <span className="playlist-date">{new Date(p.created_at).toLocaleDateString()}</span>
              </Link>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function ScenePage() {
  const { scene } = useParams<{ scene: string }>();
  const [playlists, setPlaylists] = useState<PlaylistListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadByScene = async () => {
      if (!scene) return;

      setLoading(true);
      setError(null);

      try {
        const response = await fetch(`/api/scenes/${encodeURIComponent(scene)}`);
        if (!response.ok) {
          throw new Error('Failed to load scene page');
        }
        const data = await response.json();
        setPlaylists(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load scene page');
      } finally {
        setLoading(false);
      }
    };

    loadByScene();
  }, [scene]);

  if (loading) {
    return <p className="placeholder-text">Loading...</p>;
  }

  if (error) {
    return <p className="error">{error}</p>;
  }

  const relatedCountries = collectTopFromPlaylists(playlists, 'countries');
  const relatedCities = collectTopFromPlaylists(playlists, 'cities');
  const relatedStudios = collectTopFromPlaylists(playlists, 'studios');
  const relatedVenues = collectTopFromPlaylists(playlists, 'venues');
  const relatedScenes = collectTopFromPlaylists(playlists, 'scenes', 8, scene ?? '');
  const exploreTargets = pickExploreTargets(
    [
      { type: 'artist', values: [] },
      { type: 'scene', values: relatedScenes },
      { type: 'country', values: relatedCountries },
      { type: 'city', values: relatedCities },
      { type: 'studio', values: relatedStudios },
      { type: 'venue', values: relatedVenues },
    ],
    'scene',
    scene ?? '',
    5
  );

  return (
    <div className="scene-page">
      <Link to="/" className="back-link">← Back to Generator</Link>
      <h2>Scene: {scene}</h2>
      {scene && (
        <div className="tags">
          <Link to={`/connect?fromType=scene&fromValue=${encodeURIComponent(scene)}`} className="tag tag-link">
            Find connection from this node
          </Link>
        </div>
      )}

      <AtlasConnections
        groups={[
          { label: 'Countries', values: relatedCountries, to: (value) => `/country/${encodeURIComponent(value)}` },
          { label: 'Cities', values: relatedCities, to: (value) => `/city/${encodeURIComponent(value)}` },
          { label: 'Studios', values: relatedStudios, to: (value) => `/studio/${encodeURIComponent(value)}` },
          { label: 'Venues', values: relatedVenues, to: (value) => `/venue/${encodeURIComponent(value)}` },
          { label: 'Scenes', values: relatedScenes, to: (value) => `/scene/${encodeURIComponent(value)}` },
          { label: 'Artists', values: [], to: (value) => `/artist/${encodeURIComponent(value)}` },
        ]}
      />

      <ExplorePaths fromType="scene" fromValue={scene ?? ''} targets={exploreTargets} />

      {playlists.length === 0 ? (
        <p className="placeholder-text">No playlists found for this scene.</p>
      ) : (
        <ul className="playlist-list">
          {playlists.map((p) => (
            <li key={p.id} className="playlist-item">
              <Link to={getPlaylistPath(p.id, p.title)} className="playlist-link">
                <span className="playlist-title">{p.title}</span>
                <span className="playlist-prompt">{p.prompt}</span>
                <span className="playlist-date">{new Date(p.created_at).toLocaleDateString()}</span>
              </Link>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function LocationNodePage({
  nodeName,
  apiPath,
  title,
  nodeType,
  showExplorePaths = true,
}: {
  nodeName: string;
  apiPath: string;
  title: string;
  nodeType: 'country' | 'city' | 'studio' | 'venue';
  showExplorePaths?: boolean;
}) {
  const [data, setData] = useState<LocationAtlasResponse>({
    playlists: [],
    scenes: [],
    relatedArtists: [],
    relatedCountries: [],
    relatedCities: [],
    relatedStudios: [],
    relatedVenues: [],
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadNode = async () => {
      if (!nodeName) return;

      setLoading(true);
      setError(null);

      try {
        const response = await fetch(apiPath);
        if (!response.ok) {
          throw new Error(`Failed to load ${title.toLowerCase()} page`);
        }
        const payload = await response.json() as LocationAtlasResponse;
        setData({
          playlists: Array.isArray(payload.playlists) ? payload.playlists : [],
          scenes: Array.isArray(payload.scenes) ? payload.scenes : [],
          relatedArtists: Array.isArray(payload.relatedArtists) ? payload.relatedArtists : [],
          relatedCountries: Array.isArray(payload.relatedCountries) ? payload.relatedCountries : [],
          relatedCities: Array.isArray(payload.relatedCities) ? payload.relatedCities : [],
          relatedStudios: Array.isArray(payload.relatedStudios) ? payload.relatedStudios : [],
          relatedVenues: Array.isArray(payload.relatedVenues) ? payload.relatedVenues : [],
          relatedEquipment: Array.isArray(payload.relatedEquipment) ? payload.relatedEquipment : [],
          city: typeof payload.city === 'string' ? payload.city : null,
          country: typeof payload.country === 'string' ? payload.country : null,
        });
      } catch (err) {
        setError(err instanceof Error ? err.message : `Failed to load ${title.toLowerCase()} page`);
      } finally {
        setLoading(false);
      }
    };

    loadNode();
  }, [nodeName, apiPath, title]);

  if (loading) {
    return <p className="placeholder-text">Loading...</p>;
  }

  if (error) {
    return <p className="error">{error}</p>;
  }

  const exploreTargets = pickExploreTargets(
    [
      { type: 'artist', values: data.relatedArtists },
      { type: 'scene', values: data.scenes },
      { type: 'country', values: data.relatedCountries },
      { type: 'city', values: data.relatedCities },
      { type: 'studio', values: data.relatedStudios },
      { type: 'venue', values: data.relatedVenues },
    ],
    nodeType,
    nodeName,
    5
  );

  const displayCountries = nodeType === 'studio'
    ? (data.country ? [data.country] : [])
    : data.relatedCountries;
  const displayCities = nodeType === 'studio'
    ? (data.city ? [data.city] : [])
    : data.relatedCities;

  return (
    <div className="location-node-page">
      <Link to="/" className="back-link">← Back to Generator</Link>
      <h2>{title}: {nodeName}</h2>
      {nodeName && (
        <div className="tags">
          <Link to={`/connect?fromType=${nodeType}&fromValue=${encodeURIComponent(nodeName)}`} className="tag tag-link">
            Find connection from this node
          </Link>
        </div>
      )}

      <AtlasConnections
        groups={[
          { label: 'Countries', values: displayCountries, to: (value) => `/country/${encodeURIComponent(value)}` },
          { label: 'Cities', values: displayCities, to: (value) => `/city/${encodeURIComponent(value)}` },
          { label: 'Studios', values: nodeType === 'studio' ? [] : data.relatedStudios, to: (value) => `/studio/${encodeURIComponent(value)}` },
          { label: 'Venues', values: nodeType === 'studio' ? [] : data.relatedVenues, to: (value) => `/venue/${encodeURIComponent(value)}` },
          { label: 'Scenes', values: data.scenes, to: (value) => `/scene/${encodeURIComponent(value)}` },
          { label: 'Related artists', values: data.relatedArtists, to: (value) => `/artist/${encodeURIComponent(value)}` },
        ]}
      />

      {nodeType === 'studio' && Array.isArray(data.relatedEquipment) && data.relatedEquipment.length > 0 && (
        <div className="meta">
          <p><strong>Equipment</strong></p>
          <div className="tags">
            {data.relatedEquipment.map((item) => (
              <Link key={item} to={`/equipment/${encodeURIComponent(item)}`} className="tag tag-link">{item}</Link>
            ))}
          </div>
        </div>
      )}

      {showExplorePaths && <ExplorePaths fromType={nodeType} fromValue={nodeName} targets={exploreTargets} />}

      {data.playlists.length === 0 ? (
        <p className="placeholder-text">No playlists found for this node.</p>
      ) : (
        <>
          {nodeType === 'studio' && <h3>Related playlists</h3>}
          <ul className="playlist-list">
            {data.playlists.map((p) => (
              <li key={p.id} className="playlist-item">
                <Link to={getPlaylistPath(p.id, p.title)} className="playlist-link">
                  <span className="playlist-title">{p.title}</span>
                  <span className="playlist-prompt">{p.prompt}</span>
                  <span className="playlist-date">{new Date(p.created_at).toLocaleDateString()}</span>
                </Link>
              </li>
            ))}
          </ul>
        </>
      )}
    </div>
  );
}

function CountryPage() {
  const { country } = useParams<{ country: string }>();
  return <LocationNodePage nodeName={country ?? ''} apiPath={`/api/countries/${encodeURIComponent(country ?? '')}`} title="Country" nodeType="country" />;
}

function CityPage() {
  const { city } = useParams<{ city: string }>();
  return <LocationNodePage nodeName={city ?? ''} apiPath={`/api/cities/${encodeURIComponent(city ?? '')}`} title="City" nodeType="city" />;
}

function StudioPage() {
  const { studio } = useParams<{ studio: string }>();
  return <LocationNodePage nodeName={studio ?? ''} apiPath={`/api/studios/${encodeURIComponent(studio ?? '')}`} title="Studio" nodeType="studio" showExplorePaths={false} />;
}

function VenuePage() {
  const { venue } = useParams<{ venue: string }>();
  return <LocationNodePage nodeName={venue ?? ''} apiPath={`/api/venues/${encodeURIComponent(venue ?? '')}`} title="Venue" nodeType="venue" />;
}

function ArtistPage() {
  const { artist } = useParams<{ artist: string }>();
  const [data, setData] = useState<ArtistAtlasResponse>({ playlists: [], scenes: [], places: [], memberOf: [], relatedStudios: [], relatedArtists: [], relatedCredits: [], relatedEquipment: [] });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadArtist = async () => {
      if (!artist) return;

      setLoading(true);
      setError(null);

      try {
        const response = await fetch(`/api/artists/${encodeURIComponent(artist)}`);
        if (!response.ok) {
          throw new Error('Failed to load artist page');
        }
        const payload = await response.json();
        setData(payload);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load artist page');
      } finally {
        setLoading(false);
      }
    };

    loadArtist();
  }, [artist]);

  if (loading) {
    return <p className="placeholder-text">Loading...</p>;
  }

  if (error) {
    return <p className="error">{error}</p>;
  }

  return (
    <div className="artist-page">
      <Link to="/" className="back-link">← Back to Generator</Link>
      <h2>Artist: {artist}</h2>
      {artist && (
        <div className="tags">
          <Link to={`/connect?fromType=artist&fromValue=${encodeURIComponent(artist)}`} className="tag tag-link">
            Find connection from this node
          </Link>
        </div>
      )}

      <AtlasConnections
        groups={[
          { label: 'Studios', values: data.relatedStudios || [], to: (value) => `/studio/${encodeURIComponent(value)}` },
          {
            label: 'Related artists',
            values: data.relatedArtists,
            to: (value) => `/artist/${encodeURIComponent(value)}`,
            subtitle: data.relatedArtistsEvidence
              ? `Connections are based on repeated ${data.relatedArtistsEvidence.relationType} evidence (max ${data.relatedArtistsEvidence.evidenceCount}).`
              : 'Ranked using repeated atlas and playlist evidence.',
          },
        ]}
      />

      {data.memberOf && data.memberOf.length > 0 && (
        <div className="meta">
          <p><strong>Member of</strong></p>
          <div className="tags">
            {data.memberOf.map((band) => (
              <Link key={band} to={`/artist/${encodeURIComponent(band)}`} className="tag tag-link">
                {band}
              </Link>
            ))}
          </div>
        </div>
      )}

      {data.relatedEquipment && data.relatedEquipment.length > 0 && (
        <div className="meta">
          <p><strong>Equipment</strong></p>
          <div className="tags">
            {data.relatedEquipment.map((equipment) => (
              <Link key={equipment} to={`/equipment/${encodeURIComponent(equipment)}`} className="tag tag-link">
                {equipment}
              </Link>
            ))}
          </div>
        </div>
      )}

      {data.relatedCredits.length > 0 && (
        <div className="meta">
          <p><strong>Related credits</strong></p>
          <div className="tags">
            {data.relatedCredits.map((credit) => (
              <Link key={credit.name} to={`/credit/${encodeURIComponent(credit.name)}`} className="tag tag-link">
                {credit.name}
              </Link>
            ))}
          </div>
        </div>
      )}

      <h3>Playlists containing this artist</h3>
      {data.playlists.length === 0 ? (
        <p className="placeholder-text">No playlists found for this artist.</p>
      ) : (
        <ul className="playlist-list">
          {data.playlists.map((p) => (
            <li key={p.id} className="playlist-item">
              <Link to={getPlaylistPath(p.id, p.title)} className="playlist-link">
                <span className="playlist-title">{p.title}</span>
                <span className="playlist-prompt">{p.prompt}</span>
                <span className="playlist-date">{new Date(p.created_at).toLocaleDateString()}</span>
              </Link>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function CreditPage() {
  const { name } = useParams<{ name: string }>();
  const [data, setData] = useState<CreditAtlasResponse>({ name: name ?? '', roles: [], primaryRoles: [], playlists: [], relatedArtists: [], memberOf: [], associatedStudios: [] });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadCredit = async () => {
      if (!name) return;

      setLoading(true);
      setError(null);

      try {
        const response = await fetch(`/api/credits/${encodeURIComponent(name)}`);
        if (!response.ok) {
          throw new Error('Failed to load credit page');
        }
        const payload = await response.json() as CreditAtlasResponse;
        setData(payload);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load credit page');
      } finally {
        setLoading(false);
      }
    };

    loadCredit();
  }, [name]);

  if (loading) {
    return <p className="placeholder-text">Loading...</p>;
  }

  if (error) {
    return <p className="error">{error}</p>;
  }

  return (
    <div className="artist-page">
      <Link to="/" className="back-link">← Back to Generator</Link>
      <h2>Credit: {data.name || name}</h2>

      {data.primaryRoles && data.primaryRoles.length > 0 && (
        <div className="meta">
          <p><strong>Primary roles</strong></p>
          <div className="tags">
            {data.primaryRoles.map((role) => (
              <span key={role} className="tag">{formatCreditRole(role)}</span>
            ))}
          </div>
        </div>
      )}

      <div className="meta">
        <p><strong>Recording-related roles</strong></p>
        {data.roles.length === 0 ? (
          <p className="placeholder-text">No roles found.</p>
        ) : (
          <div className="tags">
            {data.roles.map((role) => (
              <span key={role} className="tag">{formatCreditRole(role)}</span>
            ))}
          </div>
        )}
      </div>

      {data.relatedArtists.length > 0 && (
        <div className="meta">
          <p><strong>Related artists</strong></p>
          <div className="tags">
            {data.relatedArtists.map((artist) => (
              <Link key={artist} to={`/artist/${encodeURIComponent(artist)}`} className="tag tag-link">
                {artist}
              </Link>
            ))}
          </div>
        </div>
      )}

      {data.associatedStudios && data.associatedStudios.length > 0 && (
        <div className="meta">
          <p><strong>Associated with these studios</strong></p>
          <div className="tags">
            {data.associatedStudios.map((studio) => (
              <Link key={studio} to={`/studio/${encodeURIComponent(studio)}`} className="tag tag-link">
                {studio}
              </Link>
            ))}
          </div>
        </div>
      )}

      <h3>Playlists containing this credit</h3>
      {data.playlists.length === 0 ? (
        <p className="placeholder-text">No playlists found for this credit.</p>
      ) : (
        <ul className="playlist-list">
          {data.playlists.map((p) => (
            <li key={p.id} className="playlist-item">
              <Link to={getPlaylistPath(p.id, p.title)} className="playlist-link">
                <span className="playlist-title">{p.title}</span>
                <span className="playlist-prompt">{p.prompt}</span>
                <span className="playlist-date">{new Date(p.created_at).toLocaleDateString()}</span>
              </Link>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function EquipmentPage() {
  const { name } = useParams<{ name: string }>();
  const [data, setData] = useState<EquipmentAtlasResponse>({
    name: name ?? '',
    category: '',
    playlists: [],
    relatedArtists: [],
    relatedScenes: [],
    relatedStudios: [],
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadEquipment = async () => {
      if (!name) return;

      setLoading(true);
      setError(null);

      try {
        const response = await fetch(`/api/equipment/${encodeURIComponent(name)}`);
        if (!response.ok) {
          throw new Error('Failed to load equipment page');
        }
        const payload = await response.json() as EquipmentAtlasResponse;
        setData(payload);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load equipment page');
      } finally {
        setLoading(false);
      }
    };

    loadEquipment();
  }, [name]);

  if (loading) {
    return <p className="placeholder-text">Loading...</p>;
  }

  if (error) {
    return <p className="error">{error}</p>;
  }

  return (
    <div className="artist-page">
      <Link to="/" className="back-link">← Back to Generator</Link>
      <h2>Equipment: {data.name || name}</h2>
      {data.category && <p className="playlist-date">Category: {data.category.replace(/_/g, ' ')}</p>}

      <AtlasConnections
        groups={[
          {
            label: 'Artists',
            values: data.relatedArtists,
            to: (value) => `/artist/${encodeURIComponent(value)}`,
            subtitle: data.relatedArtistsEvidence
              ? `Connections are based on repeated ${data.relatedArtistsEvidence.relationType} evidence (max ${data.relatedArtistsEvidence.evidenceCount}).`
              : 'Ranked using repeated atlas and recording evidence.',
          },
          { label: 'Scenes', values: data.relatedScenes, to: (value) => `/scene/${encodeURIComponent(value)}` },
          {
            label: 'Studios',
            values: data.relatedStudios,
            to: (value) => `/studio/${encodeURIComponent(value)}`,
            subtitle: data.relatedStudiosEvidence
              ? `Connections are based on repeated studio evidence (max ${data.relatedStudiosEvidence.evidenceCount}).`
              : 'Ranked using repeated atlas and recording evidence.',
          },
        ]}
      />

      <h3>Playlists containing this equipment</h3>
      {data.playlists.length === 0 ? (
        <p className="placeholder-text">No playlists found for this equipment.</p>
      ) : (
        <ul className="playlist-list">
          {data.playlists.map((p) => (
            <li key={p.id} className="playlist-item">
              <Link to={getPlaylistPath(p.id, p.title)} className="playlist-link">
                <span className="playlist-title">{p.title}</span>
                <span className="playlist-prompt">{p.prompt}</span>
                <span className="playlist-date">{new Date(p.created_at).toLocaleDateString()}</span>
              </Link>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function ConnectPage() {
  const [searchParams] = useSearchParams();
  const initialValue = searchParams.get('fromValue') || '';
  const [nodeQuery, setNodeQuery] = useState('');
  const [nodeSuggestions, setNodeSuggestions] = useState<ConnectSuggestion[]>([]);
  const [selectedNode, setSelectedNode] = useState<ConnectSuggestion | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!initialValue.trim()) return;
    setNodeQuery(initialValue);
  }, [initialValue]);

  useEffect(() => {
    const loadSuggestions = async () => {
      const query = nodeQuery.trim();
      if (query.length < 2) {
        setNodeSuggestions([]);
        return;
      }

      try {
        const response = await fetch(`/api/connect/suggest?q=${encodeURIComponent(query)}`);
        if (!response.ok) {
          setNodeSuggestions([]);
          return;
        }
        const data = await response.json() as ConnectSuggestion[];
        setNodeSuggestions(Array.isArray(data) ? data : []);
      } catch {
        setNodeSuggestions([]);
      }
    };

    loadSuggestions();
  }, [nodeQuery]);

  return (
    <div className="connect-page">
      <Link to="/" className="back-link">← Back to Generator</Link>
      <h2>Search atlas nodes</h2>

      <div className="prompt-form">
        <input
          value={nodeQuery}
          onChange={(e) => {
            setNodeQuery(e.target.value);
            setSelectedNode(null);
          }}
          placeholder="Search a node (e.g. David Bowie, Hansa Studios)"
        />
        {nodeSuggestions.length > 0 && (
          <div className="tags">
            {nodeSuggestions.map((suggestion) => (
              <button
                key={`node-${suggestion.type}-${suggestion.value}`}
                type="button"
                className="tag"
                onClick={() => {
                  setSelectedNode(suggestion);
                  setNodeQuery(suggestion.value);
                  setNodeSuggestions([]);
                  setError(null);
                }}
              >
                {suggestion.value} ({suggestion.type})
              </button>
            ))}
          </div>
        )}
      </div>

      {selectedNode && (
        <div className="meta">
          <p><strong>Selected node:</strong> {selectedNode.value} ({selectedNode.type})</p>
          <p><strong>Open node page</strong></p>
          <div className="tags">
            <Link
              to={getNodeRoute(selectedNode.type, selectedNode.value)}
              className="tag tag-link"
            >
              View {selectedNode.type} page
            </Link>
          </div>
        </div>
      )}

      {error && <p className="error">{error}</p>}
    </div>
  );
}

function App() {
  return (
    <div className="container">
      <h1>🎵 AI Playlist</h1>
      <div className="tags">
        <Link to="/connect" className="tag tag-link">Search atlas nodes</Link>
      </div>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/connect" element={<ConnectPage />} />
        <Route path="/playlist/:id" element={<PlaylistPage />} />
        <Route path="/playlist/:id/:slug" element={<PlaylistPage />} />
        <Route path="/tag/:tag" element={<TagPage />} />
        <Route path="/place/:place" element={<PlacePage />} />
        <Route path="/scene/:scene" element={<ScenePage />} />
        <Route path="/country/:country" element={<CountryPage />} />
        <Route path="/city/:city" element={<CityPage />} />
        <Route path="/studio/:studio" element={<StudioPage />} />
        <Route path="/venue/:venue" element={<VenuePage />} />
        <Route path="/artist/:artist" element={<ArtistPage />} />
        <Route path="/credit/:name" element={<CreditPage />} />
        <Route path="/equipment/:name" element={<EquipmentPage />} />
      </Routes>
    </div>
  );
}

export default App;
