import Database from 'better-sqlite3';

interface CountRow {
  count: number;
}

interface NameCountRow {
  name: string;
  count: number;
}

interface StudioAliasGroup {
  key: string;
  variants: string[];
  totalCount: number;
}

function hasColumn(db: Database.Database, table: string, column: string): boolean {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string | null }>;
  return rows.some((row) => typeof row.name === 'string' && row.name === column);
}

function readCount(db: Database.Database, table: string): number {
  const row = db.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get() as CountRow;
  return Number.isFinite(row?.count) ? row.count : 0;
}

function readTopNames(db: Database.Database, sql: string): NameCountRow[] {
  const rows = db.prepare(sql).all() as Array<{ name: string | null; count: number | null }>;
  return rows
    .map((row) => ({
      name: typeof row.name === 'string' ? row.name.trim() : '',
      count: typeof row.count === 'number' ? row.count : 0,
    }))
    .filter((row) => row.name.length > 0);
}

function normalizeStudioAliasKey(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/^the\s+/, '')
    .replace(/\brecording\s+studios?\b/g, 'studio')
    .replace(/\bstudios\b/g, 'studio')
    .replace(/\s+/g, ' ')
    .trim();
}

function readStudioAliasGroups(db: Database.Database, limit = 10): StudioAliasGroup[] {
  const rows = db.prepare(`
    SELECT studio_name AS name, COUNT(*) AS count
    FROM recording_studio_evidence
    WHERE studio_name IS NOT NULL
      AND trim(studio_name) != ''
    GROUP BY lower(trim(studio_name))
    ORDER BY count DESC
  `).all() as Array<{ name: string | null; count: number | null }>;

  const groups = new Map<string, { variants: Map<string, string>; totalCount: number }>();

  for (const row of rows) {
    const name = typeof row.name === 'string' ? row.name.trim() : '';
    const count = typeof row.count === 'number' ? row.count : 0;
    if (!name || count <= 0) continue;

    const key = normalizeStudioAliasKey(name);
    if (!key) continue;

    const existing = groups.get(key) || { variants: new Map<string, string>(), totalCount: 0 };
    existing.totalCount += count;
    const variantKey = name.toLowerCase();
    if (!existing.variants.has(variantKey)) {
      existing.variants.set(variantKey, name);
    }
    groups.set(key, existing);
  }

  return Array.from(groups.entries())
    .map(([key, value]) => ({
      key,
      variants: Array.from(value.variants.values()).sort((a, b) => a.localeCompare(b)),
      totalCount: value.totalCount,
    }))
    .filter((item) => item.variants.length > 1)
    .sort((a, b) => {
      if (b.variants.length !== a.variants.length) return b.variants.length - a.variants.length;
      if (b.totalCount !== a.totalCount) return b.totalCount - a.totalCount;
      return a.key.localeCompare(b.key);
    })
    .slice(0, limit);
}

function run(): void {
  const db = new Database('playlists.db');
  const hasMembershipCanonicalColumns = hasColumn(db, 'artist_membership_evidence', 'band_name_canonical')
    && hasColumn(db, 'artist_membership_evidence', 'person_name_canonical');
  const hasCreditCanonicalColumn = hasColumn(db, 'recording_credit_evidence', 'credit_name_canonical');
  const hasStudioCanonicalColumn = hasColumn(db, 'recording_studio_evidence', 'studio_name_canonical');

  const tableCounts: Array<{ table: string; count: number }> = [
    { table: 'playlists', count: readCount(db, 'playlists') },
    { table: 'recordings', count: readCount(db, 'recordings') },
    { table: 'recording_equipment_evidence', count: readCount(db, 'recording_equipment_evidence') },
    { table: 'recording_studio_evidence', count: readCount(db, 'recording_studio_evidence') },
    { table: 'recording_credit_evidence', count: readCount(db, 'recording_credit_evidence') },
    { table: 'artist_membership_evidence', count: readCount(db, 'artist_membership_evidence') },
    { table: 'atlas_edges', count: readCount(db, 'atlas_edges') },
    { table: 'atlas_node_stats', count: readCount(db, 'atlas_node_stats') },
  ];

  const topBands = readTopNames(
    db,
    `
      SELECT band_name AS name, COUNT(*) AS count
      FROM artist_membership_evidence
      GROUP BY lower(trim(band_name))
      ORDER BY count DESC, band_name ASC
      LIMIT 10
    `
  );

  const topMembers = readTopNames(
    db,
    `
      SELECT person_name AS name, COUNT(*) AS count
      FROM artist_membership_evidence
      GROUP BY lower(trim(person_name))
      ORDER BY count DESC, person_name ASC
      LIMIT 10
    `
  );

  const topCredits = readTopNames(
    db,
    `
      SELECT credit_name AS name, COUNT(*) AS count
      FROM recording_credit_evidence
      GROUP BY lower(trim(credit_name))
      ORDER BY count DESC, credit_name ASC
      LIMIT 10
    `
  );

  const topStudios = readTopNames(
    db,
    `
      SELECT studio_name AS name, COUNT(*) AS count
      FROM recording_studio_evidence
      GROUP BY lower(trim(studio_name))
      ORDER BY count DESC, studio_name ASC
      LIMIT 10
    `
  );

  const missingMembershipCanonical = hasMembershipCanonicalColumns
    ? readCount(
      db,
      `(
        SELECT 1
        FROM artist_membership_evidence
        WHERE band_name_canonical IS NULL
          OR trim(band_name_canonical) = ''
          OR person_name_canonical IS NULL
          OR trim(person_name_canonical) = ''
      )`
    )
    : -1;

  const membershipCanonicalCollisions = hasMembershipCanonicalColumns
    ? readCount(
      db,
      `(
        SELECT 1
        FROM artist_membership_evidence
        GROUP BY band_name_canonical, person_name_canonical
        HAVING COUNT(DISTINCT lower(trim(band_name)) || '::' || lower(trim(person_name))) > 1
      )`
    )
    : -1;

  const creditCanonicalCollisions = hasCreditCanonicalColumn
    ? readCount(
      db,
      `(
        SELECT 1
        FROM recording_credit_evidence
        GROUP BY credit_name_canonical
        HAVING COUNT(DISTINCT lower(trim(credit_name))) > 1
      )`
    )
    : -1;

  const studioCanonicalCollisions = hasStudioCanonicalColumn
    ? readCount(
      db,
      `(
        SELECT 1
        FROM recording_studio_evidence
        GROUP BY studio_name_canonical
        HAVING COUNT(DISTINCT lower(trim(studio_name))) > 1
      )`
    )
    : -1;
  const studioAliasGroups = readStudioAliasGroups(db);

  console.log('[eval:coverage] Evidence coverage summary');
  for (const item of tableCounts) {
    console.log(`[eval:coverage] ${item.table}: ${item.count}`);
  }
  if (hasMembershipCanonicalColumns) {
    console.log(`[eval:coverage] membership_missing_canonical: ${missingMembershipCanonical}`);
    console.log(`[eval:coverage] membership_canonical_collisions: ${membershipCanonicalCollisions}`);
  }
  if (hasCreditCanonicalColumn) {
    console.log(`[eval:coverage] credit_canonical_collisions: ${creditCanonicalCollisions}`);
  }
  if (hasStudioCanonicalColumn) {
    console.log(`[eval:coverage] studio_canonical_collisions: ${studioCanonicalCollisions}`);
  }

  const printTop = (label: string, rows: NameCountRow[]): void => {
    console.log(`[eval:coverage] ${label}:`);
    if (rows.length === 0) {
      console.log('[eval:coverage]   (none)');
      return;
    }
    for (const row of rows) {
      console.log(`[eval:coverage]   ${row.name} -> ${row.count}`);
    }
  };

  printTop('top membership bands', topBands);
  printTop('top membership people', topMembers);
  printTop('top credit names', topCredits);
  printTop('top studio names', topStudios);

  console.log('[eval:coverage] studio alias groups:');
  if (studioAliasGroups.length === 0) {
    console.log('[eval:coverage]   (none)');
  } else {
    for (const group of studioAliasGroups) {
      console.log(`[eval:coverage]   ${group.key} -> variants=${group.variants.length}, total=${group.totalCount}, names=[${group.variants.join(', ')}]`);
    }
  }
}

run();
