function normalizeQuotes(value: string): string {
  return value
    .replace(/[\u2018\u2019\u02BC]/g, "'")
    .replace(/[\u201C\u201D]/g, '"');
}

function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, ' ').trim();
}

function stripLeadingThe(value: string): string {
  return value.replace(/^the\s+/i, '').trim();
}

function toCanonicalKey(value: string, dropLeadingThe = false): string {
  const quoted = normalizeQuotes(value);
  const compact = normalizeWhitespace(quoted);
  if (!compact) return '';

  const candidate = dropLeadingThe ? stripLeadingThe(compact) : compact;
  return candidate.toLowerCase();
}

export function canonicalizeDisplayName(value: string): string {
  return normalizeWhitespace(normalizeQuotes(value));
}

export function buildArtistCanonicalKey(value: string): string {
  return toCanonicalKey(value, true);
}

export function buildPersonCanonicalKey(value: string): string {
  return toCanonicalKey(value, false);
}

export function buildStudioCanonicalKey(value: string): string {
  const canonical = toCanonicalKey(value, true);
  if (!canonical) return '';

  return canonical
    .replace(/[.,;:()\[\]{}]/g, ' ')
    .replace(/\s+(?:in|during|from|at)\s+the\s+\d{2}(?:['’]s|s)\b.*$/g, '')
    .replace(/\s+(?:in|during|from|at)\s+\d{4}\b.*$/g, '')
    .replace(/\s+p[aå]\s+\d{2}-talet\b.*$/g, '')
    .replace(/\brecording\s+studios?\b/g, 'studio')
    .replace(/\bstudios\b/g, 'studio')
    .replace(/\s+/g, ' ')
    .trim();
}

export function buildCreditCanonicalKey(value: string): string {
  return buildPersonCanonicalKey(value);
}
