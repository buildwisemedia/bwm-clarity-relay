/**
 * bwm-clarity-relay — Microsoft Clarity Data Export API → Supabase relay.
 *
 * Cron polls Clarity project w7ga9q22fa (buildwisemedia.com).  The Clarity
 * export API returns a metric-pivoted array — one entry per metricName, each
 * with an `information` array of per-URL rows.  We re-aggregate those into
 * per-(page_path, utm_content) rows and upsert into public.clarity_events.
 *
 * Unique index (post migration 032):
 *   (client_slug, page_path, metric_window_start, COALESCE(utm_content, ''))
 *
 * Token flow: BROKER_BEARER → bwm-cred-broker /mint → CLARITY_API_TOKEN_BWM.
 * Worker ships in fail-soft inert state; comes alive once Robert pushes
 * VAULT_CLARITY_API_TOKEN_BWM to the broker and registers this agent's
 * BROKER_BEARER in cred_broker_agents.
 *
 * Full scope: PROJ-META-LEARN-001 multi-source.
 */

export interface Env {
  SUPABASE_URL: string;
  SUPABASE_SERVICE_ROLE_KEY: string;
  BROKER_URL: string;
  // Service binding to bwm-cred-broker — REQUIRED for same-account Worker→Worker
  // mint calls. Public *.workers.dev URLs fail with CF error 1042 on intra-
  // account subrequests. See memory: feedback_cf_worker_subrequest_1042.md.
  BROKER: Fetcher;
  BROKER_BEARER: string;
  CLARITY_PROJECT_ID: string;
  BWM_INTERNAL_KEY: string;
  BWM_INTERNAL_KEY_NEXT?: string;
}

export function isInternalKeyAuthorized(
  supplied: string,
  env: Pick<Env, "BWM_INTERNAL_KEY" | "BWM_INTERNAL_KEY_NEXT">,
): boolean {
  return Boolean(env.BWM_INTERNAL_KEY) && (
    supplied === env.BWM_INTERNAL_KEY ||
    (Boolean(env.BWM_INTERNAL_KEY_NEXT) && supplied === env.BWM_INTERNAL_KEY_NEXT)
  );
}

const BWM_HOSTNAME = "buildwisemedia.com";
const CLIENT_SLUG = "buildwise-media";

// Mirrors REQUIRED_PAYLOAD_KEYS in ~/bwm-ops-events/log-event/log_event.py.
// Worker writes go direct to /rest/v1/operational_events, bypassing the
// Python CLI's validate(). Enforced at insertOperationalEvent() — call sites
// supply explicit scope/symptom; the helper backstops with a synthesized
// fallback + console.error so regressions surface in wrangler tail.
const REQUIRED_PAYLOAD_KEYS: Record<string, readonly string[]> = {
  "incident.opened": ["severity", "scope", "symptom"],
};

// --- Clarity API types (metric-pivoted shape) ---------------------------------

interface ClarityInfoRow {
  Url?: string;
  sessionsCount?: string;       // most metrics
  totalSessionCount?: string;   // Traffic metric
  subTotal?: string;            // count metrics (RageClick, DeadClick, etc.)
  averageScrollDepth?: number;  // ScrollDepth metric
  totalTime?: string;           // EngagementTime
  activeTime?: string;          // EngagementTime
  [key: string]: unknown;
}

interface ClarityMetricBlock {
  metricName: string;
  information: ClarityInfoRow[];
}

// Per-URL aggregate built during re-aggregation
interface UrlAggregate {
  page_url: string;             // original full URL (audit)
  page_path: string;            // pathname only, normalised
  utm_content: string;          // '' when no UTM (NOT NULL sentinel)
  total_sessions: number;
  rage_click_count: number;
  dead_click_count: number;
  excessive_scroll_count: number;
  quick_back_count: number;
  scroll_depth_sum: number;
  scroll_depth_n: number;
  engagement_time_sum_sec: number;
  engagement_time_n: number;
}

// --- Utility helpers ---------------------------------------------------------

const JSON_HEADERS = { "Content-Type": "application/json" };

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: JSON_HEADERS });
}

function err(status: number, code: string, extra: Record<string, unknown> = {}): Response {
  return json({ error: code, ...extra }, status);
}

// Crockford base32 ULID — matches bwm-cred-broker / bwm-meta-ads pattern for operational_events PK
const ULID_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
function ulid(): string {
  let ts = Date.now();
  let timeChars = "";
  for (let i = 0; i < 10; i++) {
    timeChars = ULID_ALPHABET[ts & 31] + timeChars;
    ts = Math.floor(ts / 32);
  }
  const rand = new Uint8Array(16);
  crypto.getRandomValues(rand);
  let randChars = "";
  for (let i = 0; i < 16; i++) {
    randChars += ULID_ALPHABET[rand[i] & 31];
  }
  return timeChars + randChars;
}

/** ISO timestamp truncated to the current hour start: "2026-05-03T14:00:00.000Z" */
function hourStart(d: Date): string {
  return new Date(
    Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), d.getUTCHours(), 0, 0, 0)
  ).toISOString();
}

/** ISO timestamp for end of current hour: "2026-05-03T14:59:59.999Z" */
function hourEnd(d: Date): string {
  return new Date(
    Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), d.getUTCHours(), 59, 59, 999)
  ).toISOString();
}

// --- Cred-broker mint --------------------------------------------------------

interface MintResponse {
  secret: string;
  secret_name: string;
  ttl_seconds: number;
  expires_at: string;
  agent: string;
}

async function mintToken(env: Env, secretName: string): Promise<string> {
  // Use service binding (env.BROKER.fetch) instead of public URL to avoid
  // CF error 1042 on same-account Worker→Worker subrequests. URL host is
  // ignored when going through the binding; the path + method + headers
  // route to the bound worker directly via CF's internal network.
  const res = await env.BROKER.fetch("https://broker/mint", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.BROKER_BEARER}`,
      "Content-Type": "application/json",
      "User-Agent": "bwm-clarity-relay/1.0.0",
    },
    body: JSON.stringify({ name: secretName }),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Broker mint failed ${res.status}: ${body.slice(0, 200)}`);
  }

  const data = (await res.json()) as MintResponse;
  if (!data.secret) throw new Error("Broker returned empty secret");
  return data.secret;
}

// --- Clarity Data Export API -------------------------------------------------

async function fetchClarityMetrics(
  clarityToken: string,
  projectId: string,
  numOfDays: number = 1,
): Promise<ClarityMetricBlock[]> {
  const url = new URL("https://www.clarity.ms/export-data/api/v1/project-live-insights");
  url.searchParams.set("projectId", projectId);
  url.searchParams.set("numOfDays", String(numOfDays));
  url.searchParams.set("dimension1", "URL");

  const res = await fetch(url.toString(), {
    headers: {
      Authorization: `Bearer ${clarityToken}`,
      "User-Agent": "bwm-clarity-relay/1.0.0",
    },
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Clarity API ${res.status}: ${body.slice(0, 300)}`);
  }

  const data = await res.json();
  // Clarity returns a metric-pivoted array: [{metricName, information[]}]
  if (Array.isArray(data)) return data as ClarityMetricBlock[];
  // Defensive: wrapped envelope
  if (data && typeof data === "object" && Array.isArray((data as { value?: unknown }).value)) {
    return (data as { value: ClarityMetricBlock[] }).value;
  }
  return [];
}

// --- Re-aggregation: metric-pivoted → per-URL rows ---------------------------

/**
 * Canonical key for a URL: page_path + utm_content (null if absent).
 * Two distinct utm_content values on /book/ produce two separate rows —
 * that is the desired per-variant attribution behaviour.
 */
function parseUrlFields(rawUrl: string): { page_path: string; utm_content: string | null } | null {
  let parsed: URL;
  try {
    parsed = new URL(rawUrl);
  } catch {
    return null;
  }

  // Filter: only track buildwisemedia.com (drops "https://Electron" + localhost + *.pages.dev noise)
  if (parsed.hostname !== BWM_HOSTNAME) return null;

  // Normalise pathname: keep trailing slash on root only
  let pathname = parsed.pathname;
  if (pathname !== "/" && pathname.endsWith("/")) {
    pathname = pathname.slice(0, -1);
  }

  // Use empty string as sentinel for "no UTM" — keeps utm_content NOT NULL
  // which allows a plain column-list unique index (no COALESCE) that PostgREST
  // can reference in on_conflict.
  const utm_content = parsed.searchParams.get("utm_content") ?? "";

  return { page_path: pathname, utm_content };
}

function aggregateClarityMetrics(blocks: ClarityMetricBlock[]): ClarityEventRow[] {
  // Key: `${page_path}||${utm_content ?? ""}`
  const map = new Map<string, UrlAggregate>();

  function getOrCreate(rawUrl: string): UrlAggregate | null {
    const fields = parseUrlFields(rawUrl);
    if (!fields) return null;

    const key = `${fields.page_path}||${fields.utm_content}`;
    if (!map.has(key)) {
      map.set(key, {
        page_url: rawUrl,
        page_path: fields.page_path,
        utm_content: fields.utm_content,
        total_sessions: 0,
        rage_click_count: 0,
        dead_click_count: 0,
        excessive_scroll_count: 0,
        quick_back_count: 0,
        scroll_depth_sum: 0,
        scroll_depth_n: 0,
        engagement_time_sum_sec: 0,
        engagement_time_n: 0,
      });
    }
    return map.get(key)!;
  }

  for (const block of blocks) {
    const metric = block.metricName;
    for (const info of block.information ?? []) {
      const rawUrl = info.Url ?? "";
      if (!rawUrl) continue;

      const agg = getOrCreate(rawUrl);
      if (!agg) continue; // non-BWM hostname — skip

      switch (metric) {
        case "Traffic":
          // Traffic carries the canonical session count per URL
          agg.total_sessions = Math.max(
            agg.total_sessions,
            parseInt(info.totalSessionCount ?? "0", 10) || 0,
          );
          break;

        case "RageClickCount":
          agg.rage_click_count = parseInt(info.subTotal ?? "0", 10) || 0;
          // Also capture sessionsCount as a fallback session source
          if (!agg.total_sessions) {
            agg.total_sessions = parseInt(info.sessionsCount ?? "0", 10) || 0;
          }
          break;

        case "DeadClickCount":
          agg.dead_click_count = parseInt(info.subTotal ?? "0", 10) || 0;
          if (!agg.total_sessions) {
            agg.total_sessions = parseInt(info.sessionsCount ?? "0", 10) || 0;
          }
          break;

        case "ExcessiveScroll":
          agg.excessive_scroll_count = parseInt(info.subTotal ?? "0", 10) || 0;
          break;

        case "QuickbackClick":
          agg.quick_back_count = parseInt(info.subTotal ?? "0", 10) || 0;
          break;

        case "ScrollDepth":
          // averageScrollDepth is already an average — accumulate for a
          // per-key weighted average (same URL appears once per metric block)
          if (info.averageScrollDepth != null) {
            agg.scroll_depth_sum += Number(info.averageScrollDepth);
            agg.scroll_depth_n += 1;
          }
          break;

        case "EngagementTime":
          // totalTime in seconds (string)
          if (info.totalTime != null) {
            agg.engagement_time_sum_sec += parseInt(info.totalTime, 10) || 0;
            agg.engagement_time_n += 1;
          }
          break;

        default:
          // ScriptErrorCount, ErrorClickCount — not stored as dedicated columns
          break;
      }
    }
  }

  // Materialise aggregates into upsert rows
  const rows: Omit<ClarityEventRow, "metric_window_start" | "metric_window_end">[] = [];
  for (const agg of map.values()) {
    rows.push({
      client_slug: CLIENT_SLUG,
      page_url: agg.page_url,
      page_path: agg.page_path,
      utm_content: agg.utm_content,
      total_sessions: agg.total_sessions,
      rage_click_count: agg.rage_click_count,
      dead_click_count: agg.dead_click_count,
      excessive_scroll_count: agg.excessive_scroll_count,
      quick_back_count: agg.quick_back_count,
      scroll_depth_avg: agg.scroll_depth_n > 0
        ? Math.round((agg.scroll_depth_sum / agg.scroll_depth_n) * 100) / 100
        : null,
      engagement_time_avg_sec: agg.engagement_time_n > 0
        ? Math.round(agg.engagement_time_sum_sec / agg.engagement_time_n)
        : null,
      smart_events: null,
      raw: null, // raw per-URL JSON not feasible post-re-aggregation
    });
  }

  return rows as ClarityEventRow[];
}

// --- Supabase upsert ---------------------------------------------------------

interface ClarityEventRow {
  client_slug: string;
  page_url: string;
  page_path: string;
  utm_content: string;          // '' when no UTM (NOT NULL sentinel)
  metric_window_start: string;
  metric_window_end: string;
  total_sessions: number;
  rage_click_count: number;
  dead_click_count: number;
  excessive_scroll_count: number;
  quick_back_count: number;
  scroll_depth_avg: number | null;
  engagement_time_avg_sec: number | null;
  smart_events: Record<string, unknown> | null;
  raw: unknown;
}

async function upsertClarityRows(env: Env, rows: ClarityEventRow[]): Promise<number> {
  if (rows.length === 0) return 0;

  // Unique index post migration 033:
  //   (client_slug, page_path, metric_window_start, utm_content) — plain columns,
  //   utm_content is NOT NULL ('' sentinel for no UTM). PostgREST can reference
  //   plain column-list indexes directly in on_conflict.
  const url = `${env.SUPABASE_URL}/rest/v1/clarity_events?on_conflict=client_slug,page_path,metric_window_start,utm_content`;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates,return=minimal",
    },
    body: JSON.stringify(rows),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Supabase UPSERT ${res.status}: ${text.slice(0, 500)}`);
  }

  return rows.length;
}

// --- Operational event helpers -----------------------------------------------

async function insertOperationalEvent(
  env: Env,
  opts: {
    event_type: string;
    payload: Record<string, unknown>;
  },
): Promise<{ inserted: boolean; error?: string }> {
  // Schema backstop — see REQUIRED_PAYLOAD_KEYS above. Call sites should
  // supply explicit scope/symptom for incident.opened; this catches future
  // regressions and synthesizes valid placeholders so the event still records.
  const required = REQUIRED_PAYLOAD_KEYS[opts.event_type];
  if (required) {
    const missing = required.filter(
      (k) => opts.payload[k] === undefined || opts.payload[k] === null || opts.payload[k] === "",
    );
    if (missing.length > 0) {
      console.error(
        JSON.stringify({
          where: "insertOperationalEvent.schemaBackstop",
          event_type: opts.event_type,
          missing,
          payload_keys: Object.keys(opts.payload),
        }),
      );
      if (opts.event_type === "incident.opened") {
        opts.payload.scope ??= `clarity-relay:${opts.payload.source ?? "unknown"}:unscoped`;
        opts.payload.symptom ??= "schema-bypass-fallback";
        opts.payload.severity ??= "P3";
      }
    }
  }

  const body = {
    id: ulid(),
    event_type: opts.event_type,
    client_id: null,    // NULL = BWM-internal; client_slug lives in payload per bwm-meta-ads pattern
    payload: opts.payload,
    session_id: "bwm-clarity-relay",
    occurred_at: new Date().toISOString(),
  };

  const res = await fetch(`${env.SUPABASE_URL}/rest/v1/operational_events`, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "return=minimal",
    },
    body: JSON.stringify(body),
  });

  if (res.status >= 200 && res.status < 300) return { inserted: true };
  const text = await res.text();
  // 409 = duplicate key (idempotent re-run) — silent
  if (res.status === 409 || text.includes("duplicate key value")) return { inserted: false };
  console.error(
    JSON.stringify({ where: "insertOperationalEvent", status: res.status, body: text.slice(0, 300) }),
  );
  return { inserted: false, error: `${res.status}: ${text.slice(0, 200)}` };
}

// --- Consecutive-failure tracker (P1 escalation after 6h) -------------------
// We track failure count in a simple operational_events row (narrative type).
// The P1 gate: if the last 6 hourly cron runs have all written error events,
// escalate. We do a simple scan of recent events rather than a KV counter to
// stay stateless (no KV binding required).

async function countRecentFailures(env: Env, windowHours: number): Promise<number> {
  try {
    const since = new Date(Date.now() - windowHours * 3600 * 1000).toISOString();
    const url =
      `${env.SUPABASE_URL}/rest/v1/operational_events` +
      `?event_type=eq.incident.opened` +
      `&payload->>source=eq.bwm-clarity-relay` +
      `&occurred_at=gte.${encodeURIComponent(since)}` +
      `&select=id,occurred_at` +
      `&order=occurred_at.desc&limit=10`;

    const res = await fetch(url, {
      headers: {
        apikey: env.SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
      },
    });
    if (!res.ok) return 0;
    const rows = (await res.json()) as Array<unknown>;
    return rows.length;
  } catch {
    return 0;
  }
}

// --- Core relay logic --------------------------------------------------------

interface RelayResult {
  ok: boolean;
  window_start: string;
  window_end: string;
  clarity_rows_total: number;
  tracked_rows: number;
  upserted: number;
  error?: string;
  fail_soft?: boolean;
}

async function runRelay(env: Env): Promise<RelayResult> {
  const now = new Date();
  const windowStart = hourStart(now);
  const windowEnd = hourEnd(now);

  // 1. Mint Clarity token from broker
  let clarityToken: string;
  try {
    clarityToken = await mintToken(env, "CLARITY_API_TOKEN_BWM");
  } catch (e) {
    const errMsg = String(e);
    const isNotConfigured = errMsg.includes("not stored in broker") || errMsg.includes("not scoped");

    // Count recent failures to decide severity
    const recentFailures = await countRecentFailures(env, 6);
    const severity = recentFailures >= 5 ? "P1" : "P3";

    console.error(
      JSON.stringify({
        worker: "bwm-clarity-relay",
        event: "token_mint_failed",
        error: errMsg,
        severity,
        recent_failures: recentFailures,
        not_configured_yet: isNotConfigured,
      }),
    );

    // Emit incident — fail-soft, don't crash the worker.
    // scope/symptom required per log_event.py REQUIRED_PAYLOAD_KEYS.
    await insertOperationalEvent(env, {
      event_type: "incident.opened",
      payload: {
        title: `[${severity}] bwm-clarity-relay: token mint failed`,
        body: isNotConfigured
          ? "CLARITY_API_TOKEN_BWM not yet configured in cred-broker. Push token via: echo \"$TOKEN\" | npx wrangler secret put VAULT_CLARITY_API_TOKEN_BWM --name bwm-cred-broker"
          : `Broker mint failed: ${errMsg.slice(0, 300)}`,
        severity,
        scope: `clarity-relay:token-mint:${CLIENT_SLUG}`,
        symptom: isNotConfigured ? "broker-token-missing" : "broker-mint-error",
        source: "bwm-clarity-relay",
        client_slug: CLIENT_SLUG,
        error: errMsg.slice(0, 500),
        recent_failure_count: recentFailures,
      },
    });

    return {
      ok: false,
      window_start: windowStart,
      window_end: windowEnd,
      clarity_rows_total: 0,
      tracked_rows: 0,
      upserted: 0,
      error: errMsg.slice(0, 200),
      fail_soft: true,
    };
  }

  // 2. Fetch Clarity metrics (last 1 day = minimum Clarity supports; we filter to current hour-bucket)
  let clarityBlocks: ClarityMetricBlock[];
  try {
    clarityBlocks = await fetchClarityMetrics(clarityToken, env.CLARITY_PROJECT_ID, 1);
  } catch (e) {
    const errMsg = String(e);
    const recentFailures = await countRecentFailures(env, 6);
    const severity = recentFailures >= 5 ? "P1" : "P3";

    console.error(
      JSON.stringify({
        worker: "bwm-clarity-relay",
        event: "clarity_fetch_failed",
        error: errMsg,
        severity,
      }),
    );

    // scope/symptom required per log_event.py REQUIRED_PAYLOAD_KEYS.
    // symptom dedups on rate-limit-vs-other for incident-trend dashboards.
    await insertOperationalEvent(env, {
      event_type: "incident.opened",
      payload: {
        title: `[${severity}] bwm-clarity-relay: Clarity API fetch failed`,
        body: `Clarity Data Export API returned an error: ${errMsg.slice(0, 300)}`,
        severity,
        scope: `clarity-relay:clarity-fetch:${CLIENT_SLUG}`,
        symptom: errMsg.includes("429")
          ? "clarity-rate-limit"
          : errMsg.includes("403")
            ? "clarity-permission-denied"
            : "clarity-api-error",
        source: "bwm-clarity-relay",
        client_slug: CLIENT_SLUG,
        error: errMsg.slice(0, 500),
        recent_failure_count: recentFailures,
      },
    });

    return {
      ok: false,
      window_start: windowStart,
      window_end: windowEnd,
      clarity_rows_total: 0,
      tracked_rows: 0,
      upserted: 0,
      error: errMsg.slice(0, 200),
      fail_soft: true,
    };
  }

  // 3. Re-aggregate metric-pivoted API response into per-(page_path, utm_content) rows
  const aggregated = aggregateClarityMetrics(clarityBlocks);

  // Stamp window timestamps onto every row
  const toUpsert: ClarityEventRow[] = aggregated.map((r) => ({
    ...r,
    metric_window_start: windowStart,
    metric_window_end: windowEnd,
  }));

  // 4. Upsert to Supabase
  let upserted = 0;
  try {
    upserted = await upsertClarityRows(env, toUpsert);
  } catch (e) {
    const errMsg = String(e);
    const recentFailures = await countRecentFailures(env, 6);
    const severity = recentFailures >= 5 ? "P1" : "P3";

    console.error(
      JSON.stringify({
        worker: "bwm-clarity-relay",
        event: "supabase_upsert_failed",
        error: errMsg,
        severity,
      }),
    );

    // scope/symptom required per log_event.py REQUIRED_PAYLOAD_KEYS.
    await insertOperationalEvent(env, {
      event_type: "incident.opened",
      payload: {
        title: `[${severity}] bwm-clarity-relay: Supabase upsert failed`,
        body: `Failed to write ${toUpsert.length} rows to clarity_events: ${errMsg.slice(0, 300)}`,
        severity,
        scope: `clarity-relay:supabase-upsert:${CLIENT_SLUG}`,
        symptom: "supabase-upsert-error",
        source: "bwm-clarity-relay",
        client_slug: CLIENT_SLUG,
        error: errMsg.slice(0, 500),
        recent_failure_count: recentFailures,
      },
    });

    return {
      ok: false,
      window_start: windowStart,
      window_end: windowEnd,
      clarity_rows_total: clarityBlocks.length,
      tracked_rows: toUpsert.length,
      upserted: 0,
      error: errMsg.slice(0, 200),
      fail_soft: true,
    };
  }

  // Derive the page_paths we actually tracked for observability
  const trackedPagePaths = [...new Set(toUpsert.map((r) => r.page_path))].sort();

  // 5. Emit heartbeat
  await insertOperationalEvent(env, {
    event_type: "daemon.heartbeat",
    payload: {
      source: "bwm-clarity-relay",
      client_slug: CLIENT_SLUG,
      clarity_metric_blocks: clarityBlocks.length,
      tracked_pages: trackedPagePaths,
      tracked_rows: toUpsert.length,
      upserted,
      window_start: windowStart,
      window_end: windowEnd,
    },
  }).catch((e) => {
    // Heartbeat failure is non-blocking
    console.error(JSON.stringify({ where: "heartbeat", error: String(e) }));
  });

  console.log(
    JSON.stringify({
      worker: "bwm-clarity-relay",
      event: "cron.ok",
      clarity_metric_blocks: clarityBlocks.length,
      tracked_rows: toUpsert.length,
      upserted,
      window_start: windowStart,
      window_end: windowEnd,
    }),
  );

  return {
    ok: true,
    window_start: windowStart,
    window_end: windowEnd,
    clarity_rows_total: clarityBlocks.length,
    tracked_rows: toUpsert.length,
    upserted,
  };
}

// --- Worker export -----------------------------------------------------------

export default {
  async fetch(request: Request, env: Env, _ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname.replace(/\/+$/, "") || "/";

    // GET /health — unauthenticated
    if (request.method === "GET" && path === "/health") {
      return json({
        ok: true,
        worker: "bwm-clarity-relay",
        version: "2.0.0",
        clarity_project: env.CLARITY_PROJECT_ID ?? "w7ga9q22fa",
        filter: `hostname=${BWM_HOSTNAME}`,
        attribution: "utm_content per-variant",
        cron: "0 */3 * * *",
      });
    }

    // POST /run-now — gated on X-BWM-Internal-Key
    if (request.method === "POST" && path === "/run-now") {
      const key = request.headers.get("X-BWM-Internal-Key") ?? "";
      if (!isInternalKeyAuthorized(key, env)) {
        return err(401, "unauthorized");
      }

      try {
        const result = await runRelay(env);
        return json({ mode: "run-now", ...result }, result.ok ? 200 : 207);
      } catch (e) {
        return err(500, "relay_error", { detail: String(e).slice(0, 300) });
      }
    }

    return err(404, "not_found", { path, method: request.method });
  },

  async scheduled(_event: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(
      runRelay(env).then((result) => {
        console.log(
          JSON.stringify({
            worker: "bwm-clarity-relay",
            event: "cron.scheduled",
            ok: result.ok,
            upserted: result.upserted,
            tracked_rows: result.tracked_rows,
            clarity_rows_total: result.clarity_rows_total,
            window_start: result.window_start,
            fail_soft: result.fail_soft ?? false,
            error: result.error,
          }),
        );
      }).catch((e) => {
        console.error(
          JSON.stringify({
            worker: "bwm-clarity-relay",
            event: "cron.uncaught_error",
            error: String(e),
          }),
        );
      }),
    );
  },
} satisfies ExportedHandler<Env>;
