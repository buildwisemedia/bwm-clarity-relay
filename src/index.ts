/**
 * bwm-clarity-relay — Microsoft Clarity Data Export API → Supabase relay.
 *
 * Hourly cron polls Clarity project w7ga9q22fa (buildwisemedia.com), bucketing
 * metrics for the homepage + /book page into public.clarity_events with idempotent
 * UPSERT on (client_slug, page_url, metric_window_start).
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
  BROKER_BEARER: string;
  CLARITY_PROJECT_ID: string;
  BWM_INTERNAL_KEY: string;
}

// Pages to track. Only rows matching these URLs are upserted.
const TRACKED_PAGES = [
  "https://buildwisemedia.com/",
  "https://buildwisemedia.com/book",
];

const CLIENT_SLUG = "buildwise-media";

// --- Clarity API types -------------------------------------------------------

interface ClarityMetricRow {
  Dimension1?: string; // URL when dimension1=URL
  Dimension2?: string; // optional second dim (Browser, Device)
  ScrollDepth?: number;
  EngagementTime?: number;
  Sessions?: number;
  RageClickCount?: number;
  DeadClickCount?: number;
  ExcessiveScroll?: number;
  QuickBackClick?: number;
  ScriptError?: number;
  ErrorClickCount?: number;
  SessionsWithSmartEvent?: number;
  [key: string]: unknown;
}

// --- Utility helpers ---------------------------------------------------------

const JSON_HEADERS = { "Content-Type": "application/json" };

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: JSON_HEADERS });
}

function err(status: number, code: string, extra: Record<string, unknown> = {}): Response {
  return json({ error: code, ...extra }, status);
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
  const res = await fetch(`${env.BROKER_URL}/mint`, {
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
): Promise<ClarityMetricRow[]> {
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
  // Clarity returns either an array directly or a wrapper object
  if (Array.isArray(data)) return data as ClarityMetricRow[];
  if (data && typeof data === "object" && Array.isArray((data as { value?: unknown }).value)) {
    return (data as { value: ClarityMetricRow[] }).value;
  }
  // Fallback — return whatever we got wrapped in an array
  return [];
}

// --- Supabase upsert ---------------------------------------------------------

interface ClarityEventRow {
  client_slug: string;
  page_url: string;
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

  const url = `${env.SUPABASE_URL}/rest/v1/clarity_events?on_conflict=client_slug,page_url,metric_window_start`;

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
    client_slug?: string;
    payload: Record<string, unknown>;
  },
): Promise<void> {
  const body = {
    event_type: opts.event_type,
    payload: opts.payload,
    session_id: "bwm-clarity-relay",
    occurred_at: new Date().toISOString(),
    ...(opts.client_slug ? { client_slug: opts.client_slug } : {}),
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

  if (!res.ok) {
    const text = await res.text();
    // 409 = duplicate id, acceptable (idempotent re-run)
    if (res.status !== 409) {
      console.error(
        JSON.stringify({ where: "insertOperationalEvent", status: res.status, body: text.slice(0, 300) }),
      );
    }
  }
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

    // Emit incident — fail-soft, don't crash the worker
    await insertOperationalEvent(env, {
      event_type: "incident.opened",
      client_slug: CLIENT_SLUG,
      payload: {
        title: `[${severity}] bwm-clarity-relay: token mint failed`,
        body: isNotConfigured
          ? "CLARITY_API_TOKEN_BWM not yet configured in cred-broker. Push token via: echo \"$TOKEN\" | npx wrangler secret put VAULT_CLARITY_API_TOKEN_BWM --name bwm-cred-broker"
          : `Broker mint failed: ${errMsg.slice(0, 300)}`,
        severity,
        source: "bwm-clarity-relay",
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
  let clarityRows: ClarityMetricRow[];
  try {
    clarityRows = await fetchClarityMetrics(clarityToken, env.CLARITY_PROJECT_ID, 1);
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

    await insertOperationalEvent(env, {
      event_type: "incident.opened",
      client_slug: CLIENT_SLUG,
      payload: {
        title: `[${severity}] bwm-clarity-relay: Clarity API fetch failed`,
        body: `Clarity Data Export API returned an error: ${errMsg.slice(0, 300)}`,
        severity,
        source: "bwm-clarity-relay",
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

  // 3. Filter to tracked pages only; map to upsert shape
  const trackedSet = new Set(TRACKED_PAGES);

  const toUpsert: ClarityEventRow[] = clarityRows
    .filter((r) => {
      const url = r.Dimension1 ?? "";
      // Normalize: strip trailing slash for /book etc., but keep root /
      const normalized = url === "https://buildwisemedia.com" ? "https://buildwisemedia.com/" : url.replace(/\/$/, (url.endsWith("buildwisemedia.com/") ? "/" : ""));
      return trackedSet.has(url) || trackedSet.has(normalized);
    })
    .map((r) => ({
      client_slug: CLIENT_SLUG,
      page_url: r.Dimension1 ?? "",
      metric_window_start: windowStart,
      metric_window_end: windowEnd,
      total_sessions: r.Sessions ?? 0,
      rage_click_count: r.RageClickCount ?? 0,
      dead_click_count: r.DeadClickCount ?? 0,
      excessive_scroll_count: r.ExcessiveScroll ?? 0,
      quick_back_count: r.QuickBackClick ?? 0,
      scroll_depth_avg: r.ScrollDepth != null ? Number(r.ScrollDepth) : null,
      engagement_time_avg_sec: r.EngagementTime != null ? Number(r.EngagementTime) : null,
      smart_events:
        r.SessionsWithSmartEvent != null
          ? { sessions_with_smart_event: r.SessionsWithSmartEvent }
          : null,
      raw: r,
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

    await insertOperationalEvent(env, {
      event_type: "incident.opened",
      client_slug: CLIENT_SLUG,
      payload: {
        title: `[${severity}] bwm-clarity-relay: Supabase upsert failed`,
        body: `Failed to write ${toUpsert.length} rows to clarity_events: ${errMsg.slice(0, 300)}`,
        severity,
        source: "bwm-clarity-relay",
        error: errMsg.slice(0, 500),
        recent_failure_count: recentFailures,
      },
    });

    return {
      ok: false,
      window_start: windowStart,
      window_end: windowEnd,
      clarity_rows_total: clarityRows.length,
      tracked_rows: toUpsert.length,
      upserted: 0,
      error: errMsg.slice(0, 200),
      fail_soft: true,
    };
  }

  // 5. Emit heartbeat
  await insertOperationalEvent(env, {
    event_type: "daemon.heartbeat",
    client_slug: CLIENT_SLUG,
    payload: {
      source: "bwm-clarity-relay",
      clarity_rows_total: clarityRows.length,
      tracked_pages: TRACKED_PAGES,
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
      clarity_rows_total: clarityRows.length,
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
    clarity_rows_total: clarityRows.length,
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
        version: "1.0.0",
        clarity_project: env.CLARITY_PROJECT_ID ?? "w7ga9q22fa",
        tracked_pages: TRACKED_PAGES,
        cron: "0 * * * *",
      });
    }

    // POST /run-now — gated on X-BWM-Internal-Key
    if (request.method === "POST" && path === "/run-now") {
      const key = request.headers.get("X-BWM-Internal-Key") ?? "";
      if (!env.BWM_INTERNAL_KEY || key !== env.BWM_INTERNAL_KEY) {
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
