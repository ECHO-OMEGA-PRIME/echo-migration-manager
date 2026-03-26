import { Hono } from "hono";
import { cors } from "hono/cors";

// ─── Types ──────────────────────────────────────────────────────────────────

interface Env {
  DB: D1Database;
  CACHE: KVNamespace;
  SHARED_BRAIN: Fetcher;
  ALERT_ROUTER: Fetcher;
}

interface Migration {
  id: number;
  service_name: string;
  migration_name: string;
  version: string;
  description: string;
  up_sql: string;
  down_sql: string | null;
  checksum: string;
  status: "pending" | "applied" | "rolled_back" | "failed";
  applied_at: string | null;
  rolled_back_at: string | null;
  applied_by: string | null;
  execution_time_ms: number | null;
  created_at: string;
}

interface MigrationLock {
  id: number;
  service_name: string;
  locked_by: string;
  locked_at: string;
  expires_at: string;
}

interface SchemaSnapshot {
  id: number;
  service_name: string;
  version: string;
  snapshot: string;
  table_count: number;
  index_count: number;
  created_at: string;
}

interface MigrationPlan {
  id: number;
  name: string;
  description: string;
  status: "draft" | "approved" | "executing" | "completed" | "failed";
  migrations: string;
  cross_service: number;
  created_at: string;
  executed_at: string | null;
  completed_at: string | null;
}

interface MigrationHistory {
  id: number;
  migration_id: number;
  action: "apply" | "rollback" | "skip";
  result: string;
  error_message: string | null;
  executed_at: string;
}

interface CreateMigrationBody {
  service_name: string;
  migration_name: string;
  version: string;
  description: string;
  up_sql: string;
  down_sql?: string;
}

interface CreatePlanBody {
  name: string;
  description: string;
  migration_ids: number[];
}

interface SnapshotBody {
  snapshot: string;
  table_count: number;
  index_count: number;
  version: string;
}

interface ApplyBody {
  applied_by?: string;
  execution_time_ms?: number;
  result?: string;
}

interface RollbackBody {
  rolled_back_by?: string;
  execution_time_ms?: number;
  result?: string;
}

interface LockBody {
  locked_by: string;
  ttl_minutes?: number;
}

interface ValidateBody {
  sql: string;
}

// ─── Logging ────────────────────────────────────────────────────────────────

type LogLevel = "debug" | "info" | "warn" | "error" | "fatal";

function log(level: LogLevel, message: string, data?: Record<string, unknown>): void {
  const entry = {
    timestamp: new Date().toISOString(),
    level,
    service: "echo-migration-manager",
    message,
    ...data,
  };
  switch (level) {
    case "error":
    case "fatal":
      console.error(JSON.stringify(entry));
      break;
    case "warn":
      console.warn(JSON.stringify(entry));
      break;
    default:
      console.log(JSON.stringify(entry));
  }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

const VERSION = "1.0.0";
const START_TIME = Date.now();
const API_KEY = "echo-omega-prime-forge-x-2026";

async function sha256(text: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(text);
  const hash = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(hash))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

function now(): string {
  return new Date().toISOString();
}

function jsonOk(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function jsonError(message: string, status: number): Response {
  log("error", message, { status });
  return new Response(JSON.stringify({ error: message, status }), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

// ─── D1 Schema Init ────────────────────────────────────────────────────────

async function initSchema(db: D1Database): Promise<void> {
  const statements = [
    `CREATE TABLE IF NOT EXISTS migrations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      service_name TEXT NOT NULL,
      migration_name TEXT NOT NULL,
      version TEXT NOT NULL,
      description TEXT DEFAULT '',
      up_sql TEXT NOT NULL,
      down_sql TEXT,
      checksum TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      applied_at TEXT,
      rolled_back_at TEXT,
      applied_by TEXT,
      execution_time_ms INTEGER,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )`,
    `CREATE TABLE IF NOT EXISTS migration_locks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      service_name TEXT UNIQUE NOT NULL,
      locked_by TEXT NOT NULL,
      locked_at TEXT NOT NULL,
      expires_at TEXT NOT NULL
    )`,
    `CREATE TABLE IF NOT EXISTS schema_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      service_name TEXT NOT NULL,
      version TEXT NOT NULL,
      snapshot TEXT NOT NULL,
      table_count INTEGER DEFAULT 0,
      index_count INTEGER DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )`,
    `CREATE TABLE IF NOT EXISTS migration_plans (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      description TEXT DEFAULT '',
      status TEXT NOT NULL DEFAULT 'draft',
      migrations TEXT NOT NULL DEFAULT '[]',
      cross_service BOOLEAN DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      executed_at TEXT,
      completed_at TEXT
    )`,
    `CREATE TABLE IF NOT EXISTS migration_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      migration_id INTEGER NOT NULL,
      action TEXT NOT NULL,
      result TEXT DEFAULT '',
      error_message TEXT,
      executed_at TEXT NOT NULL DEFAULT (datetime('now'))
    )`,
    `CREATE INDEX IF NOT EXISTS idx_migrations_service ON migrations(service_name)`,
    `CREATE INDEX IF NOT EXISTS idx_migrations_status ON migrations(status)`,
    `CREATE INDEX IF NOT EXISTS idx_migrations_version ON migrations(service_name, version)`,
    `CREATE INDEX IF NOT EXISTS idx_snapshots_service ON schema_snapshots(service_name)`,
    `CREATE INDEX IF NOT EXISTS idx_history_migration ON migration_history(migration_id)`,
  ];

  for (const sql of statements) {
    await db.prepare(sql).run();
  }
}

// ─── App ────────────────────────────────────────────────────────────────────

const app = new Hono<{ Bindings: Env }>();

// CORS
app.use("*", cors({
  origin: "*",
  allowMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowHeaders: ["Content-Type", "X-Echo-API-Key", "Authorization"],
}));

// Auth middleware (skip health)
app.use("*", async (c, next) => {
  if (c.req.path === "/health" || c.req.path === "/" || c.req.method === "OPTIONS") {
    return next();
  }
  const key = c.req.header("X-Echo-API-Key");
  if (key !== API_KEY) {
    return jsonError("Unauthorized: invalid or missing X-Echo-API-Key", 401);
  }
  return next();
});

// Schema init middleware
app.use("*", async (c, next) => {
  const cacheKey = "schema_initialized_v1";
  const cached = await c.env.CACHE.get(cacheKey);
  if (!cached) {
    await initSchema(c.env.DB);
    await c.env.CACHE.put(cacheKey, "true", { expirationTtl: 86400 });
    log("info", "D1 schema initialized");
  }
  return next();
});

// ─── 1. GET /health ─────────────────────────────────────────────────────────

app.get("/health", async (c) => {
  let dbOk = false;
  try {
    await c.env.DB.prepare("SELECT 1").first();
    dbOk = true;
  } catch { /* ignore */ }

  return c.json({
    status: dbOk ? "healthy" : "degraded",
    service: "echo-migration-manager",
    version: VERSION,
    timestamp: now(),
    uptime_ms: Date.now() - START_TIME,
    dependencies: {
      d1: dbOk ? "connected" : "error",
      kv: "connected",
    },
  });
});

// ─── Root redirect ──────────────────────────────────────────────────────────

app.get("/", (c) => {
  return c.json({
    service: "echo-migration-manager",
    version: VERSION,
    description: "Database migration tracking and schema versioning for ECHO Workers",
    endpoints: [
      "GET /health", "GET /stats",
      "GET /migrations", "GET /migrations/:id", "POST /migrations",
      "POST /migrations/:id/apply", "POST /migrations/:id/rollback",
      "GET /services", "GET /services/:name", "GET /services/:name/current",
      "GET /services/:name/pending", "POST /services/:name/snapshot",
      "GET /services/:name/snapshots", "GET /services/:name/diff",
      "POST /plans", "GET /plans", "GET /plans/:id",
      "POST /plans/:id/approve", "POST /plans/:id/execute",
      "POST /lock/:service", "POST /unlock/:service", "GET /locks",
      "GET /report", "POST /validate",
    ],
  });
});

// ─── 2. GET /stats ──────────────────────────────────────────────────────────

app.get("/stats", async (c) => {
  const cacheKey = "stats_cache";
  const cached = await c.env.CACHE.get(cacheKey, "json");
  if (cached) return c.json(cached);

  const [total, byStatus, services, recent] = await Promise.all([
    c.env.DB.prepare("SELECT COUNT(*) as count FROM migrations").first<{ count: number }>(),
    c.env.DB.prepare(
      "SELECT status, COUNT(*) as count FROM migrations GROUP BY status"
    ).all<{ status: string; count: number }>(),
    c.env.DB.prepare(
      "SELECT COUNT(DISTINCT service_name) as count FROM migrations"
    ).first<{ count: number }>(),
    c.env.DB.prepare(
      "SELECT id, service_name, migration_name, version, status, applied_at, created_at FROM migrations ORDER BY id DESC LIMIT 10"
    ).all<Migration>(),
  ]);

  const statusMap: Record<string, number> = {};
  for (const row of byStatus.results) {
    statusMap[row.status] = row.count;
  }

  const data = {
    total_migrations: total?.count ?? 0,
    by_status: statusMap,
    services_tracked: services?.count ?? 0,
    recent_activity: recent.results,
    generated_at: now(),
  };

  await c.env.CACHE.put(cacheKey, JSON.stringify(data), { expirationTtl: 60 });
  return c.json(data);
});

// ─── 3. GET /migrations ─────────────────────────────────────────────────────

app.get("/migrations", async (c) => {
  const service = c.req.query("service");
  const status = c.req.query("status");
  const version = c.req.query("version");
  const limit = parseInt(c.req.query("limit") ?? "100", 10);
  const offset = parseInt(c.req.query("offset") ?? "0", 10);

  let sql = "SELECT * FROM migrations WHERE 1=1";
  const params: string[] = [];

  if (service) {
    sql += " AND service_name = ?";
    params.push(service);
  }
  if (status) {
    sql += " AND status = ?";
    params.push(status);
  }
  if (version) {
    sql += " AND version = ?";
    params.push(version);
  }

  sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?";
  params.push(String(limit), String(offset));

  const result = await c.env.DB.prepare(sql).bind(...params).all<Migration>();
  return c.json({ migrations: result.results, count: result.results.length });
});

// ─── 4. GET /migrations/:id ─────────────────────────────────────────────────

app.get("/migrations/:id", async (c) => {
  const id = c.req.param("id");
  const migration = await c.env.DB.prepare("SELECT * FROM migrations WHERE id = ?")
    .bind(id)
    .first<Migration>();

  if (!migration) return jsonError("Migration not found", 404);

  const history = await c.env.DB.prepare(
    "SELECT * FROM migration_history WHERE migration_id = ? ORDER BY executed_at DESC"
  ).bind(id).all<MigrationHistory>();

  return c.json({ migration, history: history.results });
});

// ─── 5. POST /migrations ────────────────────────────────────────────────────

app.post("/migrations", async (c) => {
  let body: CreateMigrationBody;
  try {
    body = await c.req.json<CreateMigrationBody>();
  } catch {
    return jsonError("Invalid JSON body", 400);
  }

  if (!body.service_name || !body.migration_name || !body.version || !body.up_sql) {
    return jsonError("Missing required fields: service_name, migration_name, version, up_sql", 400);
  }

  const checksum = await sha256(body.up_sql);

  // Check for duplicate
  const existing = await c.env.DB.prepare(
    "SELECT id FROM migrations WHERE service_name = ? AND version = ?"
  ).bind(body.service_name, body.version).first<{ id: number }>();

  if (existing) {
    return jsonError(`Migration version ${body.version} already exists for service ${body.service_name}`, 409);
  }

  const result = await c.env.DB.prepare(
    `INSERT INTO migrations (service_name, migration_name, version, description, up_sql, down_sql, checksum, status, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)`
  ).bind(
    body.service_name,
    body.migration_name,
    body.version,
    body.description ?? "",
    body.up_sql,
    body.down_sql ?? null,
    checksum,
    now(),
  ).run();

  const id = result.meta.last_row_id;
  log("info", "Migration registered", { id, service: body.service_name, version: body.version });

  await c.env.CACHE.delete("stats_cache");

  return c.json({ id, checksum, status: "pending", message: "Migration registered" }, 201);
});

// ─── 6. POST /migrations/:id/apply ──────────────────────────────────────────

app.post("/migrations/:id/apply", async (c) => {
  const id = c.req.param("id");
  const migration = await c.env.DB.prepare("SELECT * FROM migrations WHERE id = ?")
    .bind(id)
    .first<Migration>();

  if (!migration) return jsonError("Migration not found", 404);
  if (migration.status === "applied") return jsonError("Migration already applied", 409);

  // Check lock
  const lock = await c.env.DB.prepare(
    "SELECT * FROM migration_locks WHERE service_name = ?"
  ).bind(migration.service_name).first<MigrationLock>();

  if (lock) {
    const expiresAt = new Date(lock.expires_at).getTime();
    if (Date.now() < expiresAt) {
      return jsonError(`Service ${migration.service_name} is locked by ${lock.locked_by} until ${lock.expires_at}`, 423);
    }
    // Expired lock, remove it
    await c.env.DB.prepare("DELETE FROM migration_locks WHERE service_name = ?")
      .bind(migration.service_name).run();
  }

  // Check version ordering: all earlier versions should be applied
  const unapplied = await c.env.DB.prepare(
    "SELECT id, version FROM migrations WHERE service_name = ? AND version < ? AND status != 'applied' ORDER BY version"
  ).bind(migration.service_name, migration.version).all<{ id: number; version: string }>();

  if (unapplied.results.length > 0) {
    return jsonError(
      `Cannot apply version ${migration.version}: ${unapplied.results.length} earlier migration(s) not applied: ${unapplied.results.map((m) => m.version).join(", ")}`,
      400,
    );
  }

  let applyBody: ApplyBody = {};
  try {
    applyBody = await c.req.json<ApplyBody>();
  } catch { /* optional body */ }

  const appliedAt = now();
  await c.env.DB.prepare(
    `UPDATE migrations SET status = 'applied', applied_at = ?, applied_by = ?, execution_time_ms = ? WHERE id = ?`
  ).bind(
    appliedAt,
    applyBody.applied_by ?? "echo-migration-manager",
    applyBody.execution_time_ms ?? null,
    id,
  ).run();

  await c.env.DB.prepare(
    `INSERT INTO migration_history (migration_id, action, result, executed_at) VALUES (?, 'apply', ?, ?)`
  ).bind(id, applyBody.result ?? "Applied successfully", appliedAt).run();

  log("info", "Migration applied", { id, service: migration.service_name, version: migration.version });
  await c.env.CACHE.delete("stats_cache");

  return c.json({ id: Number(id), status: "applied", applied_at: appliedAt });
});

// ─── 7. POST /migrations/:id/rollback ───────────────────────────────────────

app.post("/migrations/:id/rollback", async (c) => {
  const id = c.req.param("id");
  const migration = await c.env.DB.prepare("SELECT * FROM migrations WHERE id = ?")
    .bind(id)
    .first<Migration>();

  if (!migration) return jsonError("Migration not found", 404);
  if (migration.status !== "applied") return jsonError("Can only rollback applied migrations", 400);
  if (!migration.down_sql) return jsonError("No down_sql defined for this migration; rollback not possible", 400);

  // Check lock
  const lock = await c.env.DB.prepare(
    "SELECT * FROM migration_locks WHERE service_name = ?"
  ).bind(migration.service_name).first<MigrationLock>();

  if (lock) {
    const expiresAt = new Date(lock.expires_at).getTime();
    if (Date.now() < expiresAt) {
      return jsonError(`Service ${migration.service_name} is locked by ${lock.locked_by} until ${lock.expires_at}`, 423);
    }
    await c.env.DB.prepare("DELETE FROM migration_locks WHERE service_name = ?")
      .bind(migration.service_name).run();
  }

  // Check that no later migrations depend on this one (are applied)
  const laterApplied = await c.env.DB.prepare(
    "SELECT id, version FROM migrations WHERE service_name = ? AND version > ? AND status = 'applied' ORDER BY version"
  ).bind(migration.service_name, migration.version).all<{ id: number; version: string }>();

  if (laterApplied.results.length > 0) {
    return jsonError(
      `Cannot rollback version ${migration.version}: ${laterApplied.results.length} later migration(s) still applied: ${laterApplied.results.map((m) => m.version).join(", ")}. Roll them back first.`,
      400,
    );
  }

  let rollbackBody: RollbackBody = {};
  try {
    rollbackBody = await c.req.json<RollbackBody>();
  } catch { /* optional body */ }

  const rolledBackAt = now();
  await c.env.DB.prepare(
    `UPDATE migrations SET status = 'rolled_back', rolled_back_at = ?, applied_by = ?, execution_time_ms = ? WHERE id = ?`
  ).bind(
    rolledBackAt,
    rollbackBody.rolled_back_by ?? "echo-migration-manager",
    rollbackBody.execution_time_ms ?? null,
    id,
  ).run();

  await c.env.DB.prepare(
    `INSERT INTO migration_history (migration_id, action, result, executed_at) VALUES (?, 'rollback', ?, ?)`
  ).bind(id, rollbackBody.result ?? "Rolled back successfully", rolledBackAt).run();

  log("info", "Migration rolled back", { id, service: migration.service_name, version: migration.version });
  await c.env.CACHE.delete("stats_cache");

  return c.json({ id: Number(id), status: "rolled_back", rolled_back_at: rolledBackAt, down_sql: migration.down_sql });
});

// ─── 8. GET /services ───────────────────────────────────────────────────────

app.get("/services", async (c) => {
  const result = await c.env.DB.prepare(
    `SELECT
       service_name,
       COUNT(*) as total_migrations,
       SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
       SUM(CASE WHEN status = 'applied' THEN 1 ELSE 0 END) as applied,
       SUM(CASE WHEN status = 'rolled_back' THEN 1 ELSE 0 END) as rolled_back,
       SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
       MAX(CASE WHEN status = 'applied' THEN version ELSE NULL END) as current_version,
       MAX(created_at) as last_activity
     FROM migrations
     GROUP BY service_name
     ORDER BY service_name`
  ).all();

  return c.json({ services: result.results, count: result.results.length });
});

// ─── 9. GET /services/:name ─────────────────────────────────────────────────

app.get("/services/:name", async (c) => {
  const name = c.req.param("name");

  const migrations = await c.env.DB.prepare(
    "SELECT * FROM migrations WHERE service_name = ? ORDER BY version ASC"
  ).bind(name).all<Migration>();

  if (migrations.results.length === 0) {
    return jsonError(`No migrations found for service: ${name}`, 404);
  }

  const lock = await c.env.DB.prepare(
    "SELECT * FROM migration_locks WHERE service_name = ?"
  ).bind(name).first<MigrationLock>();

  const currentVersion = await c.env.DB.prepare(
    "SELECT version FROM migrations WHERE service_name = ? AND status = 'applied' ORDER BY version DESC LIMIT 1"
  ).bind(name).first<{ version: string }>();

  return c.json({
    service_name: name,
    current_version: currentVersion?.version ?? null,
    migrations: migrations.results,
    total: migrations.results.length,
    lock: lock ?? null,
  });
});

// ─── 10. GET /services/:name/current ────────────────────────────────────────

app.get("/services/:name/current", async (c) => {
  const name = c.req.param("name");

  const current = await c.env.DB.prepare(
    "SELECT * FROM migrations WHERE service_name = ? AND status = 'applied' ORDER BY version DESC LIMIT 1"
  ).bind(name).first<Migration>();

  if (!current) {
    return c.json({ service_name: name, current_version: null, message: "No applied migrations" });
  }

  return c.json({
    service_name: name,
    current_version: current.version,
    migration_name: current.migration_name,
    applied_at: current.applied_at,
    checksum: current.checksum,
  });
});

// ─── 11. GET /services/:name/pending ────────────────────────────────────────

app.get("/services/:name/pending", async (c) => {
  const name = c.req.param("name");

  const pending = await c.env.DB.prepare(
    "SELECT * FROM migrations WHERE service_name = ? AND status = 'pending' ORDER BY version ASC"
  ).bind(name).all<Migration>();

  return c.json({ service_name: name, pending: pending.results, count: pending.results.length });
});

// ─── 12. POST /services/:name/snapshot ──────────────────────────────────────

app.post("/services/:name/snapshot", async (c) => {
  const name = c.req.param("name");

  let body: SnapshotBody;
  try {
    body = await c.req.json<SnapshotBody>();
  } catch {
    return jsonError("Invalid JSON body", 400);
  }

  if (!body.snapshot || !body.version) {
    return jsonError("Missing required fields: snapshot, version", 400);
  }

  const result = await c.env.DB.prepare(
    `INSERT INTO schema_snapshots (service_name, version, snapshot, table_count, index_count, created_at)
     VALUES (?, ?, ?, ?, ?, ?)`
  ).bind(
    name,
    body.version,
    body.snapshot,
    body.table_count ?? 0,
    body.index_count ?? 0,
    now(),
  ).run();

  log("info", "Schema snapshot captured", { service: name, version: body.version });

  return c.json({
    id: result.meta.last_row_id,
    service_name: name,
    version: body.version,
    message: "Snapshot captured",
  }, 201);
});

// ─── 13. GET /services/:name/snapshots ──────────────────────────────────────

app.get("/services/:name/snapshots", async (c) => {
  const name = c.req.param("name");

  const snapshots = await c.env.DB.prepare(
    "SELECT id, service_name, version, table_count, index_count, created_at FROM schema_snapshots WHERE service_name = ? ORDER BY created_at DESC"
  ).bind(name).all<Omit<SchemaSnapshot, "snapshot">>();

  return c.json({ service_name: name, snapshots: snapshots.results, count: snapshots.results.length });
});

// ─── 14. GET /services/:name/diff ───────────────────────────────────────────

app.get("/services/:name/diff", async (c) => {
  const name = c.req.param("name");
  const from = c.req.query("from");
  const to = c.req.query("to");

  if (!from || !to) {
    return jsonError("Query parameters 'from' and 'to' (version strings) are required", 400);
  }

  const [snapFrom, snapTo] = await Promise.all([
    c.env.DB.prepare(
      "SELECT * FROM schema_snapshots WHERE service_name = ? AND version = ? ORDER BY created_at DESC LIMIT 1"
    ).bind(name, from).first<SchemaSnapshot>(),
    c.env.DB.prepare(
      "SELECT * FROM schema_snapshots WHERE service_name = ? AND version = ? ORDER BY created_at DESC LIMIT 1"
    ).bind(name, to).first<SchemaSnapshot>(),
  ]);

  if (!snapFrom) return jsonError(`Snapshot not found for version ${from}`, 404);
  if (!snapTo) return jsonError(`Snapshot not found for version ${to}`, 404);

  // Compute line-level diff
  const fromLines = snapFrom.snapshot.split("\n");
  const toLines = snapTo.snapshot.split("\n");

  const fromSet = new Set(fromLines);
  const toSet = new Set(toLines);

  const added = toLines.filter((line) => !fromSet.has(line));
  const removed = fromLines.filter((line) => !toSet.has(line));

  // Migrations between versions
  const migrations = await c.env.DB.prepare(
    "SELECT id, migration_name, version, status FROM migrations WHERE service_name = ? AND version > ? AND version <= ? ORDER BY version"
  ).bind(name, from, to).all<Migration>();

  return c.json({
    service_name: name,
    from_version: from,
    to_version: to,
    diff: {
      added,
      removed,
      lines_added: added.length,
      lines_removed: removed.length,
    },
    table_count_change: snapTo.table_count - snapFrom.table_count,
    index_count_change: snapTo.index_count - snapFrom.index_count,
    migrations_between: migrations.results,
  });
});

// ─── 15. POST /plans ────────────────────────────────────────────────────────

app.post("/plans", async (c) => {
  let body: CreatePlanBody;
  try {
    body = await c.req.json<CreatePlanBody>();
  } catch {
    return jsonError("Invalid JSON body", 400);
  }

  if (!body.name || !body.migration_ids || body.migration_ids.length === 0) {
    return jsonError("Missing required fields: name, migration_ids (non-empty array)", 400);
  }

  // Validate all migration IDs exist
  const placeholders = body.migration_ids.map(() => "?").join(",");
  const existing = await c.env.DB.prepare(
    `SELECT id, service_name FROM migrations WHERE id IN (${placeholders})`
  ).bind(...body.migration_ids).all<{ id: number; service_name: string }>();

  if (existing.results.length !== body.migration_ids.length) {
    const found = new Set(existing.results.map((r) => r.id));
    const missing = body.migration_ids.filter((id) => !found.has(id));
    return jsonError(`Migration IDs not found: ${missing.join(", ")}`, 404);
  }

  // Determine if cross-service
  const services = new Set(existing.results.map((r) => r.service_name));
  const crossService = services.size > 1 ? 1 : 0;

  const result = await c.env.DB.prepare(
    `INSERT INTO migration_plans (name, description, status, migrations, cross_service, created_at)
     VALUES (?, ?, 'draft', ?, ?, ?)`
  ).bind(
    body.name,
    body.description ?? "",
    JSON.stringify(body.migration_ids),
    crossService,
    now(),
  ).run();

  log("info", "Migration plan created", { id: result.meta.last_row_id, name: body.name, cross_service: crossService });

  return c.json({
    id: result.meta.last_row_id,
    name: body.name,
    status: "draft",
    cross_service: crossService > 0,
    migration_count: body.migration_ids.length,
  }, 201);
});

// ─── 16. GET /plans ─────────────────────────────────────────────────────────

app.get("/plans", async (c) => {
  const status = c.req.query("status");
  let sql = "SELECT * FROM migration_plans";
  const params: string[] = [];

  if (status) {
    sql += " WHERE status = ?";
    params.push(status);
  }
  sql += " ORDER BY created_at DESC";

  const result = await c.env.DB.prepare(sql).bind(...params).all<MigrationPlan>();
  return c.json({ plans: result.results, count: result.results.length });
});

// ─── 17. GET /plans/:id ─────────────────────────────────────────────────────

app.get("/plans/:id", async (c) => {
  const id = c.req.param("id");
  const plan = await c.env.DB.prepare("SELECT * FROM migration_plans WHERE id = ?")
    .bind(id)
    .first<MigrationPlan>();

  if (!plan) return jsonError("Plan not found", 404);

  // Fetch the actual migrations
  const migrationIds: number[] = JSON.parse(plan.migrations);
  let migrations: Migration[] = [];

  if (migrationIds.length > 0) {
    const placeholders = migrationIds.map(() => "?").join(",");
    const result = await c.env.DB.prepare(
      `SELECT * FROM migrations WHERE id IN (${placeholders}) ORDER BY version ASC`
    ).bind(...migrationIds).all<Migration>();
    migrations = result.results;
  }

  return c.json({ plan, migrations });
});

// ─── 18. POST /plans/:id/approve ────────────────────────────────────────────

app.post("/plans/:id/approve", async (c) => {
  const id = c.req.param("id");
  const plan = await c.env.DB.prepare("SELECT * FROM migration_plans WHERE id = ?")
    .bind(id)
    .first<MigrationPlan>();

  if (!plan) return jsonError("Plan not found", 404);
  if (plan.status !== "draft") return jsonError(`Plan cannot be approved in status: ${plan.status}`, 400);

  await c.env.DB.prepare(
    "UPDATE migration_plans SET status = 'approved' WHERE id = ?"
  ).bind(id).run();

  log("info", "Migration plan approved", { id });
  return c.json({ id: Number(id), status: "approved", message: "Plan approved for execution" });
});

// ─── 19. POST /plans/:id/execute ────────────────────────────────────────────

app.post("/plans/:id/execute", async (c) => {
  const id = c.req.param("id");
  const plan = await c.env.DB.prepare("SELECT * FROM migration_plans WHERE id = ?")
    .bind(id)
    .first<MigrationPlan>();

  if (!plan) return jsonError("Plan not found", 404);
  if (plan.status !== "approved") return jsonError(`Plan must be approved first (current status: ${plan.status})`, 400);

  const migrationIds: number[] = JSON.parse(plan.migrations);
  const executedAt = now();

  await c.env.DB.prepare(
    "UPDATE migration_plans SET status = 'executing', executed_at = ? WHERE id = ?"
  ).bind(executedAt, id).run();

  const results: Array<{ migration_id: number; status: string; error?: string }> = [];
  let allSuccess = true;

  for (const migId of migrationIds) {
    const migration = await c.env.DB.prepare("SELECT * FROM migrations WHERE id = ?")
      .bind(migId)
      .first<Migration>();

    if (!migration) {
      results.push({ migration_id: migId, status: "skipped", error: "Not found" });
      await c.env.DB.prepare(
        "INSERT INTO migration_history (migration_id, action, result, error_message, executed_at) VALUES (?, 'skip', 'Migration not found', 'Migration not found', ?)"
      ).bind(migId, now()).run();
      continue;
    }

    if (migration.status === "applied") {
      results.push({ migration_id: migId, status: "skipped", error: "Already applied" });
      await c.env.DB.prepare(
        "INSERT INTO migration_history (migration_id, action, result, executed_at) VALUES (?, 'skip', 'Already applied', ?)"
      ).bind(migId, now()).run();
      continue;
    }

    if (migration.status !== "pending") {
      results.push({ migration_id: migId, status: "skipped", error: `Status is ${migration.status}` });
      allSuccess = false;
      continue;
    }

    // Mark as applied (cannot execute remote SQL from this Worker)
    const applyTime = now();
    await c.env.DB.prepare(
      "UPDATE migrations SET status = 'applied', applied_at = ?, applied_by = 'plan-executor' WHERE id = ?"
    ).bind(applyTime, migId).run();

    await c.env.DB.prepare(
      "INSERT INTO migration_history (migration_id, action, result, executed_at) VALUES (?, 'apply', 'Applied via plan execution', ?)"
    ).bind(migId, applyTime).run();

    results.push({ migration_id: migId, status: "applied" });
    log("info", "Plan migration applied", { plan_id: id, migration_id: migId });
  }

  const completedAt = now();
  const finalStatus = allSuccess ? "completed" : "failed";

  await c.env.DB.prepare(
    "UPDATE migration_plans SET status = ?, completed_at = ? WHERE id = ?"
  ).bind(finalStatus, completedAt, id).run();

  await c.env.CACHE.delete("stats_cache");

  return c.json({
    plan_id: Number(id),
    status: finalStatus,
    executed_at: executedAt,
    completed_at: completedAt,
    results,
  });
});

// ─── 20. POST /lock/:service ────────────────────────────────────────────────

app.post("/lock/:service", async (c) => {
  const service = c.req.param("service");

  let body: LockBody;
  try {
    body = await c.req.json<LockBody>();
  } catch {
    return jsonError("Invalid JSON body — required: { locked_by: string }", 400);
  }

  if (!body.locked_by) return jsonError("Missing required field: locked_by", 400);

  // Check existing lock
  const existing = await c.env.DB.prepare(
    "SELECT * FROM migration_locks WHERE service_name = ?"
  ).bind(service).first<MigrationLock>();

  if (existing) {
    const expiresAt = new Date(existing.expires_at).getTime();
    if (Date.now() < expiresAt) {
      return jsonError(`Service ${service} already locked by ${existing.locked_by} until ${existing.expires_at}`, 423);
    }
    // Expired, remove
    await c.env.DB.prepare("DELETE FROM migration_locks WHERE service_name = ?")
      .bind(service).run();
  }

  const ttlMinutes = body.ttl_minutes ?? 30;
  const lockedAt = now();
  const expiresAt = new Date(Date.now() + ttlMinutes * 60 * 1000).toISOString();

  await c.env.DB.prepare(
    "INSERT INTO migration_locks (service_name, locked_by, locked_at, expires_at) VALUES (?, ?, ?, ?)"
  ).bind(service, body.locked_by, lockedAt, expiresAt).run();

  log("info", "Lock acquired", { service, locked_by: body.locked_by, ttl_minutes: ttlMinutes });

  return c.json({
    service_name: service,
    locked_by: body.locked_by,
    locked_at: lockedAt,
    expires_at: expiresAt,
    ttl_minutes: ttlMinutes,
  }, 201);
});

// ─── 21. POST /unlock/:service ──────────────────────────────────────────────

app.post("/unlock/:service", async (c) => {
  const service = c.req.param("service");

  const existing = await c.env.DB.prepare(
    "SELECT * FROM migration_locks WHERE service_name = ?"
  ).bind(service).first<MigrationLock>();

  if (!existing) return jsonError(`No lock found for service: ${service}`, 404);

  await c.env.DB.prepare("DELETE FROM migration_locks WHERE service_name = ?")
    .bind(service).run();

  log("info", "Lock released", { service, was_locked_by: existing.locked_by });

  return c.json({ service_name: service, message: "Lock released", was_locked_by: existing.locked_by });
});

// ─── 22. GET /locks ─────────────────────────────────────────────────────────

app.get("/locks", async (c) => {
  const locks = await c.env.DB.prepare(
    "SELECT * FROM migration_locks ORDER BY locked_at DESC"
  ).all<MigrationLock>();

  // Annotate with expired status
  const annotated = locks.results.map((lock) => ({
    ...lock,
    is_expired: new Date(lock.expires_at).getTime() < Date.now(),
  }));

  return c.json({ locks: annotated, count: annotated.length });
});

// ─── 23. GET /report ────────────────────────────────────────────────────────

app.get("/report", async (c) => {
  const [services, totals, recentFailed, activeLocks, pendingPlans] = await Promise.all([
    c.env.DB.prepare(
      `SELECT
         service_name,
         COUNT(*) as total,
         SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
         SUM(CASE WHEN status = 'applied' THEN 1 ELSE 0 END) as applied,
         SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
         MAX(CASE WHEN status = 'applied' THEN version ELSE NULL END) as current_version
       FROM migrations GROUP BY service_name ORDER BY service_name`
    ).all(),
    c.env.DB.prepare(
      `SELECT
         COUNT(*) as total,
         SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
         SUM(CASE WHEN status = 'applied' THEN 1 ELSE 0 END) as applied,
         SUM(CASE WHEN status = 'rolled_back' THEN 1 ELSE 0 END) as rolled_back,
         SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
       FROM migrations`
    ).first(),
    c.env.DB.prepare(
      "SELECT * FROM migrations WHERE status = 'failed' ORDER BY created_at DESC LIMIT 5"
    ).all<Migration>(),
    c.env.DB.prepare("SELECT * FROM migration_locks").all<MigrationLock>(),
    c.env.DB.prepare(
      "SELECT * FROM migration_plans WHERE status IN ('draft', 'approved') ORDER BY created_at DESC LIMIT 10"
    ).all<MigrationPlan>(),
  ]);

  const activeLockAnnotated = activeLocks.results.map((lock) => ({
    ...lock,
    is_expired: new Date(lock.expires_at).getTime() < Date.now(),
  }));

  return c.json({
    report_generated_at: now(),
    summary: totals,
    services: services.results,
    recent_failures: recentFailed.results,
    active_locks: activeLockAnnotated,
    pending_plans: pendingPlans.results,
    health: {
      has_failures: (recentFailed.results.length > 0),
      has_expired_locks: activeLockAnnotated.some((l) => l.is_expired),
      pending_count: totals?.pending ?? 0,
    },
  });
});

// ─── 24. POST /validate ─────────────────────────────────────────────────────

app.post("/validate", async (c) => {
  let body: ValidateBody;
  try {
    body = await c.req.json<ValidateBody>();
  } catch {
    return jsonError("Invalid JSON body — required: { sql: string }", 400);
  }

  if (!body.sql || typeof body.sql !== "string") {
    return jsonError("Missing required field: sql (string)", 400);
  }

  const sql = body.sql.trim();
  const issues: string[] = [];
  const warnings: string[] = [];

  // Basic syntax checks
  if (sql.length === 0) {
    issues.push("SQL is empty");
  }

  // Check for dangerous operations
  const dangerous = ["DROP DATABASE", "TRUNCATE TABLE"];
  for (const d of dangerous) {
    if (sql.toUpperCase().includes(d)) {
      warnings.push(`Contains dangerous operation: ${d}`);
    }
  }

  // Check for multiple statements
  const statements = sql.split(";").filter((s) => s.trim().length > 0);
  if (statements.length > 1) {
    warnings.push(`Contains ${statements.length} statements — ensure D1 batch support is used`);
  }

  // Check for common SQL keywords
  const validStarters = [
    "CREATE", "ALTER", "DROP", "INSERT", "UPDATE", "DELETE",
    "SELECT", "PRAGMA", "BEGIN", "COMMIT", "ROLLBACK", "WITH",
  ];
  const firstWord = sql.split(/\s+/)[0]?.toUpperCase();
  if (firstWord && !validStarters.includes(firstWord)) {
    issues.push(`Unexpected SQL start: "${firstWord}" — expected one of: ${validStarters.join(", ")}`);
  }

  // Check balanced parentheses
  let parenDepth = 0;
  for (const char of sql) {
    if (char === "(") parenDepth++;
    if (char === ")") parenDepth--;
    if (parenDepth < 0) {
      issues.push("Unbalanced parentheses: extra closing paren");
      break;
    }
  }
  if (parenDepth > 0) {
    issues.push("Unbalanced parentheses: missing closing paren");
  }

  // Check for unclosed quotes
  let inSingle = false;
  let inDouble = false;
  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i]!;
    const prev = i > 0 ? sql[i - 1] : "";
    if (ch === "'" && !inDouble && prev !== "\\") inSingle = !inSingle;
    if (ch === '"' && !inSingle && prev !== "\\") inDouble = !inDouble;
  }
  if (inSingle) issues.push("Unclosed single quote detected");
  if (inDouble) issues.push("Unclosed double quote detected");

  const checksum = await sha256(sql);

  return c.json({
    valid: issues.length === 0,
    issues,
    warnings,
    statement_count: statements.length,
    checksum,
    sql_preview: sql.length > 200 ? sql.substring(0, 200) + "..." : sql,
  });
});

// ─── Cron Handler ───────────────────────────────────────────────────────────

async function handleCron(env: Env): Promise<void> {
  log("info", "Daily migration check cron started");

  // Init schema
  await initSchema(env.DB);

  // 1. Release expired locks
  const locks = await env.DB.prepare("SELECT * FROM migration_locks").all<MigrationLock>();
  let expiredCount = 0;
  for (const lock of locks.results) {
    if (new Date(lock.expires_at).getTime() < Date.now()) {
      await env.DB.prepare("DELETE FROM migration_locks WHERE id = ?").bind(lock.id).run();
      expiredCount++;
      log("warn", "Expired lock released by cron", { service: lock.service_name, locked_by: lock.locked_by });
    }
  }

  // 2. Check for pending migrations
  const pending = await env.DB.prepare(
    "SELECT service_name, COUNT(*) as count FROM migrations WHERE status = 'pending' GROUP BY service_name"
  ).all<{ service_name: string; count: number }>();

  // 3. Check for failed migrations
  const failed = await env.DB.prepare(
    "SELECT service_name, COUNT(*) as count FROM migrations WHERE status = 'failed' GROUP BY service_name"
  ).all<{ service_name: string; count: number }>();

  // 4. Generate summary
  const totalPending = pending.results.reduce((sum, r) => sum + r.count, 0);
  const totalFailed = failed.results.reduce((sum, r) => sum + r.count, 0);

  const summary = {
    timestamp: now(),
    expired_locks_released: expiredCount,
    total_pending_migrations: totalPending,
    pending_by_service: pending.results,
    total_failed_migrations: totalFailed,
    failed_by_service: failed.results,
  };

  // Cache report
  await env.CACHE.put("daily_cron_report", JSON.stringify(summary), { expirationTtl: 86400 });

  // Alert if failures or many pending
  if (totalFailed > 0 || totalPending > 10) {
    try {
      await env.ALERT_ROUTER.fetch("https://alert-router/alert", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          source: "echo-migration-manager",
          severity: totalFailed > 0 ? "warning" : "info",
          message: `Migration status: ${totalPending} pending, ${totalFailed} failed, ${expiredCount} expired locks released`,
          data: summary,
        }),
      });
    } catch (err) {
      log("warn", "Failed to send alert", { error: String(err) });
    }
  }

  // Post to Shared Brain
  try {
    await env.SHARED_BRAIN.fetch("https://shared-brain/api/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        role: "system",
        content: `[migration-manager cron] ${totalPending} pending, ${totalFailed} failed, ${expiredCount} expired locks released`,
        metadata: { source: "echo-migration-manager", type: "cron-report" },
      }),
    });
  } catch (err) {
    log("warn", "Failed to post to Shared Brain", { error: String(err) });
  }

  // Invalidate stats cache
  await env.CACHE.delete("stats_cache");

  log("info", "Daily migration check cron completed", summary);
}

// ─── Export ─────────────────────────────────────────────────────────────────

export default {
  fetch: app.fetch,
  scheduled: async (event: ScheduledEvent, env: Env, ctx: ExecutionContext) => {
    ctx.waitUntil(handleCron(env));
  },
};
