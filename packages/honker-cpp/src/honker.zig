//! Zig C-ABI shim over SQLite + the honker extension.
//!
//! Exposes `honker_cpp_*` functions that the C++ header wraps into
//! RAII classes. Every function takes and returns C ABI types so the
//! C++ side doesn't need to care that the implementation is Zig.

const std = @import("std");
const c = @cImport({
    @cDefine("SQLITE_ENABLE_LOAD_EXTENSION", "1");
    @cInclude("sqlite3.h");
});

comptime {
    if (!@hasDecl(c, "sqlite3_load_extension")) {
        @compileError(
            "honker-cpp needs SQLite headers with loadable extension support. " ++
                "On macOS, Apple system SQLite is not enough; install SQLite with Homebrew/MacPorts " ++
                "or another package manager and pass -Dsqlite-prefix=/path/to/sqlite.",
        );
    }
}

// Zig 0.16 cTranslation cannot cast -1 to a function pointer; the
// strings we bind are valid for the entire call so SQLITE_STATIC is
// sufficient.
const SQLITE_STATIC: ?*const fn (?*anyopaque) callconv(.c) void = null;

// ---------------------------------------------------------------------
// Error codes (returned as i32)
// ---------------------------------------------------------------------
pub const HONKER_OK: i32 = 0;
pub const HONKER_ERR_OPEN: i32 = -1;
pub const HONKER_ERR_LOAD_EXT: i32 = -2;
pub const HONKER_ERR_BOOTSTRAP: i32 = -3;
pub const HONKER_ERR_SQL: i32 = -4;

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

fn dupe_cstring(src: []const u8) ?[*:0]u8 {
    const buf = std.c.malloc(src.len + 1) orelse return null;
    const bytes: [*]u8 = @ptrCast(buf);
    @memcpy(bytes[0..src.len], src);
    bytes[src.len] = 0;
    return @ptrCast(bytes);
}

fn exec_sql(db: ?*c.sqlite3, sql: [*:0]const u8) i32 {
    var errmsg: [*c]u8 = null;
    if (c.sqlite3_exec(db, sql, null, null, &errmsg) != c.SQLITE_OK) {
        if (errmsg) |e| c.sqlite3_free(e);
        return HONKER_ERR_SQL;
    }
    return HONKER_OK;
}

fn step_scalar_int64(stmt: ?*c.sqlite3_stmt) i64 {
    const rc = c.sqlite3_step(stmt);
    if (rc != c.SQLITE_ROW) return HONKER_ERR_SQL;
    return c.sqlite3_column_int64(stmt, 0);
}

fn step_scalar_text_duped(stmt: ?*c.sqlite3_stmt) ?[*:0]u8 {
    const rc = c.sqlite3_step(stmt);
    if (rc != c.SQLITE_ROW) return null;
    const txt = c.sqlite3_column_text(stmt, 0);
    if (txt == null) return null;
    return dupe_cstring(std.mem.span(@as([*:0]const u8, @ptrCast(txt))));
}

fn bind_text(stmt: ?*c.sqlite3_stmt, idx: c_int, z: [*:0]const u8) void {
    _ = c.sqlite3_bind_text(stmt, idx, z, -1, SQLITE_STATIC);
}

fn bind_optional_text(stmt: ?*c.sqlite3_stmt, idx: c_int, z: ?[*:0]const u8) void {
    if (z) |s| {
        bind_text(stmt, idx, s);
    } else {
        _ = c.sqlite3_bind_null(stmt, idx);
    }
}

fn sqlite3_load_honker_extension(db: ?*c.sqlite3, ext_path_z: [*:0]const u8, errmsg: *[*c]u8) c_int {
    if (comptime @hasDecl(c, "sqlite3_load_extension")) {
        return c.sqlite3_load_extension(db, ext_path_z, null, errmsg);
    }
    return c.SQLITE_ERROR;
}

// ---------------------------------------------------------------------
// open / close
// ---------------------------------------------------------------------

export fn honker_cpp_open(
    path_z: [*:0]const u8,
    ext_path_z: [*:0]const u8,
    out_db: *?*c.sqlite3,
) callconv(.c) i32 {
    var db: ?*c.sqlite3 = null;
    if (c.sqlite3_open(path_z, &db) != c.SQLITE_OK) {
        if (db) |d| _ = c.sqlite3_close(d);
        return HONKER_ERR_OPEN;
    }

    _ = c.sqlite3_db_config(
        db,
        c.SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION,
        @as(c_int, 1),
        @as(?*c_int, null),
    );
    var errmsg: [*c]u8 = null;
    if (sqlite3_load_honker_extension(db, ext_path_z, &errmsg) != c.SQLITE_OK) {
        if (errmsg) |e| c.sqlite3_free(e);
        _ = c.sqlite3_close(db);
        return HONKER_ERR_LOAD_EXT;
    }
    _ = c.sqlite3_db_config(
        db,
        c.SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION,
        @as(c_int, 0),
        @as(?*c_int, null),
    );

    const pragmas =
        "PRAGMA journal_mode = WAL;" ++
        "PRAGMA synchronous = NORMAL;" ++
        "PRAGMA busy_timeout = 5000;" ++
        "PRAGMA foreign_keys = ON;" ++
        "PRAGMA cache_size = -32000;" ++
        "PRAGMA temp_store = MEMORY;" ++
        "PRAGMA wal_autocheckpoint = 10000;";
    if (c.sqlite3_exec(db, pragmas, null, null, &errmsg) != c.SQLITE_OK) {
        if (errmsg) |e| c.sqlite3_free(e);
        _ = c.sqlite3_close(db);
        return HONKER_ERR_SQL;
    }

    if (c.sqlite3_exec(db, "SELECT honker_bootstrap()", null, null, &errmsg) != c.SQLITE_OK) {
        if (errmsg) |e| c.sqlite3_free(e);
        _ = c.sqlite3_close(db);
        return HONKER_ERR_BOOTSTRAP;
    }

    out_db.* = db;
    return HONKER_OK;
}

export fn honker_cpp_close(db: ?*c.sqlite3) callconv(.c) void {
    if (db) |d| _ = c.sqlite3_close(d);
}

// ---------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------

export fn honker_cpp_begin_tx(db: ?*c.sqlite3) callconv(.c) i32 {
    return exec_sql(db, "BEGIN IMMEDIATE");
}

export fn honker_cpp_commit_tx(db: ?*c.sqlite3) callconv(.c) i32 {
    return exec_sql(db, "COMMIT");
}

export fn honker_cpp_rollback_tx(db: ?*c.sqlite3) callconv(.c) i32 {
    return exec_sql(db, "ROLLBACK");
}

// ---------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------

export fn honker_cpp_enqueue(
    db: ?*c.sqlite3,
    queue_z: [*:0]const u8,
    payload_z: [*:0]const u8,
    delay_sec: i64,
    priority: i64,
    max_attempts: i64,
) callconv(.c) i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_enqueue(?1, ?2, NULL, ?3, ?4, ?5, NULL)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    bind_text(stmt, 1, queue_z);
    bind_text(stmt, 2, payload_z);
    if (delay_sec == 0) {
        _ = c.sqlite3_bind_null(stmt, 3);
    } else {
        _ = c.sqlite3_bind_int64(stmt, 3, delay_sec);
    }
    _ = c.sqlite3_bind_int64(stmt, 4, priority);
    _ = c.sqlite3_bind_int64(stmt, 5, max_attempts);

    return step_scalar_int64(stmt);
}

export fn honker_cpp_claim_one(
    db: ?*c.sqlite3,
    queue_z: [*:0]const u8,
    worker_z: [*:0]const u8,
    visibility_timeout_s: i64,
) callconv(.c) ?[*:0]u8 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_claim_batch(?1, ?2, 1, ?3)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return null;
    defer _ = c.sqlite3_finalize(stmt);

    bind_text(stmt, 1, queue_z);
    bind_text(stmt, 2, worker_z);
    _ = c.sqlite3_bind_int64(stmt, 3, visibility_timeout_s);

    return step_scalar_text_duped(stmt);
}

export fn honker_cpp_claim_batch(
    db: ?*c.sqlite3,
    queue_z: [*:0]const u8,
    worker_z: [*:0]const u8,
    n: i64,
    visibility_timeout_s: i64,
) callconv(.c) ?[*:0]u8 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_claim_batch(?1, ?2, ?3, ?4)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return null;
    defer _ = c.sqlite3_finalize(stmt);

    bind_text(stmt, 1, queue_z);
    bind_text(stmt, 2, worker_z);
    _ = c.sqlite3_bind_int64(stmt, 3, n);
    _ = c.sqlite3_bind_int64(stmt, 4, visibility_timeout_s);

    return step_scalar_text_duped(stmt);
}

export fn honker_cpp_ack(
    db: ?*c.sqlite3,
    job_id: i64,
    worker_z: [*:0]const u8,
) callconv(.c) i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_ack(?1, ?2)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    _ = c.sqlite3_bind_int64(stmt, 1, job_id);
    bind_text(stmt, 2, worker_z);

    return step_scalar_int64(stmt);
}

export fn honker_cpp_ack_batch(
    db: ?*c.sqlite3,
    ids_json_z: [*:0]const u8,
    worker_z: [*:0]const u8,
) callconv(.c) i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_ack_batch(?1, ?2)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    bind_text(stmt, 1, ids_json_z);
    bind_text(stmt, 2, worker_z);

    return step_scalar_int64(stmt);
}

export fn honker_cpp_retry(
    db: ?*c.sqlite3,
    job_id: i64,
    worker_z: [*:0]const u8,
    delay_sec: i64,
    error_z: [*:0]const u8,
) callconv(.c) i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_retry(?1, ?2, ?3, ?4)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    _ = c.sqlite3_bind_int64(stmt, 1, job_id);
    bind_text(stmt, 2, worker_z);
    _ = c.sqlite3_bind_int64(stmt, 3, delay_sec);
    bind_text(stmt, 4, error_z);

    return step_scalar_int64(stmt);
}

export fn honker_cpp_fail(
    db: ?*c.sqlite3,
    job_id: i64,
    worker_z: [*:0]const u8,
    error_z: [*:0]const u8,
) callconv(.c) i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_fail(?1, ?2, ?3)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    _ = c.sqlite3_bind_int64(stmt, 1, job_id);
    bind_text(stmt, 2, worker_z);
    bind_text(stmt, 3, error_z);

    return step_scalar_int64(stmt);
}

export fn honker_cpp_heartbeat(
    db: ?*c.sqlite3,
    job_id: i64,
    worker_z: [*:0]const u8,
    extend_sec: i64,
) callconv(.c) i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_heartbeat(?1, ?2, ?3)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    _ = c.sqlite3_bind_int64(stmt, 1, job_id);
    bind_text(stmt, 2, worker_z);
    _ = c.sqlite3_bind_int64(stmt, 3, extend_sec);

    return step_scalar_int64(stmt);
}

export fn honker_cpp_sweep_expired(
    db: ?*c.sqlite3,
    queue_z: [*:0]const u8,
) callconv(.c) i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_sweep_expired(?1)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    bind_text(stmt, 1, queue_z);

    return step_scalar_int64(stmt);
}

// ---------------------------------------------------------------------
// Stream
// ---------------------------------------------------------------------

export fn honker_cpp_stream_publish(
    db: ?*c.sqlite3,
    topic_z: [*:0]const u8,
    key_z: ?[*:0]const u8,
    payload_z: [*:0]const u8,
) callconv(.c) i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_stream_publish(?1, ?2, ?3)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    bind_text(stmt, 1, topic_z);
    bind_optional_text(stmt, 2, key_z);
    bind_text(stmt, 3, payload_z);

    return step_scalar_int64(stmt);
}

export fn honker_cpp_stream_read_since(
    db: ?*c.sqlite3,
    topic_z: [*:0]const u8,
    offset: i64,
    limit: i64,
) callconv(.c) ?[*:0]u8 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_stream_read_since(?1, ?2, ?3)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return null;
    defer _ = c.sqlite3_finalize(stmt);

    bind_text(stmt, 1, topic_z);
    _ = c.sqlite3_bind_int64(stmt, 2, offset);
    _ = c.sqlite3_bind_int64(stmt, 3, limit);

    return step_scalar_text_duped(stmt);
}

export fn honker_cpp_stream_save_offset(
    db: ?*c.sqlite3,
    consumer_z: [*:0]const u8,
    topic_z: [*:0]const u8,
    offset: i64,
) callconv(.c) i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_stream_save_offset(?1, ?2, ?3)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    bind_text(stmt, 1, consumer_z);
    bind_text(stmt, 2, topic_z);
    _ = c.sqlite3_bind_int64(stmt, 3, offset);

    return step_scalar_int64(stmt);
}

export fn honker_cpp_stream_get_offset(
    db: ?*c.sqlite3,
    consumer_z: [*:0]const u8,
    topic_z: [*:0]const u8,
) callconv(.c) i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_stream_get_offset(?1, ?2)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    bind_text(stmt, 1, consumer_z);
    bind_text(stmt, 2, topic_z);

    return step_scalar_int64(stmt);
}

// ---------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------

export fn honker_cpp_scheduler_register(
    db: ?*c.sqlite3,
    name_z: [*:0]const u8,
    queue_z: [*:0]const u8,
    cron_z: [*:0]const u8,
    payload_z: [*:0]const u8,
    priority: i64,
    expires_sec: i64,
) callconv(.c) i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_scheduler_register(?1, ?2, ?3, ?4, ?5, ?6)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    bind_text(stmt, 1, name_z);
    bind_text(stmt, 2, queue_z);
    bind_text(stmt, 3, cron_z);
    bind_text(stmt, 4, payload_z);
    _ = c.sqlite3_bind_int64(stmt, 5, priority);
    if (expires_sec <= 0) {
        _ = c.sqlite3_bind_null(stmt, 6);
    } else {
        _ = c.sqlite3_bind_int64(stmt, 6, expires_sec);
    }

    return step_scalar_int64(stmt);
}

export fn honker_cpp_scheduler_unregister(
    db: ?*c.sqlite3,
    name_z: [*:0]const u8,
) callconv(.c) i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_scheduler_unregister(?1)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    bind_text(stmt, 1, name_z);

    return step_scalar_int64(stmt);
}

export fn honker_cpp_scheduler_tick(
    db: ?*c.sqlite3,
    now_unix: i64,
) callconv(.c) ?[*:0]u8 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_scheduler_tick(?1)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return null;
    defer _ = c.sqlite3_finalize(stmt);

    _ = c.sqlite3_bind_int64(stmt, 1, now_unix);

    return step_scalar_text_duped(stmt);
}

export fn honker_cpp_scheduler_soonest(
    db: ?*c.sqlite3,
) callconv(.c) i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_scheduler_soonest()";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    return step_scalar_int64(stmt);
}

// ---------------------------------------------------------------------
// Lock
// ---------------------------------------------------------------------

export fn honker_cpp_lock_acquire(
    db: ?*c.sqlite3,
    name_z: [*:0]const u8,
    owner_z: [*:0]const u8,
    ttl_sec: i64,
) callconv(.c) i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_lock_acquire(?1, ?2, ?3)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    bind_text(stmt, 1, name_z);
    bind_text(stmt, 2, owner_z);
    _ = c.sqlite3_bind_int64(stmt, 3, ttl_sec);

    return step_scalar_int64(stmt);
}

export fn honker_cpp_lock_release(
    db: ?*c.sqlite3,
    name_z: [*:0]const u8,
    owner_z: [*:0]const u8,
) callconv(.c) i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_lock_release(?1, ?2)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    bind_text(stmt, 1, name_z);
    bind_text(stmt, 2, owner_z);

    return step_scalar_int64(stmt);
}

export fn honker_cpp_lock_heartbeat(
    db: ?*c.sqlite3,
    name_z: [*:0]const u8,
    owner_z: [*:0]const u8,
    ttl_sec: i64,
) callconv(.c) i64 {
    // honker_lock_acquire uses INSERT OR IGNORE, so a pure heartbeat
    // needs a direct UPDATE on _honker_locks to refresh expires_at.
    var stmt: ?*c.sqlite3_stmt = null;
    const sql =
        "UPDATE _honker_locks SET expires_at = unixepoch() + ?3 " ++
        "WHERE name = ?1 AND owner = ?2 RETURNING 1";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    bind_text(stmt, 1, name_z);
    bind_text(stmt, 2, owner_z);
    _ = c.sqlite3_bind_int64(stmt, 3, ttl_sec);

    const rc = c.sqlite3_step(stmt);
    if (rc == c.SQLITE_ROW) return 1;
    return 0;
}

// ---------------------------------------------------------------------
// Rate limit
// ---------------------------------------------------------------------

export fn honker_cpp_rate_limit_try(
    db: ?*c.sqlite3,
    name_z: [*:0]const u8,
    limit: i64,
    per_sec: i64,
) callconv(.c) i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_rate_limit_try(?1, ?2, ?3)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    bind_text(stmt, 1, name_z);
    _ = c.sqlite3_bind_int64(stmt, 2, limit);
    _ = c.sqlite3_bind_int64(stmt, 3, per_sec);

    return step_scalar_int64(stmt);
}

// ---------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------

export fn honker_cpp_result_save(
    db: ?*c.sqlite3,
    job_id: i64,
    value_z: [*:0]const u8,
    ttl_sec: i64,
) callconv(.c) i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_result_save(?1, ?2, ?3)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    _ = c.sqlite3_bind_int64(stmt, 1, job_id);
    bind_text(stmt, 2, value_z);
    if (ttl_sec <= 0) {
        _ = c.sqlite3_bind_null(stmt, 3);
    } else {
        _ = c.sqlite3_bind_int64(stmt, 3, ttl_sec);
    }

    return step_scalar_int64(stmt);
}

export fn honker_cpp_result_get(
    db: ?*c.sqlite3,
    job_id: i64,
) callconv(.c) ?[*:0]u8 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_result_get(?1)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return null;
    defer _ = c.sqlite3_finalize(stmt);

    _ = c.sqlite3_bind_int64(stmt, 1, job_id);

    return step_scalar_text_duped(stmt);
}

export fn honker_cpp_result_sweep(
    db: ?*c.sqlite3,
) callconv(.c) i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT honker_result_sweep()";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    return step_scalar_int64(stmt);
}

// ---------------------------------------------------------------------
// Notify
// ---------------------------------------------------------------------

export fn honker_cpp_notify(
    db: ?*c.sqlite3,
    channel_z: [*:0]const u8,
    payload_z: [*:0]const u8,
) callconv(.c) i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT notify(?1, ?2)";
    if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return HONKER_ERR_SQL;
    defer _ = c.sqlite3_finalize(stmt);

    bind_text(stmt, 1, channel_z);
    bind_text(stmt, 2, payload_z);

    return step_scalar_int64(stmt);
}

// ---------------------------------------------------------------------
// Memory management for returned strings
// ---------------------------------------------------------------------

export fn honker_cpp_free(ptr: ?[*:0]u8) callconv(.c) void {
    if (ptr) |p| std.c.free(p);
}
