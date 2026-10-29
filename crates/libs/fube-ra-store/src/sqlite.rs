use crate::{
    api::{record_bytes, RECORD_OVERHEAD},
    supervisor::{lock, Shared},
    *,
};
use std::{
    fs::{self, File, OpenOptions},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{atomic::Ordering, Arc},
};
use tokio_rusqlite::{
    rusqlite::{self, params, types::ValueRef, Connection as Raw, ErrorCode, OpenFlags},
    Connection,
};

const FORMAT: &str = "fube.ra-store";
const VERSION: i64 = 1;
const APPLICATION_ID: i64 = 0x46524131;
const RECORD_SCHEMA: &str = "CREATE TABLE records (record_type TEXT NOT NULL, record_id TEXT NOT NULL, value BLOB NOT NULL, PRIMARY KEY(record_type, record_id)) STRICT, WITHOUT ROWID";
const META_SCHEMA: &str = "CREATE TABLE metadata (id INTEGER PRIMARY KEY CHECK(id = 1), format TEXT NOT NULL, version INTEGER NOT NULL, deployment TEXT NOT NULL, node TEXT NOT NULL) STRICT";

pub(crate) struct Sqlite {
    connection: Option<Connection>,
    path: PathBuf,
    settings: Settings,
}

fn io(error: std::io::Error) -> Error {
    Error::new(
        if error.kind() == std::io::ErrorKind::NotFound {
            ErrorKind::NotFound
        } else {
            ErrorKind::Io
        },
        error.to_string(),
    )
}

pub(crate) fn sql(error: rusqlite::Error, insert: bool) -> Error {
    let kind = match error.sqlite_error_code() {
        Some(ErrorCode::ConstraintViolation)
            if insert
                && error.sqlite_extended_error_code()
                    == Some(rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY) =>
        {
            ErrorKind::Conflict
        }
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked) => ErrorKind::Busy,
        Some(ErrorCode::SystemIoFailure | ErrorCode::CannotOpen | ErrorCode::ReadOnly) => {
            ErrorKind::Io
        }
        Some(ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase) => ErrorKind::Corrupt,
        Some(ErrorCode::DiskFull | ErrorCode::TooBig) => ErrorKind::Capacity,
        _ => ErrorKind::Sqlite,
    };
    Error::new(kind, error.to_string())
}

fn unknown(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Unknown, message)
}
fn incompatible(message: &str) -> Error {
    Error::new(ErrorKind::Incompatible, message)
}
fn capacity(message: &str) -> Error {
    Error::new(ErrorKind::Capacity, message)
}
fn raw<T>(result: rusqlite::Result<T>) -> Result<T> {
    result.map_err(|e| sql(e, false))
}

/// Trusted cooperative same-path ownership, not hostile path/administrator protection.
fn reserve(path: PathBuf, create: bool, settings: &Settings) -> Result<(PathBuf, File)> {
    let parent = fs::canonicalize(
        path.parent()
            .ok_or_else(|| incompatible("database needs parent"))?,
    )
    .map_err(io)?;
    let mode = fs::metadata(&parent).map_err(io)?.permissions().mode();
    if mode & 0o077 != 0 {
        return Err(incompatible("database parent must be private (0700)"));
    }
    let path = parent.join(
        path.file_name()
            .ok_or_else(|| incompatible("database needs filename"))?,
    );
    let mut sidecar = path.as_os_str().to_os_string();
    sidecar.push(".lock");
    let lease = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(sidecar)
        .map_err(io)?;
    let metadata = lease.metadata().map_err(io)?;
    if !metadata.is_file() || metadata.mode() & 0o077 != 0 || metadata.nlink() != 1 {
        return Err(incompatible("invalid private ownership sidecar"));
    }
    lease.try_lock().map_err(|error| match error {
        fs::TryLockError::WouldBlock => Error::new(
            ErrorKind::Busy,
            "another cooperative store owner holds this path",
        ),
        fs::TryLockError::Error(e) => io(e),
    })?;
    if create {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    Error::new(ErrorKind::Conflict, "create-new path already exists")
                } else {
                    io(e)
                }
            })?;
        file.sync_all().map_err(io)?;
    }
    let metadata = fs::symlink_metadata(&path).map_err(io)?;
    if !metadata.is_file() || metadata.mode() & 0o077 != 0 || metadata.nlink() != 1 {
        return Err(incompatible(
            "database must be a private regular unaliased file",
        ));
    }
    if !create && metadata.len() == 0 {
        return Err(incompatible("existing database is empty"));
    }
    if metadata.len() > settings.max_database_bytes {
        return Err(capacity("existing main database exceeds budget"));
    }
    Ok((path, lease))
}

impl Sqlite {
    pub(crate) async fn open(
        path: PathBuf,
        identity: Identity,
        create: bool,
        settings: Settings,
        shared: &Arc<Shared>,
    ) -> Result<Self> {
        // This function runs on the owned non-Tokio-application supervisor thread.
        let (path, lease) = reserve(path, create, &settings)?;
        *lock(&shared.resource) = Some(Box::new(lease));
        #[cfg(test)]
        shared.hooks.bootstrap_async().await;
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW;
        let connection = match Connection::open_with_flags(&path, flags).await {
            Ok(c) => c,
            Err(error) => {
                // This API returns raw SQLite construction failures; transport
                // construction panic is caught by the supervisor's outer owner.
                lock(&shared.resource).take();
                return Err(sql(error, false));
            }
        };
        let mut store = Self {
            connection: Some(connection),
            path: path.clone(),
            settings: settings.clone(),
        };
        let result = store
            .connection
            .as_ref()
            .unwrap()
            .call_raw(move |conn| {
                raw(conn.busy_timeout(settings.busy_timeout))?;
                configure_limits(conn, &settings)?;
                if create {
                    // WAL/FULL must precede the bootstrap metadata commit.
                    profile(conn, &settings)?;
                    raw(conn.execute_batch("BEGIN IMMEDIATE"))?;
                    let result = (|| {
                        raw(conn.execute_batch(RECORD_SCHEMA))?;
                        raw(conn.execute_batch(META_SCHEMA))?;
                        raw(conn.execute(
                            "INSERT INTO metadata VALUES (1, ?1, ?2, ?3, ?4)",
                            params![FORMAT, VERSION, identity.deployment, identity.node],
                        ))?;
                        raw(conn.pragma_update(None, "application_id", APPLICATION_ID))?;
                        raw(conn.pragma_update(None, "user_version", VERSION))?;
                        Ok(())
                    })();
                    complete_transaction(conn, result)?;
                    // The committed WAL plus containing directory entries must survive successful bootstrap.
                    File::open(&path).map_err(io)?.sync_all().map_err(io)?;
                    File::open(path.parent().unwrap())
                        .map_err(io)?
                        .sync_all()
                        .map_err(io)?;
                } else {
                    validate_existing(conn, &identity, &settings)?;
                    profile(conn, &settings)?;
                }
                Ok(())
            })
            .await
            .map_err(|_| unknown("bootstrap worker outcome lost"))
            .and_then(|r| r);
        if let Err(error) = result {
            store.close(shared).await?;
            return Err(error);
        }
        Ok(store)
    }

    pub(crate) async fn execute(
        &mut self,
        request: Request,
        shared: &Arc<Shared>,
    ) -> Result<Output> {
        let path = self.path.clone();
        let settings = self.settings.clone();
        let owner = shared.clone();
        let result = self
            .connection
            .as_ref()
            .unwrap()
            .call_raw(move |conn| {
                #[cfg(test)]
                if owner.hooks.lose_worker.swap(false, Ordering::SeqCst) {
                    panic!("injected real worker loss");
                }
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    execute(conn, request, &path, &settings, &owner)
                }));
                result.unwrap_or_else(|_| {
                    Err(unknown("database operation panicked; outcome unknown"))
                })
            })
            .await;
        match result {
            Ok(result) => result,
            Err(_) => {
                shared.worker_lost.store(true, Ordering::Release);
                Err(unknown("database worker transport lost"))
            }
        }
    }

    pub(crate) async fn close(&mut self, shared: &Arc<Shared>) -> Result<()> {
        let owner = shared.clone();
        // tokio-rusqlite close alone returns Ok on channel loss. Positively witness
        // consuming the DISK connection on its worker, then dispose its placeholder.
        self.connection
            .as_ref()
            .unwrap()
            .call_raw(move |conn| -> Result<()> {
                #[cfg(test)]
                if owner.hooks.fail_close.swap(false, Ordering::SeqCst) {
                    std::mem::forget(raw(conn.prepare("SELECT 1"))?);
                }
                let placeholder = raw(Raw::open_in_memory())?;
                let disk = std::mem::replace(conn, placeholder);
                match disk.close() {
                    Ok(()) => {
                        lock(&owner.resource).take();
                        Ok(())
                    }
                    Err((disk, error)) => {
                        let placeholder = std::mem::replace(conn, disk);
                        let _ = placeholder.close();
                        Err(unknown(format!(
                            "disk-close failed, ownership quarantined: {error}"
                        )))
                    }
                }
            })
            .await
            .map_err(|_| unknown("disk-close witness lost; ownership quarantined"))??;
        self.connection
            .take()
            .unwrap()
            .close()
            .await
            .map_err(|e| unknown(format!("placeholder worker cleanup failed: {e}")))?;
        Ok(())
    }
}

fn configure_limits(conn: &mut Raw, settings: &Settings) -> Result<()> {
    use rusqlite::limits::Limit;
    let row_limit =
        (settings.max_type_bytes + settings.max_id_bytes + settings.max_value_bytes + 4096) as i32;
    raw(conn.set_limit(Limit::SQLITE_LIMIT_LENGTH, row_limit))?;
    raw(conn.set_limit(Limit::SQLITE_LIMIT_SQL_LENGTH, 16384))?;
    raw(conn.set_limit(Limit::SQLITE_LIMIT_ATTACHED, 0))?;
    raw(conn.set_limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER, 8))?;
    Ok(())
}

fn pragma_i64(conn: &Raw, name: &str) -> Result<i64> {
    raw(conn.pragma_query_value(None, name, |row| row.get(0)))
}

fn profile(conn: &mut Raw, settings: &Settings) -> Result<()> {
    let mode: String =
        raw(conn.pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0)))?;
    raw(conn.pragma_update(None, "synchronous", "FULL"))?;
    if mode != "wal" || pragma_i64(conn, "synchronous")? != 2 {
        return Err(incompatible(
            "effective WAL/FULL durability profile unavailable",
        ));
    }
    raw(conn.pragma_update(None, "wal_autocheckpoint", 0))?;
    raw(conn.pragma_update(None, "journal_size_limit", 0))?;
    let page_size = pragma_i64(conn, "page_size")? as u64;
    let pages = settings.max_database_bytes / page_size;
    if (pragma_i64(conn, "page_count")? as u64) > pages {
        return Err(capacity("existing logical database exceeds page budget"));
    }
    let effective: i64 =
        raw(conn.pragma_update_and_check(None, "max_page_count", pages as i64, |row| row.get(0)))?;
    if effective as u64 != pages {
        return Err(capacity("cannot enforce database page budget"));
    }
    Ok(())
}

fn validate_existing(conn: &mut Raw, identity: &Identity, settings: &Settings) -> Result<()> {
    if pragma_i64(conn, "page_count")? as u64 * pragma_i64(conn, "page_size")? as u64
        > settings.max_database_bytes
    {
        return Err(capacity("existing logical database exceeds budget"));
    }
    let integrity: String = raw(conn.query_row("PRAGMA integrity_check(1)", [], |r| r.get(0)))?;
    if integrity != "ok" {
        return Err(Error::new(ErrorKind::Corrupt, integrity));
    }
    let schema = {
        let mut stmt = raw(conn.prepare(
            "SELECT name, sql FROM sqlite_schema WHERE substr(name, 1, 7) <> 'sqlite_' ORDER BY name",
        ))?;
        let rows =
            raw(stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))))?;
        raw(rows.take(3).collect::<rusqlite::Result<Vec<_>>>())?
    };
    if schema
        != vec![
            ("metadata".into(), META_SCHEMA.into()),
            ("records".into(), RECORD_SCHEMA.into()),
        ]
        || pragma_i64(conn, "application_id")? != APPLICATION_ID
        || pragma_i64(conn, "user_version")? != VERSION
    {
        return Err(incompatible(
            "unrecognized store format/schema/version; migration unsupported",
        ));
    }
    if pragma_count(conn, "SELECT count(*) FROM metadata")? != 1 {
        return Err(incompatible("missing/multiple store metadata records"));
    }
    let metadata: (i64, String, i64, String, String) = raw(conn.query_row(
        "SELECT id, format, version, deployment, node FROM metadata",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
    ))?;
    if metadata
        != (
            1,
            FORMAT.into(),
            VERSION,
            identity.deployment.clone(),
            identity.node.clone(),
        )
    {
        return Err(incompatible("metadata or deployment/node binding mismatch"));
    }
    scan(conn, None, settings, false)?;
    Ok(())
}

fn pragma_count(conn: &Raw, sql: &str) -> Result<i64> {
    raw(conn.query_row(sql, [], |r| r.get(0)))
}

fn scan(
    conn: &Raw,
    record_type: Option<&str>,
    settings: &Settings,
    snapshot: bool,
) -> Result<Vec<Record>> {
    let mut statement = raw(conn.prepare("SELECT record_type, record_id, value FROM records WHERE (?1 IS NULL OR record_type = ?1) ORDER BY record_type, record_id"))?;
    let mut rows = raw(statement.query(params![record_type]))?;
    let mut result = Vec::new();
    let mut count = 0;
    let mut bytes = 0;
    while let Some(row) = raw(rows.next())? {
        let (ValueRef::Text(t), ValueRef::Text(id), ValueRef::Blob(value)) = (
            raw(row.get_ref(0))?,
            raw(row.get_ref(1))?,
            raw(row.get_ref(2))?,
        ) else {
            return Err(Error::new(
                ErrorKind::Corrupt,
                "invalid record column types",
            ));
        };
        if t.is_empty()
            || t.len() > settings.max_type_bytes
            || id.is_empty()
            || id.len() > settings.max_id_bytes
            || value.len() > settings.max_value_bytes
            || t.contains(&0)
            || id.contains(&0)
        {
            return Err(capacity("stored record exceeds configured field budgets"));
        }
        count += 1;
        bytes += RECORD_OVERHEAD + t.len() + id.len() + value.len();
        if count > settings.max_records
            || bytes > settings.max_stored_bytes
            || (snapshot && bytes > settings.max_snapshot_bytes)
        {
            return Err(capacity("complete record/snapshot budget exceeded"));
        }
        let t = std::str::from_utf8(t)
            .map_err(|_| Error::new(ErrorKind::Corrupt, "invalid UTF-8 record type"))?;
        let id = std::str::from_utf8(id)
            .map_err(|_| Error::new(ErrorKind::Corrupt, "invalid UTF-8 record ID"))?;
        if snapshot {
            result.push(Record {
                key: RecordId::new(t, id),
                value: value.to_vec(),
            });
        }
    }
    Ok(result)
}

fn pressure(conn: &Raw, path: &Path, settings: &Settings, force: bool) -> Result<()> {
    let wal_bytes = || -> Result<u64> {
        let mut wal = path.as_os_str().to_os_string();
        wal.push("-wal");
        match fs::metadata(wal) {
            Ok(m) => Ok(m.len()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(io(e)),
        }
    };
    if force || wal_bytes()? >= settings.max_wal_bytes {
        let (busy, _, _): (i64, i64, i64) =
            raw(conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            }))?;
        if busy != 0 {
            return Err(Error::new(
                ErrorKind::Busy,
                "checkpoint pinned by another connection",
            ));
        }
    }
    let logical = pragma_i64(conn, "page_count")? as u64 * pragma_i64(conn, "page_size")? as u64;
    if logical > settings.max_database_bytes
        || fs::metadata(path).map_err(io)?.len() > settings.max_database_bytes
        || wal_bytes()? >= settings.max_wal_bytes
    {
        return Err(capacity("database/live-WAL pressure"));
    }
    Ok(())
}

fn complete_transaction(conn: &mut Raw, statement: Result<()>) -> Result<()> {
    let committing = statement.is_ok();
    let result = statement.and_then(|()| raw(conn.execute_batch("COMMIT")));
    if let Err(error) = result {
        if !conn.is_autocommit() && conn.execute_batch("ROLLBACK").is_err() {
            return Err(unknown(format!("rollback failed after {error}")));
        }
        if !conn.is_autocommit() || (committing && error.kind == ErrorKind::Io) {
            return Err(unknown(format!("transaction outcome uncertain: {error}")));
        }
        return Err(error);
    }
    Ok(())
}

fn execute(
    conn: &mut Raw,
    request: Request,
    path: &Path,
    settings: &Settings,
    shared: &Arc<Shared>,
) -> Result<Output> {
    if let Request::Enumerate(record_type) = request {
        raw(conn.execute_batch("BEGIN"))?;
        let rows = scan(conn, Some(&record_type), settings, true);
        complete_transaction(conn, rows.as_ref().map(|_| ()).map_err(Clone::clone))?;
        return rows.map(Output::Snapshot);
    }
    let maintenance = matches!(request, Request::Maintain);
    let gate = pressure(conn, path, settings, maintenance);
    shared.pressure.store(gate.is_err(), Ordering::Release);
    gate?;
    if maintenance {
        return Ok(Output::Written);
    }
    raw(conn.execute_batch("BEGIN IMMEDIATE"))?;
    let statement = (|| {
        let key = request.key().unwrap();
        let old: Option<i64> = raw(conn
            .query_row(
                "SELECT length(value) FROM records WHERE record_type=?1 AND record_id=?2",
                params![key.record_type, key.id],
                |r| r.get(0),
            )
            .optional())?;
        match (&request, old) {
            (Request::Insert(_), Some(_)) => {
                return Err(Error::new(ErrorKind::Conflict, "record already exists"))
            }
            (Request::Update(_) | Request::Delete(_), None) => {
                return Err(Error::new(ErrorKind::NotFound, "record not found"))
            }
            _ => {}
        }
        let (count, bytes): (i64, i64) = raw(conn.query_row("SELECT count(*), coalesce(sum(128 + length(CAST(record_type AS BLOB)) + length(CAST(record_id AS BLOB)) + length(value)),0) FROM records", [], |r| Ok((r.get(0)?,r.get(1)?))))?;
        let old_bytes = old.map_or(0, |v| {
            RECORD_OVERHEAD + key.record_type.len() + key.id.len() + v as usize
        });
        let new_bytes = match &request {
            Request::Insert(r) | Request::Update(r) => record_bytes(r),
            _ => 0,
        };
        if bytes as usize - old_bytes + new_bytes > settings.max_stored_bytes
            || (matches!(request, Request::Insert(_)) && count as usize >= settings.max_records)
        {
            return Err(capacity("stored record budget"));
        }
        match &request {
            Request::Insert(r) => {
                conn.execute(
                    "INSERT INTO records VALUES (?1,?2,?3)",
                    params![key.record_type, key.id, r.value],
                )
                .map_err(|e| sql(e, true))?;
            }
            Request::Update(r) => {
                if raw(conn.execute(
                    "UPDATE records SET value=?3 WHERE record_type=?1 AND record_id=?2",
                    params![key.record_type, key.id, r.value],
                ))? != 1
                {
                    return Err(Error::new(ErrorKind::NotFound, "record not found"));
                }
            }
            Request::Delete(_) => {
                if raw(conn.execute(
                    "DELETE FROM records WHERE record_type=?1 AND record_id=?2",
                    params![key.record_type, key.id],
                ))? != 1
                {
                    return Err(Error::new(ErrorKind::NotFound, "record not found"));
                }
            }
            _ => unreachable!(),
        }
        #[cfg(test)]
        {
            shared.hooks.before_blocking()?;
            if shared.hooks.commit_veto.swap(false, Ordering::SeqCst) {
                raw(conn.commit_hook(Some(|| true)))?;
            }
        }
        Ok(())
    })();
    let result = complete_transaction(conn, statement);
    #[cfg(test)]
    raw(conn.commit_hook(None::<fn() -> bool>))?;
    result?;
    #[cfg(test)]
    if shared
        .hooks
        .panic_after_commit
        .swap(false, Ordering::SeqCst)
    {
        panic!("injected post-COMMIT panic");
    }
    // Maintenance failure after COMMIT cannot rewrite a committed success.
    let mut wal = path.as_os_str().to_os_string();
    wal.push("-wal");
    match fs::metadata(wal) {
        Ok(metadata) => {
            shared
                .pressure
                .store(metadata.len() >= settings.max_wal_bytes, Ordering::Release);
        }
        Err(e) => {
            shared.pressure.store(true, Ordering::Release);
            shared.failure(io(e));
        }
    }
    Ok(Output::Written)
}

use rusqlite::OptionalExtension;
