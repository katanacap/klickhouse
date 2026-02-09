use crate::{query_parser, ClickhouseLock, FromSql};
use log::warn;
use refinery_core::traits::r#async::{AsyncMigrate, AsyncQuery, AsyncTransaction};
use refinery_core::Migration;
use std::borrow::Cow;
use std::marker::PhantomData;
use std::time::{Duration, Instant};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::{Client, KlickhouseError, Result, Row, Type, Value};

/// copied from refinery_core
#[allow(dead_code)]
enum State {
    Applied,
    Unapplied,
}

/// copied from refinery_core
#[allow(dead_code)]
enum TypeInner {
    Versioned,
    Unversioned,
}

/// copied from refinery_core
#[allow(dead_code)]
struct MigrationInner {
    state: State,
    name: String,
    checksum: u64,
    version: i32,
    prefix: TypeInner,
    sql: Option<String>,
    applied_on: Option<OffsetDateTime>,
}

impl MigrationInner {
    fn applied(
        version: i32,
        name: String,
        applied_on: OffsetDateTime,
        checksum: u64,
    ) -> MigrationInner {
        MigrationInner {
            state: State::Applied,
            name,
            checksum,
            version,
            // applied migrations are always versioned
            prefix: TypeInner::Versioned,
            sql: None,
            applied_on: Some(applied_on),
        }
    }
}

impl From<MigrationInner> for Migration {
    fn from(inner: MigrationInner) -> Self {
        // SAFETY: `MigrationInner` is a field-for-field copy of `refinery_core::Migration`
        // (which has no public constructor). The size assertion guards against layout changes
        // if `refinery-core` is updated. The exact version pin `=0.8.16` in Cargo.toml
        // ensures this layout is stable. If you bump refinery-core, re-verify the layout.
        assert_eq!(
            std::mem::size_of::<Migration>(),
            std::mem::size_of::<MigrationInner>(),
            "MigrationInner and refinery_core::Migration have different sizes -- \
             refinery-core layout may have changed, update MigrationInner accordingly"
        );
        assert_eq!(
            std::mem::align_of::<Migration>(),
            std::mem::align_of::<MigrationInner>(),
            "MigrationInner and refinery_core::Migration have different alignment"
        );
        unsafe { std::mem::transmute(inner) }
    }
}

impl Row for Migration {
    const COLUMN_COUNT: Option<usize> = Some(4);

    fn column_names() -> Option<Vec<Cow<'static, str>>> {
        Some(vec![
            "version".into(),
            "name".into(),
            "applied_on".into(),
            "checksum".into(),
        ])
    }

    fn deserialize_row(map: Vec<(&str, &Type, Value)>) -> Result<Self> {
        if map.len() != 4 {
            return Err(KlickhouseError::DeserializeError(
                "bad column count for migration".to_string(),
            ));
        }
        let mut version = None::<i32>;
        let mut name_out = None::<String>;
        let mut applied_on = None::<String>;
        let mut checksum = None::<String>;
        for (name, type_, value) in map {
            match name {
                "version" => {
                    if version.is_some() {
                        return Err(KlickhouseError::DeserializeError(
                            "duplicate version column".to_string(),
                        ));
                    }
                    version = Some(i32::from_sql(type_, value)?);
                }
                "name" => {
                    if name_out.is_some() {
                        return Err(KlickhouseError::DeserializeError(
                            "duplicate name column".to_string(),
                        ));
                    }
                    name_out = Some(String::from_sql(type_, value)?);
                }
                "applied_on" => {
                    if applied_on.is_some() {
                        return Err(KlickhouseError::DeserializeError(
                            "duplicate applied_on column".to_string(),
                        ));
                    }
                    applied_on = Some(String::from_sql(type_, value)?);
                }
                "checksum" => {
                    if checksum.is_some() {
                        return Err(KlickhouseError::DeserializeError(
                            "duplicate checksum column".to_string(),
                        ));
                    }
                    checksum = Some(String::from_sql(type_, value)?);
                }
                name => {
                    return Err(KlickhouseError::DeserializeError(format!(
                        "unexpected column {name}"
                    )));
                }
            }
        }
        if version.is_none() {
            return Err(KlickhouseError::DeserializeError(
                "missing version".to_string(),
            ));
        }
        if name_out.is_none() {
            return Err(KlickhouseError::DeserializeError(
                "missing name".to_string(),
            ));
        }
        if applied_on.is_none() {
            return Err(KlickhouseError::DeserializeError(
                "missing applied_on".to_string(),
            ));
        }
        if checksum.is_none() {
            return Err(KlickhouseError::DeserializeError(
                "missing checksum".to_string(),
            ));
        }
        let applied_on =
            OffsetDateTime::parse(applied_on.as_ref().unwrap(), &Rfc3339).map_err(|e| {
                KlickhouseError::DeserializeError(format!("failed to parse time: {:?}", e))
            })?;

        Ok(MigrationInner::applied(
            version.unwrap(),
            name_out.unwrap(),
            applied_on,
            checksum.unwrap().parse::<u64>().map_err(|e| {
                KlickhouseError::DeserializeError(format!("failed to parse checksum: {:?}", e))
            })?,
        )
        .into())
    }

    fn serialize_row(
        self,
        _type_hints: &indexmap::IndexMap<String, Type>,
    ) -> Result<Vec<(Cow<'static, str>, Value)>> {
        Err(KlickhouseError::NotImplemented(
            "Migration rows are read-only and cannot be serialized".to_string(),
        ))
    }
}

#[async_trait::async_trait]
impl AsyncTransaction for Client {
    type Error = KlickhouseError;

    async fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error> {
        let lock = ClickhouseLock::new(self.clone(), "refinery_exec");
        let start = Instant::now();
        let handle = loop {
            if let Some(handle) = lock.try_lock().await? {
                break handle;
            } else {
                let elapsed = start.elapsed();
                if elapsed > Duration::from_secs(300) {
                    return Err(KlickhouseError::Timeout(format!(
                        "migration lock acquisition timed out after {:?}",
                        elapsed,
                    )));
                }
                if elapsed > Duration::from_secs(60) {
                    warn!(
                        "migration lock held for {:?}, still waiting (will timeout at 5 min)",
                        elapsed,
                    );
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        };
        for query in queries {
            if query.is_empty() {
                continue;
            }
            for query in query_parser::split_query_statements(query) {
                if query.is_empty() {
                    continue;
                }
                Client::execute(self, query).await?;
            }
        }
        handle.unlock().await?;
        Ok(queries.len())
    }
}

#[async_trait::async_trait]
impl AsyncQuery<Vec<Migration>> for Client {
    async fn query(
        &mut self,
        query: &str,
    ) -> Result<Vec<Migration>, <Self as AsyncTransaction>::Error> {
        self.query_collect::<Migration>(query).await
    }
}

impl AsyncMigrate for Client {
    fn assert_migrations_table_query(migration_table_name: &str) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {migration_table_name}(
            version INT,
            name VARCHAR(255),
            applied_on VARCHAR(255),
            checksum VARCHAR(255)) Engine=MergeTree() ORDER BY version;"
        )
    }
}

pub trait ClusterName: Send + Sync {
    fn cluster_name() -> String;
    fn database() -> String;
}

/// Wrapper for Client to use migrations on clusters
pub struct ClusterMigration<T: ClusterName> {
    client: Client,
    _t: PhantomData<T>,
}

impl<T: ClusterName> ClusterMigration<T> {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            _t: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl<T: ClusterName> AsyncTransaction for ClusterMigration<T> {
    type Error = KlickhouseError;

    async fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error> {
        let lock = ClickhouseLock::new(self.client.clone(), "refinery_exec")
            .with_cluster(T::cluster_name());
        let start = Instant::now();
        let handle = loop {
            if let Some(handle) = lock.try_lock().await? {
                break handle;
            } else {
                let elapsed = start.elapsed();
                if elapsed > Duration::from_secs(300) {
                    return Err(KlickhouseError::Timeout(format!(
                        "cluster migration lock acquisition timed out after {:?}",
                        elapsed,
                    )));
                }
                if elapsed > Duration::from_secs(60) {
                    warn!(
                        "cluster migration lock held for {:?}, still waiting (will timeout at 5 min)",
                        elapsed,
                    );
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        };
        for query in queries {
            for query in query_parser::split_query_statements(query) {
                Client::execute(&self.client, query).await?;
            }
        }
        handle.unlock().await?;
        Ok(queries.len())
    }
}

#[async_trait::async_trait]
impl<T: ClusterName> AsyncQuery<Vec<Migration>> for ClusterMigration<T> {
    async fn query(
        &mut self,
        query: &str,
    ) -> Result<Vec<Migration>, <Self as AsyncTransaction>::Error> {
        <Client as AsyncQuery<Vec<Migration>>>::query(&mut self.client, query).await
    }
}

impl<T: ClusterName> AsyncMigrate for ClusterMigration<T> {
    fn assert_migrations_table_query(migration_table_name: &str) -> String {
        format!(
            r"CREATE TABLE IF NOT EXISTS {migration_table_name}_local ON CLUSTER {0}(
                version INT,
                name VARCHAR(255),
                applied_on VARCHAR(255),
                checksum VARCHAR(255)
            ) Engine=MergeTree() ORDER BY version;
            CREATE TABLE IF NOT EXISTS
                {migration_table_name}
            ON CLUSTER {0}
            AS {migration_table_name}_local ENGINE = Distributed({0}, {1}, {migration_table_name}_local, rand());
            ",
            T::cluster_name(),
            T::database()
        )
    }
}
