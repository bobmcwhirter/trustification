use std::str::FromStr;

use chrono::{DateTime, TimeZone, Utc};
use futures::{Stream, StreamExt};
use log::LevelFilter;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, Row, SqlitePool};

static DB_FILE_NAME: &str = "gatherer.db";

pub struct Db {
    pool: SqlitePool,
}

impl Db {
    pub async fn new() -> Result<Self, anyhow::Error> {
        let db = Self {
            pool: SqlitePool::connect_with(if cfg!(test) {
                SqliteConnectOptions::from_str(":memory:")?
            } else {
                SqliteConnectOptions::default()
                    .filename(DB_FILE_NAME)
                    .create_if_missing(true)
            })
            .await?,
        };
        db.initialize().await?;
        Ok(db)
    }

    pub async fn insert_purl(&self, purl: String) -> Result<(), anyhow::Error> {
        sqlx::query(
            r#"INSERT OR IGNORE INTO purls (purl) VALUES ($1)
            "#,
        )
        .bind(purl.clone())
        .bind(purl.clone())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_purls(&self) -> impl Stream<Item = String> {
        sqlx::query(r#"SELECT purl FROM purls"#)
            .fetch(&self.pool)
            .filter_map(|row| async move {
                if let Ok(row) = row {
                    Some(row.get::<String, _>("purl"))
                } else {
                    None
                }
            })
    }

    pub async fn update_purl_scan_time(&self, collector_id: String, purl: String) -> Result<(), anyhow::Error> {
        sqlx::query(r#"REPLACE INTO collector (collector, purl, timestamp) VALUES ($1, $2, $3)"#)
            .bind(collector_id.clone())
            .bind(purl.clone())
            .bind(Utc::now())
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn get_purl_scan_time(
        &self,
        collector_id: String,
        purl: String,
    ) -> Result<Option<DateTime<Utc>>, anyhow::Error> {
        Ok(sqlx::query(
            r#"
            SELECT
                timestamp
            FROM
                collector
            WHERE
                collector = $1 AND purl = $2"#,
        )
        .bind(collector_id.clone())
        .bind(purl.clone())
        .fetch_optional(&self.pool)
        .await?
        .map(|row| row.get::<DateTime<Utc>, _>("timestamp")))
    }

    pub async fn get_purls_to_scan(
        &self,
        collector_id: String,
        since: DateTime<Utc>,
        limit: u32,
    ) -> impl Stream<Item = String> {
        sqlx::query(
            r#"
            SELECT
                purl, timestamp
            FROM
                collector
            WHERE
                collector = $1 AND timestamp < $2
            UNION
            SELECT
                purl, 0
            FROM
                purls
            ORDER BY
                collector.timestamp ASC
            LIMIT
                $3
            "#,
        )
        .bind(collector_id)
        .bind(since)
        .bind(limit)
        .fetch(&self.pool)
        .filter_map(|row| async move {
            if let Ok(row) = row {
                Some(row.get::<String, _>("purl"))
            } else {
                None
            }
        })
    }

    async fn initialize(&self) -> Result<(), anyhow::Error> {
        self.create_purls_table().await?;
        self.create_collector_table().await?;
        Ok(())
    }

    async fn create_purls_table(&self) -> Result<(), anyhow::Error> {
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS purls (
                    purl text
                )"#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS purl_idx ON purls ( purl ) ;
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn create_collector_table(&self) -> Result<(), anyhow::Error> {
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS collector (
                    collector text,
                    purl text,
                    timestamp datetime
                )"#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS collector_idx ON collector ( purl ) ;
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::thread::sleep;

    use chrono::{DateTime, Duration, Utc};
    use futures::StreamExt;

    use crate::db::Db;

    #[actix_web::test]
    async fn insert_purl() -> Result<(), anyhow::Error> {
        let db = Db::new().await?;

        db.insert_purl("bob".into()).await?;
        db.insert_purl("bob".into()).await?;
        db.insert_purl("bob".into()).await?;
        db.insert_purl("bob".into()).await?;
        db.insert_purl("jens".into()).await?;
        db.insert_purl("jim".into()).await?;
        db.insert_purl("jim".into()).await?;
        db.insert_purl("jim".into()).await?;

        let mut result = Box::pin(db.get_purls().await);
        let purls: Vec<_> = result.collect().await;

        assert_eq!(3, purls.len());
        assert!(purls.contains(&"jens".to_owned()));
        assert!(purls.contains(&"jim".to_owned()));
        assert!(purls.contains(&"bob".to_owned()));
        Ok(())
    }

    #[actix_web::test]
    async fn update_purl_scan_time() -> Result<(), anyhow::Error> {
        let db = Db::new().await?;

        db.insert_purl("not-scanned".into()).await?;

        db.update_purl_scan_time("test-scanner".into(), "bob".into()).await?;
        let time_1 = db.get_purl_scan_time("test-scanner".into(), "bob".into()).await?;

        sleep(Duration::seconds(2).to_std()?);

        db.update_purl_scan_time("test-scanner".into(), "bob".into()).await?;
        let time_2 = db.get_purl_scan_time("test-scanner".into(), "bob".into()).await?;

        assert!(time_1 < time_2);

        sleep(Duration::seconds(2).to_std()?);
        let mut result = Box::pin(db.get_purls_to_scan("test-scanner".into(), Utc::now(), 10).await);
        let purls: Vec<_> = result.collect().await;

        assert_eq!(2, purls.len());
        assert!( purls.contains(&"bob".to_owned()) );
        assert!( purls.contains(&"not-scanned".to_owned()) );

        Ok(())
    }
}
