use std::{fmt::Display, ops::Bound, path::PathBuf};

use sikula::prelude::{Ordered, Primary, Search};
// Rexport to align versions
use log::{debug, warn};
pub use tantivy;
pub use tantivy::schema::Document;
use tantivy::{
    collector::{Count, TopDocs},
    directory::{MmapDirectory, INDEX_WRITER_LOCK},
    query::{BooleanQuery, Occur, Query, RangeQuery, RegexQuery, TermQuery},
    schema::*,
    DateTime, Directory, DocAddress, Index as SearchIndex, IndexSettings, Searcher,
};
use time::{OffsetDateTime, UtcOffset};

#[derive(Clone, Debug, clap::Parser)]
#[command(rename_all_env = "SCREAMING_SNAKE_CASE")]
pub struct IndexConfig {
    /// Local folder to store index.
    #[arg(short = 'i', long = "index-dir")]
    pub index: Option<std::path::PathBuf>,

    /// Synchronization interval for index persistence.
    #[arg(long = "index-sync-interval", default_value = "30s")]
    pub sync_interval: humantime::Duration,
}

pub struct IndexStore<INDEX: Index> {
    inner: SearchIndex,
    path: Option<PathBuf>,
    index: INDEX,
}

pub trait Index {
    type MatchedDocument: core::fmt::Debug;
    type Document;

    fn settings(&self) -> IndexSettings;
    fn schema(&self) -> Schema;
    fn prepare_query(&self, q: &str) -> Result<Box<dyn Query>, Error>;
    fn process_hit(
        &self,
        doc: DocAddress,
        score: f32,
        searcher: &Searcher,
        query: &dyn Query,
        explain: bool,
    ) -> Result<Self::MatchedDocument, Error>;
    fn index_doc(&self, id: &str, document: &Self::Document) -> Result<Vec<Document>, Error>;
    fn doc_id_to_term(&self, id: &str) -> Term;
}

#[derive(Debug)]
pub enum Error {
    Open,
    Snapshot,
    NotFound,
    NotPersisted,
    Parser(String),
    Search(tantivy::TantivyError),
    Io(std::io::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Open => write!(f, "Error opening index"),
            Self::Snapshot => write!(f, "Error snapshotting index"),
            Self::NotFound => write!(f, "Not found"),
            Self::NotPersisted => write!(f, "Database is not persisted"),
            Self::Parser(e) => write!(f, "Failed to parse query: {e}"),
            Self::Search(e) => write!(f, "Error in search index: {:?}", e),
            Self::Io(e) => write!(f, "I/O error: {:?}", e),
        }
    }
}

impl std::error::Error for Error {}

impl From<tantivy::TantivyError> for Error {
    fn from(e: tantivy::TantivyError) -> Self {
        Self::Search(e)
    }
}

pub struct IndexWriter {
    writer: tantivy::IndexWriter,
}

impl IndexWriter {
    pub fn add_document<INDEX: Index>(
        &mut self,
        index: &mut INDEX,
        id: &str,
        raw: &INDEX::Document,
    ) -> Result<(), Error> {
        self.delete_document(index, id);
        let docs = index.index_doc(id, raw)?;
        for doc in docs {
            self.writer.add_document(doc)?;
        }
        Ok(())
    }

    pub fn commit(mut self) -> Result<(), Error> {
        self.writer.commit()?;
        self.writer.wait_merging_threads()?;
        Ok(())
    }

    pub fn delete_document<INDEX: Index>(&self, index: &INDEX, key: &str) {
        let term = index.doc_id_to_term(key);
        self.writer.delete_term(term);
    }
}

impl<INDEX: Index> IndexStore<INDEX> {
    pub fn new_in_memory(index: INDEX) -> Result<Self, Error> {
        let schema = index.schema();
        let settings = index.settings();
        let builder = SearchIndex::builder().schema(schema).settings(settings);
        let inner = builder.create_in_ram()?;
        Ok(Self {
            inner,
            index,
            path: None,
        })
    }

    pub fn new(config: &IndexConfig, index: INDEX) -> Result<Self, Error> {
        let path = config.index.clone().unwrap_or_else(|| {
            use rand::RngCore;
            let r = rand::thread_rng().next_u32();
            std::env::temp_dir().join(format!("index.{}", r))
        });

        std::fs::create_dir(&path).map_err(|_| Error::Open)?;

        let schema = index.schema();
        let settings = index.settings();
        let builder = SearchIndex::builder().schema(schema).settings(settings);
        let dir = MmapDirectory::open(&path).map_err(|_e| Error::Open)?;
        let inner = builder.open_or_create(dir)?;
        Ok(Self {
            inner,
            path: Some(path),
            index,
        })
    }

    pub fn index(&self) -> &INDEX {
        &self.index
    }

    pub fn index_as_mut(&mut self) -> &mut INDEX {
        &mut self.index
    }

    pub fn restore(config: &IndexConfig, data: &[u8], index: INDEX) -> Result<Self, Error> {
        if let Some(path) = &config.index {
            let dec = zstd::stream::Decoder::new(data).map_err(Error::Io)?;
            let mut archive = tar::Archive::new(dec);
            archive.unpack(path).map_err(Error::Io)?;
            Self::new(config, index)
        } else {
            Err(Error::Open)
        }
    }

    pub fn reload(&mut self, data: &[u8]) -> Result<(), Error> {
        if let Some(path) = &self.path {
            let dec = zstd::stream::Decoder::new(data).map_err(Error::Io)?;
            let mut archive = tar::Archive::new(dec);
            archive.unpack(path).map_err(Error::Io)?;
        }
        Ok(())
    }

    pub fn snapshot(&mut self, writer: IndexWriter) -> Result<Vec<u8>, Error> {
        if let Some(path) = &self.path {
            log::info!("Committing index to path {:?}", path);
            writer.commit()?;
            self.inner.directory_mut().sync_directory().map_err(Error::Io)?;
            let lock = self.inner.directory_mut().acquire_lock(&INDEX_WRITER_LOCK);

            let mut out = Vec::new();
            log::info!("Creating encoder");
            let enc = zstd::stream::Encoder::new(&mut out, 3).map_err(Error::Io)?;
            log::info!("Creating builder");
            let mut archive = tar::Builder::new(enc.auto_finish());
            log::info!("Adding directories from {:?}", path);
            archive.append_dir_all("", path).map_err(Error::Io)?;
            log::info!("Added it all to the archive");
            drop(archive);
            drop(lock);
            Ok(out)
        } else {
            Err(Error::NotPersisted)
        }
    }

    pub fn writer(&mut self) -> Result<IndexWriter, Error> {
        let writer = self.inner.writer(100_000_000)?;
        Ok(IndexWriter { writer })
    }

    pub fn search(
        &self,
        q: &str,
        offset: usize,
        len: usize,
        explain: bool,
    ) -> Result<(Vec<INDEX::MatchedDocument>, usize), Error> {
        let reader = self.inner.reader()?;
        let searcher = reader.searcher();

        let query = self.index.prepare_query(q)?;

        debug!("Processed query: {:?}", query);

        let (top_docs, count) = searcher.search(&query, &(TopDocs::with_limit(len).and_offset(offset), Count))?;

        debug!("Found {} docs", count);

        let mut hits = Vec::new();
        for hit in top_docs {
            if let Ok(value) = self.index.process_hit(hit.1, hit.0, &searcher, &query, explain) {
                debug!("HIT: {:?}", value);
                hits.push(value);
            } else {
                warn!("Error processing hit {:?}", hit);
            }
        }

        debug!("Filtered to {}", hits.len());

        Ok((hits, count))
    }
}

/// Convert a sikula term to a query
pub fn term2query<'m, R: Search<'m>, F: Fn(&R::Parsed) -> Box<dyn Query>>(
    term: &sikula::prelude::Term<'m, R>,
    f: &F,
) -> Box<dyn Query> {
    match term {
        sikula::prelude::Term::Match(resource) => f(resource),
        sikula::prelude::Term::Not(term) => {
            let query_terms = vec![(Occur::MustNot, term2query(term, f))];
            let query = BooleanQuery::new(query_terms);
            Box::new(query)
        }
        sikula::prelude::Term::And(terms) => {
            let mut query_terms = Vec::new();
            for term in terms {
                query_terms.push(term2query(term, f));
            }
            Box::new(BooleanQuery::intersection(query_terms))
        }
        sikula::prelude::Term::Or(terms) => {
            let mut query_terms = Vec::new();
            for term in terms {
                query_terms.push(term2query(term, f));
            }
            Box::new(BooleanQuery::union(query_terms))
        }
    }
}

/// Crate a date query based on an ordered value
pub fn create_date_query(field: Field, value: &Ordered<time::OffsetDateTime>) -> Box<dyn Query> {
    match value {
        Ordered::Less(e) => Box::new(RangeQuery::new_term_bounds(
            field,
            Type::Date,
            &Bound::Unbounded,
            &Bound::Excluded(Term::from_field_date(field, DateTime::from_utc(*e))),
        )),
        Ordered::LessEqual(e) => Box::new(RangeQuery::new_term_bounds(
            field,
            Type::Date,
            &Bound::Unbounded,
            &Bound::Included(Term::from_field_date(field, DateTime::from_utc(*e))),
        )),
        Ordered::Greater(e) => Box::new(RangeQuery::new_term_bounds(
            field,
            Type::Date,
            &Bound::Excluded(Term::from_field_date(field, DateTime::from_utc(*e))),
            &Bound::Unbounded,
        )),
        Ordered::GreaterEqual(e) => Box::new(RangeQuery::new_term_bounds(
            field,
            Type::Date,
            &Bound::Included(Term::from_field_date(field, DateTime::from_utc(*e))),
            &Bound::Unbounded,
        )),
        Ordered::Equal(e) => {
            let theday = e.to_offset(UtcOffset::UTC).date();
            let theday_after = theday.next_day().unwrap_or(theday);

            let theday = theday.midnight().assume_utc();
            let theday_after = theday_after.midnight().assume_utc();

            let from = Bound::Included(Term::from_field_date(field, DateTime::from_utc(theday)));
            let to = Bound::Included(Term::from_field_date(field, DateTime::from_utc(theday_after)));
            Box::new(RangeQuery::new_term_bounds(field, Type::Date, &from, &to))
        }
        Ordered::Range(from, to) => {
            let from = bound_map(*from, |f| Term::from_field_date(field, DateTime::from_utc(f)));
            let to = bound_map(*to, |f| Term::from_field_date(field, DateTime::from_utc(f)));
            Box::new(RangeQuery::new_term_bounds(field, Type::Date, &from, &to))
        }
    }
}

/// Convert a sikula primary to a tantivy query for string fields
pub fn create_string_query(field: Field, primary: &Primary<'_>) -> Box<dyn Query> {
    match primary {
        Primary::Equal(value) => Box::new(TermQuery::new(Term::from_field_text(field, value), Default::default())),
        Primary::Partial(value) => {
            // Note: This could be expensive so consider alternatives
            let pattern = format!(".*{}.*", value);
            let mut queries: Vec<Box<dyn Query>> = Vec::new();
            if let Ok(query) = RegexQuery::from_pattern(&pattern, field) {
                queries.push(Box::new(query));
            } else {
                warn!("Unable to partial query from {}", pattern);
            }
            queries.push(Box::new(TermQuery::new(
                Term::from_field_text(field, value),
                Default::default(),
            )));
            Box::new(BooleanQuery::union(queries))
        }
    }
}

/// Convert a sikula primary to a tantivy query for text fields
pub fn create_text_query(field: Field, primary: &Primary<'_>) -> Box<dyn Query> {
    match primary {
        Primary::Equal(value) => Box::new(TermQuery::new(Term::from_field_text(field, value), Default::default())),
        Primary::Partial(value) => Box::new(TermQuery::new(Term::from_field_text(field, value), Default::default())),
    }
}

/// Map over a bound
pub fn bound_map<F: FnOnce(T) -> R, T, R>(bound: Bound<T>, func: F) -> Bound<R> {
    match bound {
        Bound::Included(f) => Bound::Included(func(f)),
        Bound::Excluded(f) => Bound::Excluded(func(f)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Create a boolean query
pub fn create_boolean_query(occur: Occur, term: Term) -> Box<dyn Query> {
    Box::new(BooleanQuery::new(vec![(
        occur,
        Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
    )]))
}

pub fn field2strvec(doc: &Document, field: Field) -> Result<Vec<&str>, Error> {
    Ok(doc.get_all(field).map(|s| s.as_text().unwrap_or_default()).collect())
}

pub fn field2f64vec(doc: &Document, field: Field) -> Result<Vec<f64>, Error> {
    Ok(doc.get_all(field).map(|s| s.as_f64().unwrap_or_default()).collect())
}

pub fn field2str(doc: &Document, field: Field) -> Result<&str, Error> {
    let value = doc.get_first(field).map(|s| s.as_text()).unwrap_or(None);
    value.map(Ok).unwrap_or(Err(Error::NotFound))
}

pub fn field2date(doc: &Document, field: Field) -> Result<OffsetDateTime, Error> {
    let value = doc.get_first(field).map(|s| s.as_date()).unwrap_or(None);
    value.map(|v| Ok(v.into_utc())).unwrap_or(Err(Error::NotFound))
}

pub fn field2float(doc: &Document, field: Field) -> Result<f64, Error> {
    let value = doc.get_first(field).map(|s| s.as_f64()).unwrap_or(None);
    value.map(Ok).unwrap_or(Err(Error::NotFound))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestIndex {
        schema: Schema,
        id: Field,
        text: Field,
    }

    impl TestIndex {
        pub fn new() -> Self {
            let mut builder = Schema::builder();
            let id = builder.add_text_field("id", STRING | FAST | STORED);
            let text = builder.add_text_field("text", TEXT);
            let schema = builder.build();
            Self { schema, id, text }
        }
    }

    impl Index for TestIndex {
        type MatchedDocument = String;
        type Document = String;

        fn settings(&self) -> IndexSettings {
            IndexSettings::default()
        }

        fn schema(&self) -> Schema {
            self.schema.clone()
        }

        fn prepare_query(&self, q: &str) -> Result<Box<dyn Query>, Error> {
            let queries: Vec<Box<dyn Query>> = vec![
                Box::new(TermQuery::new(
                    Term::from_field_text(self.id, q),
                    IndexRecordOption::Basic,
                )),
                Box::new(TermQuery::new(
                    Term::from_field_text(self.text, q),
                    IndexRecordOption::Basic,
                )),
            ];
            Ok(Box::new(BooleanQuery::union(queries)))
        }

        fn process_hit(
            &self,
            doc: DocAddress,
            _score: f32,
            searcher: &Searcher,
            _query: &dyn Query,
            _explain: bool,
        ) -> Result<Self::MatchedDocument, Error> {
            let d = searcher.doc(doc)?;
            let id = d.get_first(self.id).map(|v| v.as_text()).ok_or(Error::NotFound)?;
            Ok(id.unwrap_or("").to_string())
        }

        fn index_doc(&self, id: &str, document: &Self::Document) -> Result<Vec<Document>, Error> {
            let doc = tantivy::doc!(
                self.id => id.to_string(),
                self.text => document.to_string()
            );
            Ok(vec![doc])
        }

        fn doc_id_to_term(&self, id: &str) -> Term {
            Term::from_field_text(self.id, id)
        }
    }

    #[tokio::test]
    async fn test_basic_index() {
        let _ = tracing_subscriber::fmt::try_init();
        let mut store = IndexStore::new_in_memory(TestIndex::new()).unwrap();
        let mut writer = store.writer().unwrap();

        writer
            .add_document(store.index_as_mut(), "foo", &"Foo is great".to_string())
            .unwrap();

        writer.commit().unwrap();

        assert_eq!(store.search("is", 0, 10, false).unwrap().1, 1);
    }

    #[tokio::test]
    async fn test_index_removal() {
        let _ = tracing_subscriber::fmt::try_init();
        let mut store = IndexStore::new_in_memory(TestIndex::new()).unwrap();
        let mut writer = store.writer().unwrap();

        writer
            .add_document(store.index_as_mut(), "foo", &"Foo is great".to_string())
            .unwrap();

        writer.commit().unwrap();

        assert_eq!(store.search("is", 0, 10, false).unwrap().1, 1);

        let writer = store.writer().unwrap();
        writer.delete_document(store.index_as_mut(), "foo");
        writer.commit().unwrap();

        assert_eq!(store.search("is", 0, 10, false).unwrap().1, 0);
    }

    #[tokio::test]
    async fn test_duplicates() {
        let _ = tracing_subscriber::fmt::try_init();
        let mut store = IndexStore::new_in_memory(TestIndex::new()).unwrap();
        let mut writer = store.writer().unwrap();

        writer
            .add_document(store.index_as_mut(), "foo", &"Foo is great".to_string())
            .unwrap();

        writer
            .add_document(store.index_as_mut(), "foo", &"Foo is great".to_string())
            .unwrap();

        writer.commit().unwrap();

        assert_eq!(store.search("is", 0, 10, false).unwrap().1, 1);

        // Duplicates also removed if separate commits.
        let mut writer = store.writer().unwrap();
        writer
            .add_document(store.index_as_mut(), "foo", &"Foo is great".to_string())
            .unwrap();

        writer.commit().unwrap();

        assert_eq!(store.search("is", 0, 10, false).unwrap().1, 1);
    }
}
