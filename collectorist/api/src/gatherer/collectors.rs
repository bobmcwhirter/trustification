use std::collections::{HashMap, HashSet};

use futures::future::join_all;
use futures::StreamExt;
use tokio::join;
use crate::gatherer::collector::Collector;

use crate::server::collect::CollectRequest;
use crate::server::collector::CollectorConfig;
use crate::SharedState;

#[derive(Default)]
pub struct Collectors {
    collectors: HashMap<String, Collector>,
}

impl Collectors {
    pub fn register(&mut self, id: String, config: CollectorConfig) -> Result<(), ()> {
        self.collectors.insert(id.clone(), Collector::new(id, config));
        Ok(())
    }

    pub fn deregister(&mut self, id: String) -> Result<bool, ()> {
        Ok(self.collectors.remove(&id).is_some())
    }

    #[allow(unused)]
    pub fn collector_ids(&self) -> impl Iterator<Item = &String> {
        self.collectors.keys()
    }

    pub fn collector_config(&self, id: String) -> Option<CollectorConfig> {
        self.collectors.get(&id).map(|e| e.config.clone())
    }

    pub async fn gather(&self, state: SharedState, request: CollectRequest) -> HashSet<String>{
        let mut futures = Vec::new();

        for (name, collector) in &self.collectors {
            futures.push(collector.gather(state.clone(), request.purls.clone()));
        }

        join_all(futures).await.into_iter().flatten().collect()
    }
}
