use std::time::Duration;

use chrono::Utc;
use collector_client::GatherRequest;
use collectorist_client::CollectorConfig;
use futures::StreamExt;
use log::info;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::SharedState;

pub struct Collector {
    pub(crate) id: String,
    pub(crate) config: CollectorConfig,
    pub(crate) update: JoinHandle<()>,
}

impl Collector {
    pub fn new(state: SharedState, id: String, config: CollectorConfig) -> Self {
        let update = tokio::spawn(Collector::update(state.clone(), id.clone(), config.clone()));
        Self {
            id: id.clone(),
            config: config.clone(),
            update,
        }
    }

    pub async fn gather(&self, state: SharedState, purls: Vec<String>) -> Vec<String> {
        let client = reqwest::Client::new().post(&self.config.url).json(&purls).send();

        if let Ok(response) = client.await {
            if let Ok(retained) = response.json().await {
                retained
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    }

    pub async fn update(state: SharedState, id: String, config: CollectorConfig) {
        loop {
            if let Some(config) = state.collectors.read().await.collector_config(id.clone()) {
                let collector_url = config.url;
                info!("polling for {} -> {}", id, collector_url);
                let purls: Vec<String> = state
                    .db
                    .get_purls_to_scan(id.clone(), Utc::now() - chrono::Duration::seconds(5), 3)
                    .await
                    .collect()
                    .await;

                info!("sending {}", purls.len());

                let response = collector_client::Client::new(collector_url)
                    .gather(GatherRequest { purls })
                    .await;
            }
            sleep(Duration::from_secs(120)).await;
        }
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        self.update.abort();
    }
}
