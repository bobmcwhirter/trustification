use crate::server::collector::CollectorConfig;
use crate::SharedState;

pub struct Collector {
    pub(crate) id: String,
    pub(crate) config: CollectorConfig,
}

impl Collector {
    pub fn new(id: String, config: CollectorConfig) -> Self {
        Self { id, config }
    }

    pub async fn gather(&self, state: SharedState, purls: Vec<String>) -> Vec<String> {
        let client = reqwest::Client::new()
            .post(&self.config.url)
            .json(&purls)
            .send();

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
}
