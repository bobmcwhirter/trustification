use futures::future::join_all;
use futures::join;
use osv::schema::Vulnerability;
use serde::{Deserialize, Serialize};

use collector_client::{GatherRequest, GatherResponse};

//schemafy::schemafy!("schema.json");

const QUERY_URL: &str = "https://api.osv.dev/v1/query";

pub struct OsvClient {}

#[derive(Serialize, Deserialize)]
pub struct QueryPackageRequest {
    package: Package,
}

#[derive(Serialize, Deserialize)]
pub struct Package {
    purl: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum QueryResponse {
    Vulnerabilities { vulns: Vec<Vulnerability> },
    NoResult(serde_json::Value),
}

impl OsvClient {
    pub async fn query(request: &GatherRequest) -> Result<GatherResponse, anyhow::Error> {
        let requests: Vec<_> = request
            .purls
            .iter()
            .map(|purl| {
                let json_body = serde_json::to_string(&QueryPackageRequest {
                    package: Package { purl: purl.clone() },
                })
                .ok()
                .unwrap_or("".to_string());

                async move {
                    (
                        purl.clone(),
                        reqwest::Client::new()
                            .post(QUERY_URL)
                            .json(&QueryPackageRequest {
                                package: Package { purl: purl.clone() },
                            })
                            .send()
                            .await,
                    )
                }
            })
            .collect();

        let responses = join_all(requests).await;

        let mut purls = Vec::new();
        for (purl, response) in responses {
            if let Ok(response) = response {
                let response: Result<QueryResponse, _> = response.json().await;
                if let Ok(response) = response {
                    println!("{:?}", response);
                }
                purls.push(purl);
            } else {
                println!("bogus");
            }
        }

        Ok(GatherResponse {
            purls,
        })

        //println!("{:?}", results);
    }
}

#[cfg(test)]
mod test {
    use std::str::from_utf8;
    use collector_client::GatherRequest;
    use crate::osv_client::OsvClient;

    #[actix_web::test]
    async fn test_ingest() -> Result<(), anyhow::Error> {
        let vulns = from_utf8(include_bytes!("log4j-vuln-osv.json"))?;
        println!("{:?}", vulns);

        Ok(())

    }

}
