use futures::future::join_all;
use futures::join;
use osv::schema::Vulnerability;
use serde::{Deserialize, Serialize};

use collector_client::GatherRequest;

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
    pub async fn query(request: &GatherRequest) -> Result<(), anyhow::Error> {
        let requests: Vec<_> = request
            .purls
            .iter()
            .map(|purl| {

                let json_body = serde_json::to_string(
                    &QueryPackageRequest {
                        package: Package {
                            purl: purl.clone(),
                        },
                    }
                ).ok().unwrap_or( "".to_string());

                println!("---------> {}", json_body);

                reqwest::Client::new()
                    .post(QUERY_URL)
                    .json(&QueryPackageRequest {
                        package: Package { purl: purl.clone() },
                    })
                    .send()
            })
            .collect();

        println!("A");

        let responses = join_all(requests).await;
        println!("B");

        for response in responses {
            println!("C");
            if let Ok(response) = response {
                println!("D");
                let response: Result<QueryResponse, _> = response.json().await;
                println!("E");
                println!("-- {:?}", response);
            } else {
                println!("bogus");
            }
        }

        Ok(())

        //println!("{:?}", results);
    }
}
