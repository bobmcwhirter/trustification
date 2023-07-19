use std::time::Duration;

use actix_web::{delete, get, post, web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};

use crate::SharedState;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CollectorConfig {
    pub(crate) url: String,
    #[serde(with = "humantime_serde", default = "default_cadence")]
    pub(crate) cadence: Duration,
}

pub fn default_cadence() -> Duration {
    Duration::from_secs(30 * 60)
}

/// Register a collector
#[utoipa::path(
    post,
    tag = "collectorist",
    path = "/api/v1/collector",
    responses(
        (status = 200, description = "Collector registered"),
        (status = BAD_REQUEST, description = "Missing valid id"),
    ),
)]
#[post("/collector/{id}")]
pub(crate) async fn register_collector(
    state: web::Data<SharedState>,
    id: web::Path<String>,
    config: web::Json<CollectorConfig>,
) -> actix_web::Result<impl Responder> {
    if state
        .collectors
        .write()
        .await
        .register((*id).clone(), (*config).clone())
        .is_ok()
    {
        Ok(HttpResponse::Ok().finish())
    } else {
        Ok(HttpResponse::InternalServerError().finish())
    }
}

/// Register a collector
#[utoipa::path(
    get,
    tag = "collectorist",
    path = "/api/v1/collector",
    responses(
        (status = 200, description = "Collector configuration located"),
        (status = BAD_REQUEST, description = "Missing valid id"),
    ),
)]
#[get("/collector/{id}")]
pub(crate) async fn collector_config(
    state: web::Data<SharedState>,
    id: web::Path<String>,
) -> actix_web::Result<impl Responder> {
    if let Some(config) = state.collectors.read().await.collector_config(id.clone()) {
        Ok(HttpResponse::Ok().json(config))
    } else {
        Ok(HttpResponse::InternalServerError().finish())
    }
}

/// Register a collector
#[utoipa::path(
    delete,
    tag = "collectorist",
    path = "/api/v1/collector",
    responses(
        (status = 200, description = "Collector unregistered"),
        (status = NOT_FOUND, description = "Collector not found"),
        (status = BAD_REQUEST, description = "Missing valid id"),
    ),
)]
#[delete("/collector/{id}")]
pub(crate) async fn deregister_collector(
    state: web::Data<SharedState>,
    id: web::Path<String>,
) -> actix_web::Result<impl Responder> {
    if let Ok(result) = state.collectors.write().await.deregister((*id).clone()) {
        if result {
            Ok(HttpResponse::Ok().finish())
        } else {
            Ok(HttpResponse::NotFound().finish())
        }
    } else {
        Ok(HttpResponse::InternalServerError().finish())
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use actix_web::http::StatusCode;
    use actix_web::test::TestRequest;
    use actix_web::{test, web, App};
    use serde_json::json;

    use crate::server::collector::CollectorConfig;
    use crate::server::config;
    use crate::state::AppState;
    use crate::SharedState;

    #[test]
    async fn collector_config_round_trip() -> Result<(), anyhow::Error> {
        let json = json!(
            {
                "url": "http://mycollector.example.com/",
                "cadence": "12m"
            }
        );

        let config: CollectorConfig = serde_json::from_str(serde_json::to_string(&json)?.as_str())?;
        assert_eq!(config.cadence, Duration::from_secs(12 * 60));

        Ok(())
    }

    #[actix_web::test]
    async fn register_collector() -> Result<(), anyhow::Error> {
        let state = SharedState::new(AppState::new("http://csub.example.com/".into()).await?);

        let request = TestRequest::post()
            .uri("/api/v1/collector/foo")
            .set_json(json!(
                {
                    "url": "http://example.com/collector-endpoint"
                }
            ))
            .to_request();

        let app = test::init_service(App::new().app_data(web::Data::new(state.clone())).configure(config)).await;

        test::call_service(&app, request).await;

        let collectors = state.collectors.read().await;

        if let Some(config) = collectors.collector_config("foo".into()) {
            assert_eq!(config.url.as_str(), "http://example.com/collector-endpoint");
        } else {
            panic!("no configuration for `foo`");
        }

        Ok(())
    }

    #[actix_web::test]
    async fn get_collector_config() -> Result<(), anyhow::Error> {
        let state = SharedState::new(AppState::new("http://csub.example.com/".into()).await?);
        let app = test::init_service(App::new().app_data(web::Data::new(state.clone())).configure(config)).await;

        let request = TestRequest::post()
            .uri("/api/v1/collector/foo")
            .set_json(json!(
                {
                    "url": "http://example.com/collector-endpoint"
                }
            ))
            .to_request();

        test::call_service(&app, request).await;

        {
            let collectors = state.collectors.read().await;

            if let Some(config) = collectors.collector_config("foo".into()) {
                assert_eq!(config.url.as_str(), "http://example.com/collector-endpoint");
            } else {
                panic!("no configuration for `foo`");
            }
        }

        let request = TestRequest::get().uri("/api/v1/collector/foo").to_request();

        let config: CollectorConfig = test::call_and_read_body_json(&app, request).await;
        assert_eq!(config.url.as_str(), "http://example.com/collector-endpoint");

        Ok(())
    }

    #[actix_web::test]
    async fn deregister_collector() -> Result<(), anyhow::Error> {
        let state = SharedState::new(AppState::new("http://csub.example.com/".into()).await?);

        let app = test::init_service(App::new().app_data(web::Data::new(state.clone())).configure(config)).await;

        let request = TestRequest::post()
            .uri("/api/v1/collector/foo")
            .set_json(json!(
                {
                    "url": "http://example.com/collector-endpoint"
                }
            ))
            .to_request();

        test::call_service(&app, request).await;

        {
            let collectors = state.collectors.read().await;

            if let Some(config) = collectors.collector_config("foo".into()) {
                assert_eq!(config.url.as_str(), "http://example.com/collector-endpoint");
            } else {
                panic!("no configuration for `foo`");
            }
        }

        let request = TestRequest::delete().uri("/api/v1/collector/foo").to_request();

        let response = test::call_service(&app, request).await;

        assert_eq!(StatusCode::OK, response.status());

        {
            let collectors = state.collectors.read().await;
            if collectors.collector_config("foo".into()).is_some() {
                panic!("collector not deregistered")
            }
        }

        let request = TestRequest::delete().uri("/api/v1/collector/foo").to_request();

        let response = test::call_service(&app, request).await;

        assert_eq!(StatusCode::NOT_FOUND, response.status());

        Ok(())
    }
}
