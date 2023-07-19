use actix_web::{delete, get, post, web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};

use crate::SharedState;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CollectRequest {
    pub(crate) purls: Vec<String>,
}

/// Register a collector
#[utoipa::path(
    post,
    tag = "collectorist",
    path = "/api/v1/collect",
    responses(
        (status = 200, description = "Collector registered"),
        (status = BAD_REQUEST, description = "Missing valid id"),
    ),
)]
#[post("/collect")]
pub(crate) async fn collect(
    state: web::Data<SharedState>,
    req: web::Json<CollectRequest>,
) -> actix_web::Result<impl Responder> {
    Ok(HttpResponse::Ok().finish())
}
