//! The HTTP API: the endpoints, their authentication and their answers.

mod common;

use common::{ADMIN_KEY, KEY, TestServer};
use serde_json::{Value, json};

async fn body(r: reqwest::Response) -> (u16, Value) {
    let status = r.status().as_u16();
    let text = r.text().await.unwrap();
    (
        status,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn the_version_has_the_keys_clients_decode() {
    let s = TestServer::start().await;
    let (code, v) = body(s.http.get(s.url("/version")).send().await.unwrap()).await;
    assert_eq!(code, 200);
    for k in [
        "buildDate",
        "buildVersion",
        "buildHash",
        "goVersion",
        "goOS",
        "goArch",
    ] {
        assert!(v.get(k).is_some(), "{k} in {v}");
    }
    // Anything but GET is not found, as in the Go service.
    assert_eq!(
        s.http
            .post(s.url("/version"))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_system_info_carries_the_load_of_the_instance() {
    let s = TestServer::start().await;
    let (code, v) = body(s.http.get(s.url("/system")).send().await.unwrap()).await;
    assert_eq!(code, 200);
    // `cpu_load` is the Go service's key; a client decoding only that one is unaffected by the rest.
    assert!(v["cpu_load"].is_number(), "{v}");
    assert_eq!(v["sessions"], 0, "{v}");
    assert!(v["shards"].as_u64().unwrap() >= 1, "{v}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_registers_logs_in_and_unregisters_itself() {
    let s = TestServer::start().await;
    let (code, v) = body(
        s.http
            .post(s.url("/register"))
            .json(&json!({ "clientID": "mm", "authKey": KEY }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        (code, v["clientID"].as_str(), v["code"].as_str()),
        (201, Some("mm"), Some("201"))
    );

    let (code, v) = body(
        s.http
            .post(s.url("/register"))
            .json(&json!({ "clientID": "mm", "authKey": KEY }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        (code, v["error"].as_str()),
        (400, Some("registration failed: already registered"))
    );
    let (code, v) = body(
        s.http
            .post(s.url("/register"))
            .json(&json!({ "clientID": "x", "authKey": "short" }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        (code, v["error"].as_str()),
        (400, Some("registration failed: key not long enough"))
    );

    let (code, v) = body(
        s.http
            .post(s.url("/login"))
            .json(&json!({ "clientID": "mm", "authKey": KEY }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(code, 200);
    let token = v["bearerToken"].as_str().unwrap().to_owned();
    assert_eq!(token.len(), 32);
    let (code, v) = body(
        s.http
            .post(s.url("/login"))
            .json(&json!({ "clientID": "mm", "authKey": "wrong-key-wrong-key-wrong-key-xx" }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(code, 400);
    assert!(v["error"].as_str().unwrap().starts_with("login failed"));

    let r = s
        .http
        .post(s.url("/unregister"))
        .header("Authorization", TestServer::basic("mm", KEY))
        .json(&json!({ "clientID": "mm" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = s
        .http
        .post(s.url("/login"))
        .json(&json!({ "clientID": "mm", "authKey": KEY }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400, "it is gone");
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_bodies_and_wrong_methods_are_refused() {
    let s = TestServer::start().await;
    let r = s
        .http
        .post(s.url("/register"))
        .body("not json")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let r = s
        .http
        .post(s.url("/login"))
        .body("[1,2]")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    for path in ["/register", "/login", "/unregister"] {
        assert_eq!(
            s.http.get(s.url(path)).send().await.unwrap().status(),
            404,
            "{path}"
        );
    }
    assert_eq!(
        s.http.get(s.url("/nowhere")).send().await.unwrap().status(),
        404
    );
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn registering_needs_authority_when_self_registration_is_off() {
    let s = TestServer::start_with(|c| {
        c.api.security.allow_self_registration = false;
        c.api.security.enable_admin = true;
        c.api.security.admin_secret_key = ADMIN_KEY.into();
    })
    .await;
    let reg = json!({ "clientID": "mm", "authKey": KEY });
    let r = s
        .http
        .post(s.url("/register"))
        .json(&reg)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401, "no credentials");
    let r = s
        .http
        .post(s.url("/register"))
        .header("Authorization", TestServer::basic("", "wrong"))
        .json(&reg)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401, "wrong admin key");
    let r = s
        .http
        .post(s.url("/register"))
        .header("Authorization", TestServer::basic("", ADMIN_KEY))
        .json(&reg)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201, "the administrator may register");
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unregistering_has_the_rules_of_the_go_service() {
    // Neither admin nor self registration: forbidden outright.
    let s = TestServer::start_with(|c| c.api.security.allow_self_registration = false).await;
    let r = s
        .http
        .post(s.url("/unregister"))
        .json(&json!({ "clientID": "mm" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    s.shutdown().await;

    let s = TestServer::start_with(|c| {
        c.api.security.enable_admin = true;
        c.api.security.admin_secret_key = ADMIN_KEY.into();
    })
    .await;
    s.register("a").await;
    s.register("b").await;
    let req = |auth: String, id: &str| {
        s.http
            .post(s.url("/unregister"))
            .header("Authorization", auth)
            .json(&json!({ "clientID": id }))
    };
    assert_eq!(
        req(TestServer::basic("a", KEY), "b")
            .send()
            .await
            .unwrap()
            .status(),
        403,
        "a client cannot remove another"
    );
    assert_eq!(
        req(TestServer::basic("a", KEY), "")
            .send()
            .await
            .unwrap()
            .status(),
        400,
        "an empty id"
    );
    assert_eq!(
        req(TestServer::basic("", ADMIN_KEY), "b")
            .send()
            .await
            .unwrap()
            .status(),
        200,
        "the administrator can"
    );
    assert_eq!(
        req(TestServer::basic("", ADMIN_KEY), "b")
            .send()
            .await
            .unwrap()
            .status(),
        400,
        "twice, no"
    );
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn session_lookups_need_a_client_and_answer_404_for_what_is_not_there() {
    let s = TestServer::start().await;
    s.register("mm").await;
    let ok = TestServer::basic("mm", KEY);
    assert_eq!(
        s.http
            .get(s.url("/calls/c1/sessions"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        s.http
            .get(s.url("/calls/c1/sessions"))
            .header(
                "Authorization",
                TestServer::basic("mm", "wrong-key-wrong-key-wrong-key-xx")
            )
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        s.http
            .get(s.url("/calls/c1/sessions"))
            .header("Authorization", &ok)
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(
        s.http
            .get(s.url("/calls/c1/sessions/s1"))
            .header("Authorization", &ok)
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(
        s.http
            .post(s.url("/calls/c1/sessions"))
            .header("Authorization", &ok)
            .send()
            .await
            .unwrap()
            .status(),
        404,
        "wrong method"
    );

    // A bearer token works as well as basic credentials.
    let (_, v) = body(
        s.http
            .post(s.url("/login"))
            .json(&json!({ "clientID": "mm", "authKey": KEY }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    let bearer = format!("Bearer {}", v["bearerToken"].as_str().unwrap());
    assert_eq!(
        s.http
            .get(s.url("/calls/c1/sessions"))
            .header("Authorization", bearer)
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_metrics_endpoint_serves_prometheus_text() {
    let s = TestServer::start().await;
    let r = s.http.get(s.url("/metrics")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert!(
        r.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/plain")
    );
    s.shutdown().await;
}
