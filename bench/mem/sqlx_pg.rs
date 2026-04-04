//! Memory benchmark for sqlx (PostgreSQL)
//! Run: BENCH_DATABASE_URL=... /usr/bin/time -l cargo run --release --bin mem_sqlx_pg

use sqlx::PgPool;

#[tokio::main]
async fn main() {
    let url = std::env::var("BENCH_DATABASE_URL").expect("BENCH_DATABASE_URL");
    let pool = PgPool::connect(&url).await.unwrap();

    // 10K SELECT queries
    for i in 0..10_000 {
        let id = (i % 10000 + 1) as i32;
        let _row: (i32, String, String) =
            sqlx::query_as("SELECT id, name, email FROM bench_users WHERE id = $1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap();
    }

    // 1K INSERT queries
    for i in 0..1_000 {
        let name = format!("memtest_{i}");
        let email = format!("mem{i}@test.com");
        sqlx::query(
            "INSERT INTO bench_users (name, email, active, score) VALUES ($1, $2, true, 0.0)",
        )
        .bind(&name)
        .bind(&email)
        .execute(&pool)
        .await
        .unwrap();
    }
}
