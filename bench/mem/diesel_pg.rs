//! Memory benchmark for diesel (PostgreSQL)
//! Run: BENCH_DATABASE_URL=... /usr/bin/time -l cargo run --release --bin mem_diesel_pg

use diesel::prelude::*;
use diesel::sql_types::{Integer, Text};

#[derive(QueryableByName, Debug)]
#[allow(dead_code)]
struct User {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Text)]
    email: String,
}

fn main() {
    let url = std::env::var("BENCH_DATABASE_URL").expect("BENCH_DATABASE_URL");
    let mut conn = PgConnection::establish(&url).unwrap();

    // 10K SELECT queries
    for i in 0..10_000 {
        let id = (i % 10000 + 1) as i32;
        let _rows = diesel::sql_query("SELECT id, name, email FROM bench_users WHERE id = $1")
            .bind::<Integer, _>(id)
            .load::<User>(&mut conn)
            .unwrap();
    }

    // 1K INSERT queries
    for i in 0..1_000 {
        let name = format!("memtest_{i}");
        let email = format!("mem{i}@test.com");
        diesel::sql_query(
            "INSERT INTO bench_users (name, email, active, score) VALUES ($1, $2, true, 0.0)",
        )
        .bind::<Text, _>(&name)
        .bind::<Text, _>(&email)
        .execute(&mut conn)
        .unwrap();
    }
}
