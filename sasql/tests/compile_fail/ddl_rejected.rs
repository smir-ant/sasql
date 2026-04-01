fn main() {
    let _ = sasql::query!(
        "CREATE TABLE should_not_work (id int)"
    );
}
