fn main() {
    let _ = sasql::query!(
        "SELECT id FROM users WHERE id = $1"
    );
}
