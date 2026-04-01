fn main() {
    let _ = sasql::query!(
        "SELECT id FROM nonexistent_table_xyz WHERE id = 1"
    );
}
