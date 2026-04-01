fn main() {
    let id = 1i32;
    let _ = sasql::query!(
        "SELECT nonexistent_column_xyz FROM users WHERE id = $id: i32"
    );
}
