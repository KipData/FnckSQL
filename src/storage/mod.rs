#[async_trait::async_trait]
pub trait Storage {
    async fn read_data(&self);
}
