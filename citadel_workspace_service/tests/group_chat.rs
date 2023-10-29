mod common;

#[cfg(test)]
mod tests {
    use std::error::Error;

    #[tokio::test]
    async fn test_citadel_workspace_service_group_create() -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}
