//! Firecrawl API v2 client.

use reqwest::Response;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::{FirecrawlAPIError, FirecrawlError};

pub(crate) const API_VERSION: &str = "/v2";
const CLOUD_API_URL: &str = "https://api.firecrawl.dev";

/// Firecrawl API v2 client.
///
/// This client provides access to all v2 API endpoints including scrape, crawl,
/// search, map, batch scrape, and agent operations.
///
/// # Example
///
/// ```no_run
/// use firecrawl::v2::Client;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Create a client for the Firecrawl cloud service
///     let client = Client::new("your-api-key")?;
///
///     // Or create a client for a self-hosted instance
///     let client = Client::new_selfhosted("http://localhost:3000", Some("api-key"))?;
///
///     Ok(())
/// }
/// ```
#[derive(Clone, Debug)]
pub struct Client {
    pub(crate) api_key: Option<String>,
    pub(crate) api_url: String,
    pub(crate) client: reqwest::Client,
}

impl Client {
    /// Creates a new client for the Firecrawl cloud service.
    ///
    /// # Arguments
    ///
    /// * `api_key` - Your Firecrawl API key.
    ///
    /// # Errors
    ///
    /// Returns an error if the API key is empty.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use firecrawl::v2::Client;
    ///
    /// let client = Client::new("your-api-key").unwrap();
    /// ```
    pub fn new(api_key: impl AsRef<str>) -> Result<Self, FirecrawlError> {
        Client::new_selfhosted(CLOUD_API_URL, Some(api_key))
    }

    /// Creates a new client for a self-hosted Firecrawl instance.
    ///
    /// # Arguments
    ///
    /// * `api_url` - The base URL of your Firecrawl instance.
    /// * `api_key` - Optional API key (required for cloud, optional for self-hosted).
    ///
    /// # Errors
    ///
    /// Returns an error if using the cloud service without an API key.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use firecrawl::v2::Client;
    ///
    /// // Self-hosted without authentication
    /// let client = Client::new_selfhosted("http://localhost:3000", None::<&str>).unwrap();
    ///
    /// // Self-hosted with authentication
    /// let client = Client::new_selfhosted("http://localhost:3000", Some("api-key")).unwrap();
    /// ```
    pub fn new_selfhosted(
        api_url: impl AsRef<str>,
        api_key: Option<impl AsRef<str>>,
    ) -> Result<Self, FirecrawlError> {
        let url = api_url.as_ref().to_string();

        if url == CLOUD_API_URL && api_key.is_none() {
            return Err(FirecrawlError::APIError(
                "Configuration".to_string(),
                FirecrawlAPIError {
                    success: false,
                    error: "API key is required for cloud service".to_string(),
                    details: None,
                },
            ));
        }

        Ok(Client {
            api_key: api_key.map(|x| x.as_ref().to_string()),
            api_url: url,
            client: reqwest::Client::new(),
        })
    }

    /// Prepares headers for API requests.
    pub(crate) fn prepare_headers(
        &self,
        idempotency_key: Option<&String>,
    ) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Content-Type", "application/json".parse().unwrap());
        if let Some(api_key) = self.api_key.as_ref() {
            headers.insert(
                "Authorization",
                format!("Bearer {}", api_key).parse().unwrap(),
            );
        }
        if let Some(key) = idempotency_key {
            headers.insert("x-idempotency-key", key.parse().unwrap());
        }
        headers
    }

    /// Handles API responses, parsing JSON and handling errors.
    pub(crate) async fn handle_response<T: DeserializeOwned>(
        &self,
        response: Response,
        action: impl AsRef<str>,
    ) -> Result<T, FirecrawlError> {
        let (is_success, status) = (response.status().is_success(), response.status());

        let response = response
            .text()
            .await
            .map_err(FirecrawlError::ResponseParseErrorText)
            .and_then(|response_json| {
                serde_json::from_str::<Value>(&response_json)
                    .map_err(FirecrawlError::ResponseParseError)
                    .inspect(|data| {
                        tracing::debug!("Response JSON: {:#?}", data);
                    })
            })
            .and_then(|response_value| {
                // Check for success field, or allow responses without it for status checks
                if action.as_ref().contains("status")
                    || action.as_ref().contains("cancel")
                    || response_value["success"].as_bool().unwrap_or(false)
                    || response_value.get("success").is_none()
                {
                    serde_json::from_value::<T>(response_value)
                        .map_err(FirecrawlError::ResponseParseError)
                } else {
                    Err(FirecrawlError::APIError(
                        action.as_ref().to_string(),
                        serde_json::from_value(response_value)
                            .map_err(FirecrawlError::ResponseParseError)?,
                    ))
                }
            });

        match &response {
            Ok(_) => response,
            Err(FirecrawlError::ResponseParseError(_))
            | Err(FirecrawlError::ResponseParseErrorText(_)) => {
                if is_success {
                    response
                } else {
                    Err(FirecrawlError::HttpRequestFailed(
                        action.as_ref().to_string(),
                        status.as_u16(),
                        status.as_str().to_string(),
                    ))
                }
            }
            Err(_) => response,
        }
    }

    /// Builds the full URL for an API endpoint.
    pub(crate) fn url(&self, path: &str) -> String {
        format!("{}{}{}", self.api_url, API_VERSION, path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_client() {
        let client = Client::new("test-api-key").unwrap();
        assert_eq!(client.api_key, Some("test-api-key".to_string()));
        assert_eq!(client.api_url, CLOUD_API_URL);
    }

    #[test]
    fn test_new_client_requires_api_key_for_cloud() {
        let result = Client::new_selfhosted(CLOUD_API_URL, None::<&str>);
        assert!(result.is_err());
    }

    #[test]
    fn test_new_selfhosted_client() {
        let client = Client::new_selfhosted("http://localhost:3000", Some("api-key")).unwrap();
        assert_eq!(client.api_key, Some("api-key".to_string()));
        assert_eq!(client.api_url, "http://localhost:3000");
    }

    #[test]
    fn test_selfhosted_without_api_key() {
        let client = Client::new_selfhosted("http://localhost:3000", None::<&str>).unwrap();
        assert_eq!(client.api_key, None);
        assert_eq!(client.api_url, "http://localhost:3000");
    }

    #[test]
    fn test_url_builder() {
        let client = Client::new("test-key").unwrap();
        assert_eq!(
            client.url("/scrape"),
            "https://api.firecrawl.dev/v2/scrape"
        );
    }
}
