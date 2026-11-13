//! xAI Responses provider routing and its bundled model catalog.

use std::path::PathBuf;
use std::sync::Arc;

use codex_login::AuthManager;
use codex_login::CodexAuth;
use codex_model_provider_info::ModelProviderInfo;
use codex_models_manager::manager::SharedModelsManager;
use codex_models_manager::manager::StaticModelsManager;
use codex_protocol::openai_models::ModelsResponse;

use crate::ProviderCapabilities;
use crate::RemoteCompactionSupport;
use crate::provider::ModelProvider;
use crate::provider::ModelProviderFuture;
use crate::provider::ProviderAccountResult;
use crate::provider::ProviderAccountState;

const XAI_DEFAULT_MODEL: &str = "grok-4.7";

/// Runtime provider for xAI's OpenAI-compatible Responses API.
#[derive(Clone, Debug)]
pub(crate) struct XaiModelProvider {
    info: ModelProviderInfo,
    auth_manager: Option<Arc<AuthManager>>,
}

impl XaiModelProvider {
    pub(crate) fn new(
        provider_info: ModelProviderInfo,
        auth_manager: Option<Arc<AuthManager>>,
    ) -> Self {
        Self {
            info: provider_info,
            auth_manager,
        }
    }

    async fn auth(&self) -> Option<CodexAuth> {
        match self.auth_manager.as_ref() {
            Some(auth_manager) => auth_manager.auth().await,
            None => None,
        }
    }

    fn default_model_catalog() -> ModelsResponse {
        // xAI's public model endpoint does not expose the metadata Codex needs for its picker.
        codex_models_manager::bundled_xai_models_response()
            .unwrap_or_else(|error| panic!("bundled xAI models.json should parse: {error}"))
    }
}

impl ModelProvider for XaiModelProvider {
    fn info(&self) -> &ModelProviderInfo {
        &self.info
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            // xAI returns encrypted reasoning content instead of OpenAI reasoning summaries.
            reasoning_summaries: false,
            image_generation: false,
            web_search: false,
            external_web_access: false,
            remote_compaction: RemoteCompactionSupport::Unsupported,
        }
    }

    fn approval_review_preferred_model(&self) -> &'static str {
        XAI_DEFAULT_MODEL
    }

    fn memory_extraction_preferred_model(&self) -> &'static str {
        XAI_DEFAULT_MODEL
    }

    fn memory_consolidation_preferred_model(&self) -> &'static str {
        XAI_DEFAULT_MODEL
    }

    fn auth_manager(&self) -> Option<Arc<AuthManager>> {
        self.auth_manager.clone()
    }

    fn auth(&self) -> ModelProviderFuture<'_, Option<CodexAuth>> {
        Box::pin(XaiModelProvider::auth(self))
    }

    fn account_state(&self) -> ProviderAccountResult {
        Ok(ProviderAccountState {
            // xAI API keys are environment-scoped rather than Codex account credentials.
            account: None,
            requires_openai_auth: false,
        })
    }

    fn models_manager(
        &self,
        _codex_home: PathBuf,
        config_model_catalog: Option<ModelsResponse>,
    ) -> SharedModelsManager {
        let origin = if config_model_catalog.is_some() {
            codex_models_manager::ModelOrigin::ExplicitConfig
        } else {
            codex_models_manager::ModelOrigin::Provider
        };
        Arc::new(StaticModelsManager::new(
            self.auth_manager.clone(),
            config_model_catalog.unwrap_or_else(Self::default_model_catalog),
            origin,
        ))
    }

    fn models_manager_without_cache(
        &self,
        config_model_catalog: Option<ModelsResponse>,
    ) -> SharedModelsManager {
        self.models_manager(PathBuf::new(), config_model_catalog)
    }
}
