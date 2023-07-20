use std::collections::HashMap;

use url::Url;

use crate::fivetran_sdk::{
    form_field::Type,
    FormField,
    TextField,
};

const CONFIG_KEY_DEPLOYMENT_URL: &str = "url";
const CONFIG_KEY_DEPLOYMENT_KEY: &str = "key";

pub struct Config {
    pub deploy_url: Url,
    pub deploy_key: String,
}

impl Config {
    pub fn fivetran_fields() -> Vec<FormField> {
        vec![
            FormField {
                name: CONFIG_KEY_DEPLOYMENT_URL.to_string(),
                label: "Deployment URL".to_string(),
                required: true,
                r#type: Some(Type::TextField(TextField::PlainText as i32)),
            },
            FormField {
                name: CONFIG_KEY_DEPLOYMENT_KEY.to_string(),
                label: "Deploy Key".to_string(),
                required: true,
                r#type: Some(Type::TextField(TextField::Password as i32)),
            },
        ]
    }

    pub fn from_parameters(configuration: HashMap<String, String>) -> anyhow::Result<Self> {
        let Some(deploy_url) = configuration.get(CONFIG_KEY_DEPLOYMENT_URL) else {
            anyhow::bail!("Missing {CONFIG_KEY_DEPLOYMENT_URL}");
        };

        let Ok(deploy_url) = Url::parse(deploy_url) else {
            anyhow::bail!("Invalid {CONFIG_KEY_DEPLOYMENT_URL} (must be an URL)");
        };

        // TODO(Nicolas) CX-4232 Verify the domain in prod environments

        let Some(deploy_key) = configuration.get(CONFIG_KEY_DEPLOYMENT_KEY) else {
            anyhow::bail!("Missing {CONFIG_KEY_DEPLOYMENT_KEY}");
        };

        Ok(Config {
            deploy_url,
            deploy_key: deploy_key.to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use maplit::hashmap;

    use super::*;

    #[test]
    fn accepts_valid_parameters() {
        let api = Config::from_parameters(hashmap! {
            "url".to_string() => "https://aware-llama-900.convex.cloud".to_string(),
            "key".to_string() => "prod:aware-llama-900|016b26d3900d5e482f1780969c2fa608a773140fb221db21785a9b2775b50263da6a258301b6374ef72b4c120e237c20ac50".to_string(),
        }).unwrap();

        assert_eq!(
            api.deploy_url.to_string(),
            "https://aware-llama-900.convex.cloud/"
        );
        assert_eq!(api.deploy_key, "prod:aware-llama-900|016b26d3900d5e482f1780969c2fa608a773140fb221db21785a9b2775b50263da6a258301b6374ef72b4c120e237c20ac50");
    }

    #[test]
    fn refuses_missing_deploy_url() {
        assert!(
            Config::from_parameters(hashmap! {
                "key".to_string() => "prod:aware-llama-900|016b26d3900d5e482f1780969c2fa608a773140fb221db21785a9b2775b50263da6a258301b6374ef72b4c120e237c20ac50".to_string(),
            }).is_err()
        );
    }

    #[test]
    fn refuses_missing_deploy_key() {
        assert!(Config::from_parameters(hashmap! {
            "url".to_string() => "https://aware-llama-900.convex.cloud".to_string(),
        })
        .is_err());
    }

    #[test]
    fn refuses_invalid_urls() {
        assert!(Config::from_parameters(hashmap! {
            "url".to_string() => "aware lalama convex".to_string(),
            "key".to_string() => "prod:aware-llama-900|016b26d3900d5e482f1780969c2fa608a773140fb221db21785a9b2775b50263da6a258301b6374ef72b4c120e237c20ac50".to_string(),
        })
        .is_err());
    }
}
