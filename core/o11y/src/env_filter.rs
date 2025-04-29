use std::borrow::Cow;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::ParseError;

/// The default value for the `RUST_LOG` environment variable if one isn't specified otherwise.
const DEFAULT_RUST_LOG: &str = "tokio_reactor=info,\
     config=info,\
     near=info,\
     stats=info,\
     telemetry=info,\
     db=info,\
     delay_detector=info,\
     near-performance-metrics=info,\
     state_viewer=info,\
     warn";

#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum BuildEnvFilterError {
    #[error("could not create a log filter for {1}")]
    CreateEnvFilter(#[source] ParseError, String),
}

#[derive(Debug)]
pub struct EnvFilterBuilder<'a> {
    rust_log: Cow<'a, str>,
    verbose: Option<&'a str>,
}

impl<'a> EnvFilterBuilder<'a> {
    /// Create the `EnvFilter` from the environment variable or the [`DEFAULT_RUST_LOG`] value if
    /// the environment is not set.
    pub fn from_env() -> Self {
        Self::new(
            std::env::var("RUST_LOG").map(Cow::Owned).unwrap_or(Cow::Borrowed(DEFAULT_RUST_LOG)),
        )
    }

    /// Specify an exact `RUST_LOG` value to use.
    ///
    /// This method will not inspect the environment variable.
    pub fn new<S: Into<Cow<'a, str>>>(rust_log: S) -> Self {
        Self { rust_log: rust_log.into(), verbose: None }
    }

    /// Make the produced [`EnvFilter`] verbose.
    ///
    /// If the `module` string is empty, all targets will log debug output. Otherwise only the
    /// specified target will log the debug output.
    pub fn verbose(mut self, target: Option<&'a str>) -> Self {
        self.verbose = target;
        self
    }

    /// Construct an [`EnvFilter`] as configured.
    pub fn finish(self) -> Result<EnvFilter, BuildEnvFilterError> {
        let mut env_filter = EnvFilter::try_new(self.rust_log.clone())
            .map_err(|err| BuildEnvFilterError::CreateEnvFilter(err, self.rust_log.to_string()))?;
        if let Some(module) = self.verbose {
            env_filter = env_filter
                .add_directive("cranelift_codegen=warn".parse().expect("parse directive"))
                .add_directive("h2=warn".parse().expect("parse directive"))
                .add_directive("tower=warn".parse().expect("parse directive"))
                .add_directive("trust_dns_resolver=warn".parse().expect("parse directive"))
                .add_directive("trust_dns_proto=warn".parse().expect("parse directive"));
            env_filter = if module.is_empty() {
                env_filter.add_directive(tracing::Level::DEBUG.into())
            } else {
                let directive = format!("{}=debug", module).parse().map_err(|err| {
                    BuildEnvFilterError::CreateEnvFilter(err, format!("{}=debug", module))
                })?;
                env_filter.add_directive(directive)
            };
        }
        Ok(env_filter)
    }
}

pub fn make_env_filter(verbose: Option<&str>) -> Result<EnvFilter, BuildEnvFilterError> {
    let env_filter = EnvFilterBuilder::from_env().verbose(verbose).finish()?;
    // Sandbox node can log to sandbox logging target via sandbox_debug_log host function.
    // This is hidden by default so we enable it for sandbox node.
    let env_filter = if cfg!(feature = "sandbox") {
        env_filter.add_directive("sandbox=debug".parse().unwrap())
    } else {
        env_filter
    };
    Ok(env_filter)
}
