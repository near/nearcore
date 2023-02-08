use near_config_utils::{ValidationError, ValidationErrors};

use crate::config::Config;

/// Validate Config extracted from config.json. Panics if Config is ill-formed.
pub fn validate_config(config: &Config) {
    let mut validation_errors = ValidationErrors::new();
    let mut config_validator = ConfigValidator::new(config, &mut validation_errors);
    println!("\nValidating Config, extracted from config.json...");
    config_validator.validate();
}

pub fn validate_config_no_panic(config: &Config, validation_errors: &mut ValidationErrors) {
    let mut config_validator = ConfigValidator::new(config, validation_errors);
    println!("\nValidating Config, extracted from config.json...");
    config_validator.validate_no_panic()
}

struct ConfigValidator<'a> {
    config: &'a Config,
    validation_errors: &'a mut ValidationErrors,
}

impl<'a> ConfigValidator<'a> {
    fn new(config: &'a Config, validation_errors: &'a mut ValidationErrors) -> Self {
        Self { config, validation_errors }
    }

    fn validate(&mut self) {
        self.validate_all_conditions();
        self.pass_or_panic_with_full_error();
    }

    fn validate_no_panic(&mut self) {
        self.validate_all_conditions();
    }

    /// this function would check all conditions, and add all error messages to ConfigValidator.errors
    fn validate_all_conditions(&mut self) {
        if self.config.archive == false && self.config.save_trie_changes == Some(false) {
            let error_message = format!("Configuration with archive = false and save_trie_changes = false is not supported because non-archival nodes must save trie changes in order to do do garbage collection.");
            self.validation_errors
                .push_errors(ValidationError::ConfigSemanticsError { error_message: error_message })
        }

        if self.config.consensus.min_block_production_delay
            > self.config.consensus.max_block_production_delay
        {
            let error_message = format!(
                "min_block_production_delay: {:?} is greater than max_block_production_delay: {:?}",
                self.config.consensus.min_block_production_delay,
                self.config.consensus.max_block_production_delay
            );
            self.validation_errors
                .push_errors(ValidationError::ConfigSemanticsError { error_message: error_message })
        }

        if self.config.consensus.min_block_production_delay
            > self.config.consensus.max_block_wait_delay
        {
            let error_message = format!(
                "min_block_production_delay: {:?} is greater than max_block_wait_delay: {:?}",
                self.config.consensus.min_block_production_delay,
                self.config.consensus.max_block_wait_delay
            );
            self.validation_errors
                .push_errors(ValidationError::ConfigSemanticsError { error_message: error_message })
        }

        if self.config.consensus.sync_height_threshold == 0 {
            let error_message = format!("consensus.sync_height_threshold should not be 0");
            self.validation_errors
                .push_errors(ValidationError::ConfigSemanticsError { error_message: error_message })
        }

        if self.config.consensus.header_sync_expected_height_per_second == 0 {
            let error_message =
                format!("consensus.header_sync_expected_height_per_second should not be 0");
            self.validation_errors
                .push_errors(ValidationError::ConfigSemanticsError { error_message: error_message })
        }

        if self.config.consensus.min_num_peers == 0 {
            let error_message = format!("consensus.min_num_peers should not be 0");
            self.validation_errors
                .push_errors(ValidationError::ConfigSemanticsError { error_message: error_message })
        }

        if self.config.tracked_shards.is_empty() {
            let error_message = format!("Tracked_shards should be non-empty.");
            self.validation_errors
                .push_errors(ValidationError::ConfigSemanticsError { error_message: error_message })
        }

        if self.config.gc.gc_blocks_limit == 0
            || self.config.gc.gc_fork_clean_step == 0
            || self.config.gc.gc_num_epochs_to_keep == 0
        {
            let error_message = format!("gc config values should all be greater than 0, but gc_blocks_limit is {:?}, gc_fork_clean_step is {}, gc_num_epochs_to_keep is {}.", self.config.gc.gc_blocks_limit, self.config.gc.gc_fork_clean_step, self.config.gc.gc_num_epochs_to_keep);
            self.validation_errors
                .push_errors(ValidationError::ConfigSemanticsError { error_message: error_message })
        }
    }

    /// this function iterates over all the error_messages and report pass if no error mesasge is found, otherwise panic and print all the error messages in a formatted way
    fn pass_or_panic_with_full_error(&self) {
        self.validation_errors.panic_if_errors()
    }
}
