use crate::models::{AccountIdentifier, Currency, FungibleTokenEvent};
use near_async::messaging::CanSendAsync;
use near_async::multithread::MultithreadRuntimeHandle;
use near_client::ViewClientActor;
use near_primitives::{
    types::BlockId,
    views::{ExecutionOutcomeWithIdView, ExecutionStatusView},
};
use std::{collections::HashMap, str::FromStr};
pub(crate) fn collect_nep141_events(
    receipt_execution_outcomes: &Vec<ExecutionOutcomeWithIdView>,
    block_header: &near_primitives::views::BlockHeaderView,
    currencies: &Option<Vec<Currency>>,
) -> crate::errors::Result<Vec<FungibleTokenEvent>> {
    let mut res = Vec::new();
    for outcome in receipt_execution_outcomes {
        // Logs are kept when a receipt's state is rolled back, so an outcome
        // that did not commit can still carry an EVENT_JSON log. Only committed
        // outcomes produce events. SuccessReceiptId counts as committed because
        // the receipt's state is written before the receipt it spawned runs.
        match outcome.outcome.status {
            ExecutionStatusView::SuccessValue(_) | ExecutionStatusView::SuccessReceiptId(_) => {}
            ExecutionStatusView::Failure(_) | ExecutionStatusView::Unknown => continue,
        }
        let events = extract_events(outcome);
        for event in events {
            res.extend(compose_rosetta_nep141_events(&event, outcome, block_header, currencies)?);
        }
    }
    Ok(res)
}

fn compose_rosetta_nep141_events(
    events: &crate::models::Nep141Event,
    outcome: &ExecutionOutcomeWithIdView,
    block_header: &near_primitives::views::BlockHeaderView,
    currencies: &Option<Vec<Currency>>,
) -> crate::errors::Result<Vec<FungibleTokenEvent>> {
    let mut ft_events = Vec::new();
    match &events.event_kind {
        crate::models::Nep141EventKind::FtTransfer(transfer_events) => {
            if let Some(currencies) = currencies {
                let currency_map: std::collections::HashMap<String, Currency> =
                    currencies.clone().into_iter().collect::<HashMap<String, Currency>>();
                for transfer_event in transfer_events {
                    if let Some(currency) =
                        currency_map.get(&outcome.outcome.executor_id.to_string())
                    {
                        let base = get_base(Event::Nep141, outcome, block_header)?;
                        let custom = crate::models::FtEvent {
                            affected_id: AccountIdentifier::from_str(&transfer_event.old_owner_id)?,
                            involved_id: Some(AccountIdentifier::from_str(
                                &transfer_event.new_owner_id,
                            )?),
                            delta: crate::utils::SignedDiff::cmp(
                                transfer_event.amount.parse::<u128>()?,
                                0,
                            ),
                            cause: "TRANSFER".to_string(),
                            memo: transfer_event
                                .memo
                                .as_ref()
                                .map(|s| s.escape_default().to_string()),
                            symbol: currency.symbol.clone(),
                            decimals: currency.decimals,
                        };
                        ft_events.push(build_event(base, custom)?);

                        let base = get_base(Event::Nep141, outcome, block_header)?;
                        let custom = crate::models::FtEvent {
                            affected_id: AccountIdentifier::from_str(&transfer_event.new_owner_id)?,
                            involved_id: Some(AccountIdentifier::from_str(
                                &transfer_event.old_owner_id,
                            )?),
                            delta: crate::utils::SignedDiff::from(
                                transfer_event.amount.parse::<u128>()?,
                            ),
                            cause: "TRANSFER".to_string(),
                            memo: transfer_event
                                .memo
                                .as_ref()
                                .map(|s| s.escape_default().to_string()),
                            symbol: currency.symbol.clone(),
                            decimals: currency.decimals,
                        };
                        ft_events.push(build_event(base, custom)?);
                    }
                }
            }
        }
    }
    Ok(ft_events)
}

pub(crate) async fn get_fungible_token_balance_for_account(
    view_client_addr: &MultithreadRuntimeHandle<ViewClientActor>,
    block_header: &near_primitives::views::BlockHeaderView,
    contract_address: &String,
    account_identifier: &AccountIdentifier,
) -> crate::errors::Result<u128> {
    let method_name = "ft_balance_of".to_string();
    let account_id_for_args = account_identifier.clone().address.to_string();
    let args = serde_json::json!({
        "account_id":account_id_for_args,
    })
    .to_string()
    .into_bytes();
    let block_reference =
        near_primitives::types::BlockReference::BlockId(BlockId::Hash(block_header.hash));
    let request = near_primitives::views::QueryRequest::CallFunction {
        account_id: near_account_id::AccountId::from_str(contract_address)?,
        method_name,
        args: args.into(),
    };
    let query_response = view_client_addr
        .send_async(near_client::Query { block_reference, request })
        .await?
        .map_err(|e| crate::errors::ErrorKind::InternalInvariantError(e.to_string()))?;
    let call_result = if let near_primitives::views::QueryResponseKind::CallResult(result) =
        query_response.kind
    {
        result.result
    } else {
        return Err(crate::errors::ErrorKind::InternalInvariantError(format!(
            "Couldn't retrieve ft_balance of {:?} on address {:?}",
            account_identifier.address.clone(),
            contract_address.clone(),
        )));
    };
    let serde_call_result = serde_json::from_slice(&call_result).or_else(|_| {
        Err(crate::errors::ErrorKind::InternalInvariantError(format!(
            "Couldn't read the value from the contract {:?}, for the account {:?}",
            contract_address.clone(),
            account_identifier.address.clone(),
        )))
    })?;
    let amount: String = match serde_json::from_value(serde_call_result) {
        Ok(amount) => amount,
        Err(err) => return Err(err.into()),
    };
    let amount = amount.parse::<u128>()?;
    Ok(amount)
}

pub(crate) fn extract_events(
    execution_outcome: &ExecutionOutcomeWithIdView,
) -> Vec<crate::models::Nep141Event> {
    let prefix = "EVENT_JSON:";
    execution_outcome
        .outcome
        .logs
        .iter()
        .filter_map(|untrimmed_log| {
            let log = untrimmed_log.trim();
            if !log.starts_with(prefix) {
                return None;
            }

            match serde_json::from_str::<'_, crate::models::Nep141Event>(log[prefix.len()..].trim())
            {
                Ok(result) => Some(result),
                Err(_err) => None,
            }
        })
        .collect()
}
pub(crate) fn get_base(
    event_type: Event,
    outcome: &ExecutionOutcomeWithIdView,
    block_header: &near_primitives::views::BlockHeaderView,
) -> crate::errors::Result<crate::models::EventBase> {
    Ok(crate::models::EventBase {
        standard: get_standard(&event_type),
        receipt_id: outcome.id,
        block_height: block_header.height,
        block_timestamp: block_header.timestamp,
        contract_account_id: outcome.outcome.executor_id.clone().into(),
        status: outcome.outcome.status.clone(),
    })
}

pub(crate) enum Event {
    Nep141,
}
fn get_standard(event_type: &Event) -> String {
    match event_type {
        Event::Nep141 => FT,
    }
    .to_string()
}
pub const FT: &str = "FT_NEP141";

fn build_event(
    base: crate::models::EventBase,
    custom: crate::models::FtEvent,
) -> crate::errors::Result<FungibleTokenEvent> {
    Ok(FungibleTokenEvent {
        standard: base.standard,
        receipt_id: base.receipt_id,
        block_height: base.block_height,
        block_timestamp: base.block_timestamp,
        contract_account_id: base.contract_account_id.address.to_string(),
        symbol: custom.symbol,
        decimals: custom.decimals,
        affected_account_id: custom.affected_id.address.to_string(),
        involved_account_id: custom.involved_id.map(|id| id.address.to_string()),
        delta_amount: custom.delta,
        cause: custom.cause,
        status: get_status(&base.status),
        event_memo: custom.memo,
    })
}

fn get_status(status: &near_primitives::views::ExecutionStatusView) -> String {
    match status {
        near_primitives::views::ExecutionStatusView::Unknown => "UNKNOWN",
        near_primitives::views::ExecutionStatusView::Failure(_) => "FAILURE",
        near_primitives::views::ExecutionStatusView::SuccessValue(_) => "SUCCESS",
        near_primitives::views::ExecutionStatusView::SuccessReceiptId(_) => "SUCCESS",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::CurrencyMetadata;
    use crate::utils::SignedDiff;
    use near_primitives::errors::{
        ActionError, ActionErrorKind, FunctionCallError, MethodResolveError,
    };
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::{Balance, Gas};
    use near_primitives::views::{BlockHeaderView, ExecutionMetadataView, ExecutionOutcomeView};

    const FT_CONTRACT: &str = "ft.near";

    fn ft_transfer_log(sender: &str, receiver: &str, amount: u128) -> String {
        format!(
            r#"EVENT_JSON:{{"standard":"nep141","version":"1.0.0","event":"ft_transfer","data":[{{"old_owner_id":"{sender}","new_owner_id":"{receiver}","amount":"{amount}"}}]}}"#
        )
    }

    fn outcome(
        id: CryptoHash,
        executor: &str,
        status: ExecutionStatusView,
    ) -> ExecutionOutcomeWithIdView {
        ExecutionOutcomeWithIdView {
            proof: vec![],
            block_hash: CryptoHash::default(),
            id,
            outcome: ExecutionOutcomeView {
                logs: vec![ft_transfer_log("bob.near", "alice.near", 10)],
                receipt_ids: vec![],
                gas_burnt: Gas::ZERO,
                tokens_burnt: Balance::ZERO,
                executor_id: executor.parse().unwrap(),
                status,
                metadata: ExecutionMetadataView::default(),
            },
        }
    }

    fn ft_currencies() -> Option<Vec<Currency>> {
        Some(vec![Currency {
            symbol: "FT".to_string(),
            decimals: 18,
            metadata: Some(CurrencyMetadata { contract_address: FT_CONTRACT.to_string() }),
        }])
    }

    fn failure() -> ExecutionStatusView {
        ExecutionStatusView::Failure(
            ActionError {
                index: Some(1),
                kind: ActionErrorKind::FunctionCallError(FunctionCallError::MethodResolveError(
                    MethodResolveError::MethodNotFound,
                )),
            }
            .into(),
        )
    }

    fn collect(
        outcomes: Vec<ExecutionOutcomeWithIdView>,
        currencies: &Option<Vec<Currency>>,
    ) -> Vec<FungibleTokenEvent> {
        collect_nep141_events(&outcomes, &BlockHeaderView::default(), currencies).unwrap()
    }

    #[test]
    fn committed_statuses_produce_debit_and_credit() {
        for status in [
            ExecutionStatusView::SuccessValue(vec![]),
            ExecutionStatusView::SuccessReceiptId(CryptoHash::default()),
        ] {
            let id = CryptoHash::hash_bytes(b"receipt");
            let events = collect(vec![outcome(id, FT_CONTRACT, status.clone())], &ft_currencies());

            assert_eq!(events.len(), 2, "status {status:?}");
            for event in &events {
                assert_eq!(event.receipt_id, id);
                assert_eq!(event.contract_account_id, FT_CONTRACT);
                assert_eq!(event.symbol, "FT");
                assert_eq!(event.decimals, 18);
                assert_eq!(event.cause, "TRANSFER");
            }

            let debit = &events[0];
            assert_eq!(debit.affected_account_id, "bob.near");
            assert_eq!(debit.involved_account_id.as_deref(), Some("alice.near"));
            assert_eq!(debit.delta_amount, SignedDiff::cmp(10, 0));

            let credit = &events[1];
            assert_eq!(credit.affected_account_id, "alice.near");
            assert_eq!(credit.involved_account_id.as_deref(), Some("bob.near"));
            assert_eq!(credit.delta_amount, SignedDiff::from(10u128));
        }
    }

    #[test]
    fn rolled_back_statuses_produce_no_events() {
        for status in [failure(), ExecutionStatusView::Unknown] {
            let outcomes = vec![outcome(CryptoHash::hash_bytes(b"receipt"), FT_CONTRACT, status)];
            assert_eq!(collect(outcomes, &ft_currencies()), vec![]);
        }
    }

    #[test]
    fn filter_is_per_outcome_not_per_block() {
        let failed_id = CryptoHash::hash_bytes(b"failed");
        let succeeded_id = CryptoHash::hash_bytes(b"succeeded");
        let unknown_id = CryptoHash::hash_bytes(b"unknown");
        let events = collect(
            vec![
                outcome(failed_id, FT_CONTRACT, failure()),
                outcome(succeeded_id, FT_CONTRACT, ExecutionStatusView::SuccessValue(vec![])),
                outcome(unknown_id, FT_CONTRACT, ExecutionStatusView::Unknown),
            ],
            &ft_currencies(),
        );

        assert_eq!(events.len(), 2);
        for event in &events {
            assert_eq!(event.receipt_id, succeeded_id);
        }
    }

    #[test]
    fn no_currencies_configured_produces_no_events() {
        let outcomes = vec![outcome(
            CryptoHash::hash_bytes(b"receipt"),
            FT_CONTRACT,
            ExecutionStatusView::SuccessValue(vec![]),
        )];
        assert_eq!(collect(outcomes, &None), vec![]);
    }

    #[test]
    fn contract_not_in_currencies_produces_no_events() {
        let outcomes = vec![outcome(
            CryptoHash::hash_bytes(b"receipt"),
            "other.near",
            ExecutionStatusView::SuccessValue(vec![]),
        )];
        assert_eq!(collect(outcomes, &ft_currencies()), vec![]);
    }
}
