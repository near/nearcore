use crate::models::{AccountIdentifier, Currency, FungibleTokenEvent};
use near_o11y::WithSpanContextExt;
use near_primitives::{types::BlockId, views::ExecutionOutcomeWithIdView};
use std::{collections::HashMap, str::FromStr};
pub(crate) async fn collect_nep141_events(
    receipt_execution_outcomes: &Vec<ExecutionOutcomeWithIdView>,
    block_header: &near_primitives::views::BlockHeaderView,
    currencies: &Option<Vec<Currency>>,
) -> crate::errors::Result<Vec<FungibleTokenEvent>> {
    let mut res = Vec::new();
    for outcome in receipt_execution_outcomes {
        let events = extract_events(outcome);
        for event in events {
            res.extend(
                compose_rosetta_nep141_events(&event, outcome, block_header, currencies).await?,
            );
        }
    }
    Ok(res)
}

async fn compose_rosetta_nep141_events(
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
                        ft_events.push(build_event(base, custom).await?);

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
                        ft_events.push(build_event(base, custom).await?);
                    }
                }
            }
        }
    }
    Ok(ft_events)
}

pub(crate) async fn get_fungible_token_balance_for_account(
    view_client_addr: &actix::Addr<near_client::ViewClientActor>,
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
        .send(near_client::Query { block_reference, request }.with_span_context())
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

async fn build_event(
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
