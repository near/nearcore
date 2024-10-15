use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::ProtocolVersion;
use near_primitives_core::version::ProtocolFeature;
use near_schema_checker_lib::ProtocolSchema;

/// A list of shard's bandwidth requests.
/// Describes how much the shard would like to send to other shards.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    ProtocolSchema,
)]
pub enum BandwidthRequests {
    V1(BandwidthRequestsV1),
}

impl BandwidthRequests {
    pub fn empty() -> BandwidthRequests {
        BandwidthRequests::V1(BandwidthRequestsV1 { requests: Vec::new() })
    }

    pub fn default_for_protocol_version(
        protocol_version: ProtocolVersion,
    ) -> Option<BandwidthRequests> {
        if ProtocolFeature::BandwidthScheduler.enabled(protocol_version) {
            Some(BandwidthRequests::empty())
        } else {
            None
        }
    }
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Default,
    Debug,
    Clone,
    PartialEq,
    Eq,
    ProtocolSchema,
)]
pub struct BandwidthRequestsV1 {
    pub requests: Vec<BandwidthRequest>,
}

/// `BandwidthRequest` describes the size of receipts that a shard would like to send to another shard.
/// When a shard wants to send a lot of receipts to another shard, it needs to create a request and wait
/// for a bandwidth grant from the bandwidth scheduler.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    ProtocolSchema,
)]
pub struct BandwidthRequest {
    pub to_shard: u8,
    // TODO(bandwidth_scheduler) - store requested bandwidth values inside the BandwidthRequest
}
