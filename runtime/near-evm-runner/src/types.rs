use borsh::{BorshDeserialize, BorshSerialize};

use near_vm_errors::VMLogicError;
use near_vm_logic::types::AccountId;
use std::io::{Read, Write};

pub type RawAddress = [u8; 20];
pub type RawHash = [u8; 32];
pub type RawU256 = [u8; 32];

pub type Result<T> = std::result::Result<T, VMLogicError>;

#[derive(BorshSerialize, BorshDeserialize)]
pub struct AddressArg {
    pub address: RawAddress,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ViewCallArgs {
    pub sender: RawAddress,
    pub address: RawAddress,
    pub amount: RawU256,
    pub args: Vec<u8>,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct GetStorageAtArgs {
    pub address: RawAddress,
    pub key: RawHash,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct WithdrawArgs {
    pub account_id: AccountId,
    pub amount: RawU256,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct TransferArgs {
    pub address: RawAddress,
    pub amount: RawU256,
}

impl BorshSerialize for ViewCallArgs {
    fn serialize<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write(&self.sender)?;
        writer.write(&self.address)?;
        writer.write(&self.amount)?;
        writer.write(&self.args)?;
        Ok(())
    }
}

impl BorshDeserialize for ViewCallArgs {
    fn deserialize(buf: &mut &[u8]) -> std::io::Result<Self> {
        if buf.len() < 72 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Unexpected length of input",
            ));
        }
        let sender = RawAddress::deserialize(buf)?;
        let address = RawAddress::deserialize(buf)?;
        let amount = RawU256::deserialize(buf)?;
        let mut args = Vec::new();
        buf.read_to_end(&mut args)?;
        Ok(Self { sender, address, amount, args })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_view_call() {
        let x = ViewCallArgs {
            sender: [1; 20],
            address: [2; 20],
            amount: [3; 32],
            args: vec![1, 2, 3],
        };
        let bytes = x.try_to_vec().unwrap();
        let res = ViewCallArgs::try_from_slice(&bytes).unwrap();
        assert_eq!(x, res);
        let res = ViewCallArgs::try_from_slice(&[0; 72]).unwrap();
        assert_eq!(res.args.len(), 0);
    }

    #[test]
    fn test_view_call_fail() {
        let bytes = [0; 71];
        let res = ViewCallArgs::try_from_slice(&bytes).unwrap_err();
    }
}
