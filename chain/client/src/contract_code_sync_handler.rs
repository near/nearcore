use crate::Client;

use near_chain_primitives::Error;
use near_primitives::contract_code_sync::{ContractCodeRequest, ContractCodeResponse};

impl Client {
    pub fn process_contract_code_request(
        &mut self,
        _request: ContractCodeRequest,
    ) -> Result<(), Error> {
        // TODO(#11099): Implement this - Return the contract code that belong to the given accounts.
        unimplemented!()
    }

    pub fn process_contract_code_response(
        &mut self,
        _response: ContractCodeResponse,
    ) -> Result<(), Error> {
        // TODO(#11099): Implement this - Initiate compilation of the contract code.
        unimplemented!()
    }
}
