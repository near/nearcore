impl Client {
    pub fn process_contract_code_request(
        &mut self,
        request: CodeContractRequest,
    ) -> Result<(), Error> {
        // // We need the chunk header in order to process the chunk endorsement.
        // // If we don't have the header, then queue it up for when we do have the header.
        // // We must use the partial chunk (as opposed to the full chunk) in order to get
        // // the chunk header, because we may not be tracking that shard.
        // match self.chain.chain_store().get_partial_chunk(endorsement.chunk_hash()) {
        //     Ok(chunk) => self
        //         .chunk_endorsement_tracker
        //         .process_chunk_endorsement(&chunk.cloned_header(), endorsement),
        //     Err(Error::ChunkMissing(_)) => {
        //         tracing::debug!(target: "stateless_validation", ?endorsement, "Endorsement arrived before chunk.");
        //         self.chunk_endorsement_tracker.add_chunk_endorsement_to_pending_cache(endorsement)
        //     }
        //     Err(error) => return Err(error),
        // }
    }

    pub fn process_contract_code_response(
        &mut self,
        response: CodeContractResponse,
    ) -> Result<(), Error> {
        // // We need the chunk header in order to process the chunk endorsement.
        // // If we don't have the header, then queue it up for when we do have the header.
        // // We must use the partial chunk (as opposed to the full chunk) in order to get
        // // the chunk header, because we may not be tracking that shard.
        // match self.chain.chain_store().get_partial_chunk(endorsement.chunk_hash()) {
        //     Ok(chunk) => self
        //         .chunk_endorsement_tracker
        //         .process_chunk_endorsement(&chunk.cloned_header(), endorsement),
        //     Err(Error::ChunkMissing(_)) => {
        //         tracing::debug!(target: "stateless_validation", ?endorsement, "Endorsement arrived before chunk.");
        //         self.chunk_endorsement_tracker.add_chunk_endorsement_to_pending_cache(endorsement)
        //     }
        //     Err(error) => return Err(error),
        // }
    }
}