use primitives::crypto::aggregate_signature::BlsPublicKey;
use primitives::types::AuthorityId;

use crate::nightshade::{BareState, NSVerifyErr, State};

macro_rules! check_or {
    ($condition:expr, $error:expr) => {
        if !$condition {
            return Err($error);
        }
    };
}

impl State {
    /// Check if this state has correct proofs about the triplet it contains.
    /// Each authority will check if this state is valid only if it has not successfully verified another
    /// state with the same triplet before. Once it has verified that at least one authority has such
    /// triplet, it accepts all further states with the same triplet.
    ///
    /// # Arguments
    ///
    /// * `authority` - The authority that send this state.
    /// * `public_keys` - Public key of every authority in the network
    pub fn verify(
        &self,
        authority: AuthorityId,
        public_keys: &[BlsPublicKey],
        weights: &[usize],
    ) -> Result<(), NSVerifyErr> {
        // Check this is a valid triplet
        self.bare_state.verify()?;
        // Check signature for the triplet
        check_or!(
            public_keys[authority].verify(&self.bare_state.bs_encode(), &self.signature),
            NSVerifyErr::InvalidBlsSignature
        );
        if self.bare_state.primary_confidence > 0 {
            // If primary confidence is greater than zero there must be a proof for it
            if let Some(primary_proof) = &self.primary_proof {
                // Check primary_proof is ok
                primary_proof.verify(&public_keys, &weights)?;
                if self.bare_state.secondary_confidence > 0 {
                    // If secondary confidence is greater than zero there must be a proof for it
                    // Note that secondary confidence can be only greater than zero if primary confidence is greater than zero
                    if let Some(secondary_proof) = &self.secondary_proof {
                        // Check secondary_proof is ok
                        secondary_proof.verify(&public_keys, &weights)?;
                        let cur_bs = &self.bare_state;
                        let primary_bs = &primary_proof.bare_state;
                        let secondary_bs = &secondary_proof.bare_state;
                        // Current triplet and triplet from first proof must endorse same outcome
                        check_or!(
                            cur_bs.endorses == primary_bs.endorses,
                            NSVerifyErr::InconsistentState
                        );
                        // Both proof triplets can't endorse same outcome
                        check_or!(
                            primary_bs.endorses != secondary_bs.endorses,
                            NSVerifyErr::InconsistentState
                        );
                        // Primary confidence must be equal to one plus primary confidence from first proof triplet
                        check_or!(
                            cur_bs.primary_confidence == primary_bs.primary_confidence + 1,
                            NSVerifyErr::InconsistentState
                        );
                        // Secondary confidence must equal to one plus primary confidence from second proof triplet
                        check_or!(
                            cur_bs.secondary_confidence == secondary_bs.primary_confidence + 1,
                            NSVerifyErr::InconsistentState
                        );
                        // Secondary confidence must be consistent with secondary confidence from first proof triplet
                        check_or!(
                            secondary_bs.primary_confidence + 1 >= primary_bs.secondary_confidence,
                            NSVerifyErr::InconsistentState
                        );
                    } else {
                        return Err(NSVerifyErr::MissingProof);
                    }
                } else {
                    check_or!(self.secondary_proof.is_none(), NSVerifyErr::ExtraProof);
                    let bs_primary = &primary_proof.bare_state;
                    // If our current secondary confidence is zero, then the proof for primary confidence
                    // must have zero secondary confidence too.
                    check_or!(bs_primary.secondary_confidence == 0, NSVerifyErr::InconsistentState);
                    // Check that our current triplet is equal to triplet from the proof after increasing
                    // primary confidence by one
                    check_or!(
                        self.bare_state
                            == BareState {
                                primary_confidence: bs_primary.primary_confidence + 1,
                                endorses: bs_primary.endorses.clone(),
                                secondary_confidence: bs_primary.secondary_confidence,
                            },
                        NSVerifyErr::InconsistentState
                    );
                }
            } else {
                return Err(NSVerifyErr::MissingProof);
            }
        } else {
            check_or!(self.primary_proof.is_none(), NSVerifyErr::ExtraProof);
        }
        Ok(())
    }
}
