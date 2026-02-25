# chain/chain

## Consensus
- For high-level documentation on the consensus protocol, see `docs/ChainSpec/Consensus.md`.
- Doomslug is implemented in `chain/chain/src/doomslug.rs`.

## Block Processing
- Entry point: `Chain::start_process_block_async` in `chain/chain/src/chain.rs`.
- Blocks whose parent is unknown are stored in the orphan pool and processed when the parent arrives.
- Synchronous preprocessing in `preprocess_block` validates in this order: block signature → header → block body → chunk headers → chunk endorsements (>2/3 stake per chunk for non-SPICE) → missing chunks check.
- After validation, chunk application runs asynchronously on a rayon thread pool; postprocessing saves state and updates head.
- Optimistic blocks allow chunk application to start before the full block arrives; cached results are reused during normal block processing. See `docs/architecture/how/optimistic_block.md`. Disabled for SPICE.
- See `docs/ChainSpec/BlockProcessing.md` for the spec and `docs/ChainSpec/Consensus.md` for Doomslug finality.
