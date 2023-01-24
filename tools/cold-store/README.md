# Cold Storage testing tool

## Workflow

Start by trying something on localnet,
then move on to test you code on a dedicated machine
with real archival data.  
Ideally, on archival machine we only need to do every step before experimenting once.
But accidents happen, and we should be mindful of the time it takes us
to recover from them.

### Localnet

#### Prepare data
- Run `neard init` / `neard localnet`.  
- Change your config (`--home-dir`/config.json)
to not garbage collect larger number of epochs.  
By default it is 5, 
but you can change `gc_num_epochs_to_keep` to 1000, for example.  
Epoch lasts 500 blocks on localnet (as specified in `--home-dir`/genesis.json).
That will give you sort of archival storage of max 500'0000 blocks.

#### Produce blocks
Start localnet node as usual `neard run`.
Run node WITHOUT `--archive` parameter.  
In archive mode node will not save `TrieChanges`
and they are needed to copy blocks to cold.  
You can monitor the current height and make sure that we didn't cross
`gc_num_epochs_to_keep` epoch bound.
Otherwise, garbage collection will start,
and we cannot copy the block that has been garbage collected. 

#### Migrate and experiment
Migration can be done block by block,
because we have `TrieChanges` from the very start.

### Real archival data

#### Prepare machine
**TODO**

#### Prepare data
- Download snapshot of archival db to the machine.
- Create a patched binary that overrides `save_trie_changes: false`
for archival storage.

#### Produce blocks
Run that binary for at least `gc_num_epochs_to_keep`.
Epochs are larger on testnet/mainnet, so just give it a few days.  
You can use `sudo systemctl start neard` to run `/home/ubuntu/neard`  
and `jornalctl -u neard` to check logs.  
Be careful with what binary is at `/home/ubuntu/neard`.  
**TODO** some kind of system to maintain a bunch of local binaries.

#### Migrate
After archival storage is populated with enough of `TrieChanges`, 
it should not be experimented with. If we need more block, we should 
run a binary with that and only that one path -- saving of `TrieChanges`.  
Archival storage should probably lie NOT in `/home/ubuntu/.neard/data`.

That means, that unlike in real life, hot storage will not be mutated
archival storage, but rather a brand new one,
originally populated by copying of archival storage. 

And we WILL need a semi-proper migration to start split storage.  
By semi-proper I mean everything, but accurately changing State
column in hot. That is not needed to start experimenting.

## Subcommands
Before every subcommand the `NodeStorage` is opened with `NearConfig`
from `home_dir`.

### Open
Check that `NodeStorage::has_cold` is true.

### Head
Print `HEAD` of cold storage, `FINAL_HEAD` from hot storage
and also `HEAD` for hot storage.
It is useful to check that some blocks has been copied/produced. 

### CopyNextBlocks
Does `num_of_blocks`(1 by default) iterations of
- Copy block at height "cold HEAD + 1" to cold storage.  
- Update cold storage `HEAD`.

### (TODO) CopyAllBlocks
Initial population of cold storage, where we copy all cold column
to cold storage, plus set misc data like genesis hash and head.

### (TODO) GCHotSimpleAll
Initial garbage collection of hot storage, where we just delete
all the gc columns but `State` up to head of cold storage.

### (TODO) GCState
Initial gc of `State` for hot storage

### (TODO) CompareHotState
Takes hot storage and rpc storage,
performs some manipulation using `TrieChanges`
to makes their tail and head match,
compares State column (should be exactly the same). 