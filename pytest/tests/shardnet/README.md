# Shardnet tools

## restake.py

Manages restaking of shardnet network participants. Uses `restaked` to regularly restake if a node is kicked.
Runs `restaked` on each of the remote machines. Gets the `restaked` binary from AWS.

Optionally creates accounts for the remote nodes, but requires public and private keys of account `near`.

## Example

```
python3 tests/shardnet/restake.py
    --delay-sec 60
    --near-pk $NEAR_PUBLIC_KEY
    --near-sk $NEAR_PRIVATE_KEY
```
