# Fixing a protocol finding: traps

Read before writing a fix for anything an audit turns up in the runtime, the parameter set or chunk
production. Every item here is something that went wrong, or nearly went wrong, in a real run.

## Is it a validity change, and does the action already exist?

A new limit that rejects something previously accepted is a **consensus change**. It must switch at
a protocol version boundary, or nodes disagree on whether a chunk is valid.

The shape depends on one question: **did the action exist before the limit?**

- **Action is new in the unreleased version.** Put the value straight in the base
  `parameters.yaml`, with no version diff. It cannot change any historical behaviour, because
  `require_protocol_feature` refuses the action at every earlier version. No `ProtocolFeature`
  variant is needed either.
- **Action is already live.** The base value must stay effectively unlimited, and the real value
  goes in the version diff for the upcoming version (`NN.yaml`, as `{old: <unlimited>, new: <v>}`).
  A real value in the base config **retroactively invalidates transactions that were valid at
  earlier versions and breaks replay of the chunks carrying them.** Verify with the config-store
  snapshots: earlier versions must still show the unlimited value.

If the upcoming version has not activated anywhere, including testnet, a tightening can ride its
existing `NN.yaml` and needs no extra upgrade cycle. Confirm activation status with the user; it is
not derivable from the repo.

A pure parameter tightening usually needs **no `ProtocolFeature` variant**: enforce it
unconditionally and let the version-resolved value do the gating. Precedent for both patterns exists
in the tree, so match whichever the reviewer will expect.

## The receipt-mode trap

This one breaks nodes at the upgrade boundary and is easy to miss.

Incoming and delayed receipts are validated in `ValidateReceiptMode::ExistingReceipt`, where a
failure is **fatal**: `RuntimeError::ReceiptValidationError` for an incoming receipt,
`StorageError::StorageInconsistentState` for one already in the delayed queue. An action error it is
not.

So if the action predates the limit, there can be receipts already in flight or already stored that
violate it the moment the version activates. **Enforce new restrictions in `NewReceipt` mode only.**
Where the action is new in the activating version, no such receipt can exist and the tolerance is
unnecessary, but say so in a comment rather than leaving it implicit.

Whether such a tolerance can ever be removed later: only by another version-gated change, because
the boundary-window chunks are in history forever and replay re-validates them with that version's
config. The existing tolerances in the tree have never been removed.

## Bound the aggregate, not the item

Ask what quantity the protocol actually charges for, and bound exactly that.

A per-item limit is multiplied by the item count whenever a receipt can carry many items. In the
source run every action in a receipt shares its receiver, and the action had to derive to that
receiver, so a receipt could carry many byte-identical copies and paid for each one: a per-action
limit of N became an effective limit of 65N, restoring the full attack.

Cross-check the limit against whatever bounds the **contract-created** path, which is usually a
single gas budget across a whole promise batch and therefore inherently aggregate. If the two
disagree in shape, the transaction-path limit is probably the wrong shape. Sizing the limit at or
just above the contract path's own ceiling also guarantees no action a contract can pay for becomes
unrepresentable.

## Make the match exhaustive

When counting or classifying over an action or message enum, write the match exhaustively and say in
a comment not to collapse the arms into a wildcard. A wildcard means a new variant silently counts
as zero.

Removing one wildcard in the source run surfaced a question nobody had asked: whether the count
should recurse into delegate actions. It has to, because the fee accounting it mirrors recurses, so
the outer receipt prepays the inner actions' execution fees. Without the recursion a meta
transaction doubled the reachable total. **When the quantity mirrors a fee computation, mirror that
computation's recursion exactly**, and check the limits beside it: one of them deliberately does
*not* sum across the delegate boundary, because it counts a different thing.

## Regenerate what CI checks, and fix what it does not

After changing a parameter, an error enum or a config view:

- **Protocol schema.** Read `tools/protocol-schema-check/README.md`, run the check, review the
  cascade, and copy the generated file over the resource. Adding an enum variant is a
  Borsh-compatible change the checker flags anyway; confirm existing discriminants are unmoved and
  that no on-chain outcome can carry the new variant.
- **OpenAPI and OpenRPC specs.** One generator writes both. Bump the spec version once per PR,
  following whatever the last comparable change did.
- **Config-store snapshots.** Regenerate, then read the diff: the gating is correct only if earlier
  versions kept the old value.
- **Test-only config overrides.** The params estimator lifts limits in its own `LimitConfig` block
  so its fixtures can be large. A new limit is **not** in that block, and CI does not run the
  estimator, so nothing will tell you it is broken. In the source run this would have panicked one
  estimation on a `.expect("expected no validation error")` and, worse, silently mis-measured
  another whose transaction would have been dropped as invalid with nothing asserting on outcomes.
  Grep for large fixtures of whatever you just bounded.

## Producer-side heuristics are not consensus

Chunk production choices, such as how long transaction selection may spend, are the producer's own
and already read a wall clock. Changing them needs **no protocol version, no parameter and no
feature gate**, which makes them shippable independently of a protocol release, including in a patch.
Establish which side of that line a fix sits on before designing it: it changes the cost of shipping
by an entire upgrade cycle.

## Before merging a limit, check nobody is using it

A limit that forbids something people already do is a functional regression, and for anything whose
identifier is derived from its content it can be worse: the derived address becomes permanently
unreachable, and any balance pre-funded to it is stranded.

Answering this needs chain data, not the repo. Budget for it, bound the spend, and record the method
and the queries in the audit directory so the number can be re-checked. Distinguish what the data
proves from what it merely fails to contradict, and state that the same check was not run for
testnet or other networks if it was not.
