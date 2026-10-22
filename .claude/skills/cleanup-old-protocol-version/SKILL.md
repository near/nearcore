---
name: cleanup-old-protocol-version
description: Bump MIN_SUPPORTED_PROTOCOL_VERSION by one and perform the corresponding old-protocol-version cleanups — remove deprecated ProtocolFeature variants, inline their enabled() checks, and clean up the matching runtime-parameter diff file. Use when the user wants to deprecate an old protocol version per issue #16564.
allowed-tools: Read, Grep, Glob, Bash, Edit, Write, AskUserQuestion
---

Deprecate the next old protocol version per
[issue #16564](https://github.com/near/nearcore/issues/16564). Historical
reference PRs: #14729 (inline a wasm_config param), #14756 (delete the
corresponding `.yaml`), #16454 (inline always-true flags from
`parameters.yaml`).

The skill takes no arguments and produces **a single PR** that bundles
every cleanup for one `MIN_SUPPORTED_PROTOCOL_VERSION` increment. It:

1. Reads `MIN_SUPPORTED_PROTOCOL_VERSION` from `core/primitives-core/src/version.rs`.
2. Sets `new_min = old_min + 1`.
3. Removes every `ProtocolFeature` variant with `protocol_version() <= new_min`
   and inlines each one's `.enabled(...)` call sites.
4. For each yaml under `core/parameters/res/runtime_configs/` whose version
   is `<= new_min` (typically just `{new_min}.yaml`): inlines parameter
   values into base `parameters.yaml`; for parameters that were only
   feature flags, also removes the plumbing; for operational values
   (fees, costs, limits), keeps the plumbing so tests can still override.
5. Inlines any `parameters.yaml` *feature flags* that have become dead
   now that their guarding feature is gone — the #16454-style follow-up.
6. Bumps `MIN_SUPPORTED_PROTOCOL_VERSION`, deletes the yaml files + their
   `CONFIG_DIFFS` entries + their per-version snapshots, regenerates the
   `test_json_unchanged` snapshots and the OpenAPI spec, and runs the
   prescribed Clippy pass.
7. Opens one PR for the whole bundle.

Rationale: once `MIN_SUPPORTED_PROTOCOL_VERSION = new_min`, every
`protocol_version >= new_min` holds unconditionally for all callers (either
by `clamp_to_supported_protocol_version`, which clamps up, or by
`assert_supported_protocol_version`, which rejects anything below). So any
`ProtocolFeature` whose `protocol_version <= new_min` has
`feature.enabled(v) == true` for every version the binary ever sees, and
the matching yaml diff collapses into the base `parameters.yaml`.

Do not open PRs between steps and do not pause for user approval on the
grouping — the whole thing is one PR, created in Step 7.

## 1. Read current state and plan

Collect, before touching any file:

- `old_min` = current value of `MIN_SUPPORTED_PROTOCOL_VERSION` in `core/primitives-core/src/version.rs`.
- `new_min` = `old_min + 1`.
- The set of `ProtocolFeature` variants with `protocol_version() <= new_min`
  (read the `impl ProtocolFeature { pub const fn protocol_version(self) ... }` block).
  Call this `deprecated_features`.
- The set of yaml files `{v}.yaml` under
  `core/parameters/res/runtime_configs/` with `v <= new_min`.
  Call this `deprecated_yamls`.
- For each `v > new_min` yaml file, the set of parameter names it mentions
  (needed in Step 3 to classify "overridden later").

Report to the user:
- `old_min → new_min`
- `deprecated_features` (count + names)
- `deprecated_yamls` (filenames)

No approval needed — proceed to Step 2.

## 2. Remove deprecated `ProtocolFeature` variants

In the templates below, `<F>` is the placeholder for the current feature
name — substitute the actual variant (e.g. `ImplicitAccountCreation`) when
running the greps and edits.

For each `<F>` in `deprecated_features`:

1. **In `core/primitives-core/src/version.rs`, remove both `<F>`'s enum
   variant and its arm in the `protocol_version()` match** in the same
   edit — removing one without the other won't compile. If `<F>` shared a
   grouped `|` arm, drop just that line; if it was the only variant for
   its version, drop the whole arm.
2. **Inline every call site**:
   - `grep -rn "ProtocolFeature::<F>\b"` across the workspace.
   - `<F>.enabled(v)` and `ProtocolFeature::<F>.enabled(v)` → `true`.
     Collapse the surrounding `if` / `match` / boolean expression: keep
     the taken branch, drop the dead one. `!<F>.enabled(v)` → `false`,
     drop the dead branch. For `let x = <F>.enabled(v);` with `x` used
     downstream, propagate the constant and remove the binding when it's
     no longer referenced.
   - Other uses of `ProtocolFeature::<F>` as a value (e.g. inserted into
     a set, printed in a log) → remove the use. If removing it leaves an
     empty collection or dead loop, drop that too.
3. **Clean up TODOs** anywhere in the codebase that reference `<F>` or
   its protocol version as the gating condition for removal (grep for
   `TODO.*<F>\b`, `TODO.*protocol version.*<F>'s version`). Resolve them now.
4. **Sweep for dead code and unused data structures** (see "Dead-code
   sweep" below). Collapsing branches above commonly orphans whole
   structs, enum variants, trait impls, helper fns, or crate-boundary
   types — remove them.

Do not commit or open a PR here; keep going.

## 3. Clean up each deprecated yaml

For each `{v}.yaml` in `deprecated_yamls`, **every** parameter line must
be inlined into `parameters.yaml` — the diffs form a chain validated by
`apply_diff` (each line's `old` must equal the current table value), so
simply dropping a line without updating the base would break the chain
and panic on the next yaml that touches the same parameter.

For each parameter `P: {old: A, new: B}` in `{v}.yaml`:

1. **Update `parameters.yaml`**: set `P`'s base value to `B` (the `new`
   value from `{v}.yaml`). If `P` is not already present, add it in the
   correct section (match the file's grouping conventions). If
   `parameters_testnet.yaml` exists, mirror the change there.
2. **Drop the line** from `{v}.yaml`.
3. **Update `core/parameters/res/runtime_configs/parameters.snap`** to
   reflect `P`'s new base value (match existing alignment).

Then classify `P` along two axes:

**(a) Overridden later** — its name appears in some `{w}.yaml` with `w > v`:

- Stop here for this parameter. Later yamls still read it, so it must stay
  in `enum Parameter` and in all consumer code. The `{w}.yaml` entry's
  `old` value should already match `B` (if it does not, something is wrong
  with the chain — stop and ask).

**(b) Not overridden later** — further classify:

- **Feature flag** (binary gate whose only semantic effect was to toggle
  the code paths just removed in Step 2 — e.g. `implicit_account_creation`,
  `eth_implicit_accounts`, `global_contract_host_fns`): dismantle the full
  plumbing (steps F1–F5 below).
- **Operational value** (a fee, gas cost, limit, or any numeric/structured
  value the runtime still needs to carry — e.g. `action_create_account`,
  `min_gas_purchase_price`, `account_creation_charge`, any byte or gas
  limit): **stop after the common steps 1–3 above**. Keep the `Parameter`
  variant, the `params.get(Parameter::P)?` read, the struct field, the
  `VMConfigView` field, and the OpenAPI entry. The runtime still reads the
  value; `RuntimeConfig::free()` zeroes it, `random_config()` randomizes
  it, and tests may still override it. The YAML update alone captures the
  fact that there is no longer a *per-protocol-version* difference.
- If classification is ambiguous (a bool used both as a gate *and*
  overridden by tests, or a value where the "gate" semantics aren't
  obvious), surface it to the user before removing the field.

### Feature-flag removal (steps F1–F5)

1. **Remove `P` from the `Parameter` enum** in
   `core/parameters/src/parameter.rs`, and from the
   `impl TryFrom<&ParameterTable> for RuntimeConfig` body in
   `core/parameters/src/parameter_table.rs` (the `params.get(Parameter::P)?`
   line).
2. **Remove `P` from `ConfigView` / `VMConfigView` entirely** (for
   `wasm_config` / `vm::Config` fields): delete the field from
   `VMConfigView`, from `vm::Config`, and from both `From` impls between
   them. We accept the backward-incompatibility
   ([zulip decision](https://near.zulipchat.com/#narrow/channel/524117-nearone.2Fbetter-engineering/topic/ConfigView.20cleanups/with/631917110)) —
   do not keep the field as `/// Deprecated` with a hardcoded value. The
   OpenAPI/OpenRPC specs will be regenerated in Step 6 — do not hand-edit
   `openapi.json` / `openrpc.json` here (per `AGENTS.md`'s OpenAPI policy).
3. **Replace every usage** across the codebase with the hardcoded value
   and collapse dead branches, same as Step 2.2.
4. Also remove the entry from `parameters.yaml`, `parameters.snap`, and
   `parameters_testnet.yaml` if it exists — now that `P` is gone from the
   enum, there's no reader for the base value either.
5. **Sweep for dead code and unused data structures** (see "Dead-code
   sweep" below) — once the parameter is gone and its guards are
   collapsed, config structs, helper fns and signature arguments often
   become unused.

After this step, each `{v}.yaml` in `deprecated_yamls` is empty but still
on disk (deleted in Step 6).

## 4. Verify intermediate state

Just a quick build-check before continuing — the full snapshot regen and
Clippy pass live in Step 6, after all cleanups are in. Running them per
step is both expensive and premature (Step 5 may remove more
`VMConfigView` fields).

- `cargo check -p near-parameters`
- `cargo check --workspace`

## 5. Inline leftover dead flags from `parameters.yaml`

Beyond the parameters inlined in Step 3, `parameters.yaml` may still carry
flags whose only false→true transition belonged to a now-removed
`ProtocolFeature` or to a yaml that's being deleted. Find them:

- `grep -nE ": (true|false)\s*(#.*)?$" core/parameters/res/runtime_configs/parameters.yaml`
  for boolean baselines (the pattern tolerates trailing whitespace and
  inline comments; avoids matching the many non-boolean lines that happen
  to end in a word).
- For each such flag `P`:
  - If `P` is read via `params.get(Parameter::P)` and the only call site is
    a check that always holds now (because the gating feature is gone),
    inline `P` to its post-bump value.
  - Only apply this to **feature flags** (binary gates). Do not inline
    operational values this way — same reasoning as Step 3's
    feature-flag-vs-operational-value split.
  - Remove `P` from `parameters.yaml`, from `parameters.snap`, from
    `parameters_testnet.yaml` if it exists, from the `Parameter` enum,
    from `parameter_table.rs`, from `vm::Config`, and from `VMConfigView`
    entirely (both `From` impls). Backward-incompatibility is accepted;
    do not keep a deprecated hardcoded field. (This is a change from the
    older #16454 shape, which did keep the field; follow the issue's
    updated guidance instead.) The OpenAPI/OpenRPC specs are regenerated
    in Step 6 — do not hand-edit them here.
  - Collapse all call sites; delete now-unused arguments to helper
    functions (see `transfer_exec_fee` in #16454).
  - **Sweep for dead code and unused data structures** (see "Dead-code
    sweep" below).

This step is often a no-op after Step 3; skip it if nothing qualifies.

## 6. Bump, delete, regenerate, verify

Do this once, after Steps 2–5 are all applied — then nothing else needs
touching before the PR.

1. **Bump `MIN_SUPPORTED_PROTOCOL_VERSION`** from `old_min` to `new_min` in
   `core/primitives-core/src/version.rs`.
2. For each `{v}.yaml` in `deprecated_yamls`:
   - Delete `core/parameters/res/runtime_configs/{v}.yaml`.
   - Remove the `(v, include_config!("{v}.yaml"))` entry from
     `CONFIG_DIFFS` in `core/parameters/src/config_store.rs`.
   - Delete `core/parameters/src/snapshots/near_parameters__config_store__tests__{v}.json.snap`.
3. **Regenerate the `test_json_unchanged` snapshots** —
   `cargo insta test --accept -p near-parameters -- tests::test_json_unchanged`
   (per the comment on the test itself in `config_store.rs`). This
   iterates over every stored version and re-serializes `VMConfigView`,
   so expected diff shapes are:
   - **Base-value shifts (Step 3 common path)**: for every parameter whose
     base changed from `A` to `B`, the version-0 snapshot picks up `B`.
     Snapshots for `w` where some yaml ≤ `w` still overrides the
     parameter stay unchanged (they already recorded the post-override
     value); snapshots for `w` where no override exists also stay
     unchanged (the removed `{v}.yaml` diff used to move `A → B`, so the
     effective state at `w` is the same `B` either way).
   - **`VMConfigView` field removals (Step 3 feature-flag path, Step 5)**:
     the removed field disappears from **every** snapshot (version-0 and
     every per-version snapshot), because the view is serialized once per
     version. The remaining fields' values should be unchanged for
     supported versions.
   - **Any value change at a supported version for a field that was not
     touched is a bug** — stop and investigate before accepting.
4. **Regenerate the OpenAPI/OpenRPC specs**. Per `AGENTS.md`, run the
   `openapi-spec` Justfile recipe rather than hand-editing the JSONs or
   the version strings in `src/main.rs` / `src/openrpc.rs` (the recipe
   updates all four locations in sync — the two JSONs and the two
   generator sources).
5. **Run the full verification**:
   - `cargo test -p near-parameters` — in particular
     `config_store::tests::all_configs_are_specified` (yaml file set vs
     `CONFIG_DIFFS`) and `test_json_unchanged`.
   - `cargo check --workspace`.
   - `RUSTFLAGS="-D warnings" cargo clippy --all-features --all-targets`
     — exactly as `AGENTS.md` prescribes; do not run clippy per-package or
     during intermediate sweeps.

## 7. Open the single PR

Stage every change from Steps 2–6 and invoke the `pr` skill with
`type: chore, project: runtime, title: cleanup for MIN_SUPPORTED_PROTOCOL_VERSION bump to <new_min>`.

PR body should:

- Note that `MIN_SUPPORTED_PROTOCOL_VERSION` moves `old_min → new_min`.
- List the removed `ProtocolFeature` variants.
- List the inlined and dropped parameters, distinguishing feature-flag
  removals (field dropped) from operational-value base-updates (field
  kept). Note that any removed `VMConfigView` fields are a
  backward-incompatible RPC change (per the
  [zulip decision](https://near.zulipchat.com/#narrow/channel/524117-nearone.2Fbetter-engineering/topic/ConfigView.20cleanups/with/631917110))
  and that the OpenAPI/OpenRPC specs were regenerated via the Justfile
  recipe.
- List which yaml files and snapshots were deleted.
- Reference `Part of #16564.`

## Dead-code sweep

After every cleanup in Steps 2, 3, and 5 — before you move on — inspect
the surrounding code for things that are now unreachable or orphaned.
Collapsing an `if feature.enabled(...)` guard or deleting a parameter from
a config struct frequently leaves whole items unused, but the Rust
compiler only flags a subset of these (unused `pub` items re-exported
across crates often go unwarned).

Run `cargo check --workspace` and treat every new `dead_code` /
`unused_imports` / `unused_variables` warning as in-scope. **Do not run
`cargo clippy` here** — per `AGENTS.md:31–32` clippy runs once, before
committing, with the prescribed command (see Step 6.5).

Then grep deliberately for the specific shapes the compiler misses:

- **Enum variants** that were only constructed inside the collapsed
  branch. Grep for each variant's name; if the only remaining references
  are the `match` arm plus `Debug`/serde impls, delete the variant.
  **Hard stop for Borsh-serialized enums**: variant tags are positional,
  so removing a variant from an enum that participates in persisted DB
  columns, block/chunk/transaction/action wire types, or any
  `BorshDeserialize`/`ProtocolSchema` structure shifts the tags of every
  later variant and silently breaks decoding of existing records. If the
  variant stops being constructed but the enum is Borsh-serialized (grep
  for `BorshSerialize` / `BorshDeserialize` / `ProtocolSchema` on the
  enum), **leave the variant in place** and only drop its constructor and
  internal uses. A notable example: `BlockHeaderV5` and similar
  per-version block/transaction/action variants must stay even after the
  code that constructs them is gone.
- **Structs / struct fields** that were only populated or read inside the
  collapsed branch. A field with no live reader can be removed; a struct
  whose only constructor is gone can be deleted along with its impls.
- **Helper functions / methods** whose only call site vanished.
- **Function arguments** that are no longer read inside the body (e.g.
  `transfer_exec_fee` dropping `eth_implicit_accounts_enabled` in #16454)
  — remove them from the signature, not just unused at every call site.
- **Trait impls / `From` conversions** whose target type just became
  unused.
- **Feature flags** (`#[cfg(feature = "...")]`) and `Cargo.toml` entries
  that only guarded the deleted code.
- **Imports and re-exports** that only mentioned the deleted item.
- **Protocol-version constants** (e.g. `pub const SOMETHING_PROTOCOL_VERSION`)
  defined for a now-removed feature.
- **Comments, docstrings, and `TODO`s** that reference the removed
  parameter/feature by name.

If a candidate for removal is `pub` and could plausibly be used by a
downstream crate (near-sdk, near-workspaces, external tooling), surface it
to the user before deleting rather than silently dropping a public item.

## Notes

- Operate on exactly one `MIN_SUPPORTED_PROTOCOL_VERSION` increment per
  invocation. If several bumps are needed, run the skill repeatedly — one
  PR per bump.
- Everything in Steps 2–6 lands in **one PR**. Do not call the `pr` skill
  mid-way or create interim commits for review.
- If classification uncovers anything unexpected (a parameter in `{v}.yaml`
  that isn't in `parameters.yaml` and isn't in `enum Parameter`, or an
  `old` value that disagrees with the current `parameters.yaml` baseline),
  stop and ask the user before touching code.
- If a `ProtocolFeature` arm you want to remove was `cfg`-gated or
  TODO-tagged for a different protocol version than its listed one, pause
  and confirm — the TODO may be the authoritative intent.
- Boolean inlining: `.enabled() → true` means keep the taken branch,
  delete the else; a helper argument that becomes constant should be
  removed from the signature, not just hardcoded at every call site.
