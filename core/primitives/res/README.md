## Core Resource Files

Stores resource data which is part of the protocol stable enough to be moved outside of the code.

### `runtime_configs`

All parameter value to configure the runtime are defined in `parameters.txt`.
Parameters added or changed in protocol upgrades are defined in differential
config files with a naming scheme like `V.txt`, where `V` is the new version.

The content of the initial config files and the diff is always just one flat
list of typed keys and untyped values. Key names are defined in
`core/primitives-core/src/parameter.rs`.

Purely for the purpose of testing backwards compatibility, we also keep the
older config format for protocol versions that were released before changing to
this format. Those files are found in `./legacy_configs/` and contain
`RuntimeConfig` objects deserialized to JSON. 
