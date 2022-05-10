## Core Resource Files

Stores resource data which is part of the protocol stable enough to be moved outside of the code.

### `runtime_configs`

All parameter value to configure the runtime are defined in `parameters.txt`.
Parameters added or changed in protocol upgrades are defined in differential
config files with a naming scheme like `V.txt`, where `V` is the new version.

The content of the base configuration file is one flat list of typed keys and
untyped values. Key names are defined in
`core/primitives-core/src/parameter.rs`.

The format of the differential files is slightly different. Inserting new
parameters uses the same syntax as the base configuration file: `key: value`.
Parameters that change are specified like this: `key: old_value -> new_value`.
Removing a previously defined parameter for a new version is done as follows: 
`key: old_value ->`. This causes the parameter value to be undefined in newer
versions which generally means the default value is used to fill in the
`RuntimeConfig` object.

Purely for the purpose of testing backwards compatibility, we also keep the
older config format for protocol versions that were released before changing to
this format. Those files are found in `./legacy_configs/` and contain
`RuntimeConfig` objects deserialized to JSON.
