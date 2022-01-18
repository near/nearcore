# Near Telemetry

A small utility (TelemetryActor), that tries to send the telemetry (metrics) information as JSON over HTTP-post to selected list of servers.
Telemetry is sent from all the nearcore binaries (that enabled it in the config.json) - like validators, RPC nodes etc.

The data that is sent over is of type TelemetryInfo, and is signed with the server's key.

It contains info about the code (release version), server (cpu, memory and network speeds), and chain (node_id, status, peer connected, block height etc).

TODO: add pointer to the code, that is used by the receiving server.