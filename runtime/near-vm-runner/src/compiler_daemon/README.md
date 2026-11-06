# Compiler daemon

This directory implements the parent-side compiler worker pool, its IPC
protocol, and the worker subprocess entry point.

See [Contract compilation](../../../../docs/architecture/how/compilation.md) for
the architecture, resource-recovery model, and process-reaping behavior.
