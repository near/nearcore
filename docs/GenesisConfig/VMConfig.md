# VMConfig

Config of wasm operations.

## ext_costs

`type: [ExtCostsConfig]` [ExtCostsConfig](ExtCostsConfig.md)

Costs for runtime externals

## grow_mem_cost

`type: u32

Gas cost of a growing memory by single page.

## regular_op_cost

`type: u32

Gas cost of a regular operation.

## max_gas_burnt

`type: Gas

Max amount of gas that can be used, excluding gas attached to promises.

## max_stack_height

`type: u32

How tall the stack is allowed to grow?

## initial_memory_pages

`type: u32

## max_memory_pages

`type: u32

The initial number of memory pages.
What is the maximal memory pages amount is allowed to have for
a contract.

## registers_memory_limit

`type: u64

Limit of memory used by registers.

## max_register_size

`type: u64

Maximum number of bytes that can be stored in a single register.

## max_number_registers

`type: u64

Maximum number of registers that can be used simultaneously.

## max_number_logs

`type: u64

Maximum number of log entries.

## max_log_len

`type: u64

Maximum length of a single log, in bytes.
