# Contract preparation

This document describes the contract preparation and instrumentation process as well as the
limitations this process imposes on the contract authors.

In order to provide high performance execution of the [function calls], the near validator utilizes
ahead-of-time preparation of the contract WASM code. Today the contract code is prepared during the
execution of the [`DeployContractAction`]. The results are then saved and later reported to the
user when a [`FunctionCall`] is invoked.

Note that only some parts of this document are normative. This document delves into implementation
details, which may change in the future as long as the behavior documented by the normative
portions of this book is maintained. The non-normative portions of this document will be called out
as such.

[function calls]: ./FunctionCall.md
[`DeployContractAction`]: ./Actions.md#deploycontractaction

## High level overview

**This section is not normative.**

Upon initiation of a contract deployment, broadly the following operations will be executed:
validation, instrumentation, conversion to machine code (compilation) and storage of the compiled
artifacts in the account’s storage.

Functions exported from the contract module may then be invoked through the mechanisms provided
by the protocol. Two common ways to call a function is by submitting a function call action onto
the chain or via a cross-contract call.

Most of the errors that have occurred as part of validation, instrumentation, compilation, etc. are
saved and reported when a `FunctionCallAction` is submitted. Deployment itself may only report
errors relevant to itself, as described in the specification for [`DeployContractAction`].

## Validation

The table below lists the limits that apply to a contract deployed at protocol version 88
(nearcore 2.14.0). When the parser and a later check both limit the same thing, the table gives
the lower limit.

| What is limited | Limit | Runtime parameter |
| --- | --- | --- |
| Size of the wasm code | `4_194_304` bytes | `max_contract_size` |
| Types (signatures) in the module | `1_024` | `max_types_per_contract` |
| Function imports and functions defined in the module | `10_000` | `max_functions_number_per_contract` |
| Parameters of one function defined in the module | `64` | `max_params_per_function` |
| Parameters of all functions defined in the module | `50_000` | `max_params_per_contract` |
| Parameters of one signature, for example of an imported function | `1_000` | None (parser limit) |
| Results of one signature | `1` | None (multi-value is not enabled) |
| Locals of one function | `50_000` | None (parser limit) |
| Locals of all functions | `1_000_000`, and at most the code size in bytes divided by `2` | `max_locals_per_contract`, `min_contract_size_per_local` |
| Size of one function body | `196_608` bytes | `max_function_body_size` |
| `block`, `loop` and `if` instructions in one function | `5_000` | `max_blocks_per_function` |
| `block`, `loop` and `if` instructions in the module | `50_000` | `max_blocks_per_contract` |
| Operand stack of one function | `8_192` bytes | `max_operand_stack_bytes_per_function` |
| Size of the module after instrumentation | `16_777_216` bytes | `max_instrumented_code_size` |
| Tables | `1` | `max_tables_per_contract` |
| Initial size of a table | `10_000` entries | `max_elements_per_contract_table` |
| Imports | Function imports from the `env` module only | None |

The parser also limits the number of imports, exports, globals and data segments, and the length
of names. The rest of this section describes these limits, the protocol versions that introduced
them, and when each one is checked. The order matters, because only the first violation is
reported.

A number of limits are imposed on the WebAssembly module that is being parsed:

* The length of the wasm code must not exceed the `max_contract_size` runtime parameter;
* The wasm module must be a valid module according to the WebAssembly core 1.0 specification with
  these extensions enabled: sign-extension operators, non-trapping float-to-int conversions, and,
  from protocol version 84, reference types and bulk memory operations. Other extensions, such as
  multi-value returns, SIMD, threads, tail calls, multiple memories, memory64 and exception
  handling, are rejected;
* The wasm module may contain no more than:
  * `1_024` types (signatures), set by the `max_types_per_contract` runtime parameter from protocol
    version 84;
  * `1_000_000` function imports and local function definitions;
  * `100_000` imports;
  * `100_000` exports;
  * `1_000_000` global imports and module-local global definitions;
  * `100_000` data segments;
  * `1` table;
  * `1` memory;
* UTF-8 strings comprising the wasm module definition (e.g. export name) may not exceed `100_000`
  bytes each;
* Function definitions may not specify more than `50_000` locals;
* Signatures may not specify more than `1_000` parameters;
* Functions defined in the module may not have more than `64` parameters each
  (`max_params_per_function`). All functions defined in the module together may not have more than
  `50_000` parameters (`max_params_per_contract`). Both limits apply from protocol version 84;
* Signatures may not specify more than `1` result, because the multi-value proposal is not enabled;
* Tables may not specify an initial size of more than `10_000` entries (the
  `max_elements_per_contract_table` runtime parameter, from protocol version 82);

If the contract code is invalid, the first violation in the binary encoding of the WebAssembly
module shall be reported. These additional requirements are imposed after the module is parsed:

* The wasm module may contain no more function imports and local function definitions than the
  `max_functions_number_per_contract` runtime parameter (`10_000`).
* The total number of locals declared in all functions may not exceed `max_locals_per_contract`
  (`1_000_000`). From protocol version 87 (nearcore 2.14.0), the total also may not exceed the
  contract size in bytes divided by `min_contract_size_per_local` (`2`). Before protocol version 87,
  only the `max_locals_per_contract` limit applies.
* Each function body may not exceed `max_function_body_size` (`196_608` bytes).
* Each function may contain no more than `max_blocks_per_function` (`5_000`) `block`, `loop` and
  `if` instructions. The whole module may contain no more than `max_blocks_per_contract`
  (`50_000`) of these instructions.
* The operand stack of each function may not exceed `max_operand_stack_bytes_per_function`
  (`8_192` bytes).
* The module after instrumentation may not exceed `max_instrumented_code_size` (`16_777_216`
  bytes).
* All imports must be function imports from the `env` module. A module that imports a table, a
  global or a memory is rejected. The runtime provides the contract memory itself, as described in
  [Memory normalization](#memory-normalization).

The function body, block, operand stack and instrumented size limits apply from protocol version 84.

## Memory normalization

All near contracts have the same amount of memory made available for execution. The exact amount is
specified by the `initial_memory_pages` (`1_024`) and `max_memory_pages` (`2_048`) runtime
parameters, in pages of 64 KiB. In order to ensure a level playing field, preparation removes any
memory definition of the module and adds one memory definition with these limits. A module without
a memory definition gets the same memory. The limits that the module declared are not used. A
module that imports a memory is rejected.

## Gas instrumentation

In order to implement precise and efficient gas accounting, the contract code is analyzed and
instrumented with additional operations before the compilation occurs. One such instrumentation
implements accounting of the gas fees.

Gas fees are accounted for at a granularity of sequences of instructions forming a metered block
and are consumed before execution of any instruction part of such a sequence. The accounting
mechanism verifies the remaining gas is sufficient, and subtracts the gas fee from the remaining
gas budget before continuing execution. In the case where the remaining gas balance is insufficient
to continue execution, the `GasExceeded` error is raised and execution of the contract is
terminated.

The gas instrumentation analysis will segment a wasm function into metered blocks. In the end,
every instruction will belong to exactly one metered block. The algorithm uses a stack of metered
blocks and instructions are assigned to the metered block on top of the stack. The following
terminology will be used throughout this section to refer to metered block operations:

* active metered block – the metered block at the top of the stack;
* pop – the active metered block is removed from the top of the stack;
* push – a new metered block is added to the stack, becoming the new active metered block.

A metered block is pushed onto the stack upon a function entry and after every `if` and `loop`
instruction. After the `br`, `br_if`, `br_table`, `else` & `return` (pseudo-)instructions the
active metered block is popped and a new metered block is pushed onto the stack.

The `end` pseudo-instruction associated with the `if` & `loop` instructions, or when it terminates
the function body, will cause the top-most metered block to be popped off the stack. As a
consequence, the instructions within a metered block need not be consecutive in the original
function. If the `if..end`, `loop..end` or `block..end` control block terminated by this `end`
pseudo-instruction contained any branching instructions targeting control blocks other than the
control block terminated by this `end` pseudo instruction, the currently active metered block is
popped and a new metered block is pushed.

Note that some of the instructions considered to affect the control flow in the WebAssembly
specification such as `call`, `call_indirect` or `unreachable` do not affect metered block
construction and are accounted for much like other instructions not mentioned in this section. This
also means that calling the `used_gas` host function at different points of the same metered block
would return the same value if the `base` cost was `0`.

All the instructions covered by a metered block are assigned a fee based on the `regular_op_cost`
runtime parameter. Pseudo-instructions do not cause any fee to be charged. A sum of these fees is
then charged by instrumentation inserted at the beginning of each metered block.

### Examples

**This section is not normative.**

In this section some examples of the instrumentation are presented as an understanding aid to the
specification above. The examples are annotated with comments describing how much gas is charged at
a specific point of the program. The programs presented here are not intended to be executable or
to produce meaningful behavior.

#### `block` instruction does not terminate a metered block

```wat
(func
  (; charge_gas(6 regular_op_cost) ;)
  nop
  block
    nop
    unreachable
    nop
  end
  nop)
```

This function has just 1 metered block, covering both the `block..end` block as well as the 2 `nop`
instructions outside of it. As a result the gas fee for all 6 instructions will be charged at the
beginning of the function (even if we can see `unreachable` would be executed after 4 instructions,
terminating the execution).

#### Branching instructions pop a metered block

Introducing a conditional branch to the example from the previous section would split the metered
block and the gas accounting would be introduced in two locations:

```wat
(func
  (; charge_gas([nop block br.0 nop]) ;)
  nop
  block
    br 0
    (; charge_gas([nop nop]) ;)
    nop
    nop
  end
  nop)
```

Note that the first metered block is disjoint and covers the `[nop, block, br 0]` instruction
sequence as well as the final `nop`. The analysis is able to deduce that the `br 0` will not be
able to jump past the final `nop` instruction, and therefore is able to account for the gas fees
incurred by this instruction earlier.

Replacing `br 0` with a `return` would enable jumping past this final `nop` instruction, splitting
the code into three distinct metered blocks instead:

```wat
(func
  (; charge_gas([nop block return]) ;)
  nop
  block
    return
    (; charge_gas([nop nop]) ;)
    nop
    nop
  end
  (; charge_gas([nop]) ;)
  nop)
```

#### `if` and `loop` push a new metered block

```wat
(func
  (; charge_gas([loop unreachable]) ;)
  loop
    (; charge_gas([br 0]) ;)
    br 0
  end
  unreachable
)
```

In this example the `loop` instruction will always introduce a new nested metered block for its
body, for the `end` pseudo-instruction as well as `br 0` cause a backward jump back to the
beginning of the loop body. A similar reasoning works for `if .. else .. end` sequence since the
body is only executed conditionally:

```wat
(func
  (; charge_gas([i32.const.42 if nop]) ;)
  i32.const 42
  if
    (; charge_gas([nop nop]) ;)
    nop
    nop
  else
    (; charge_gas([unreachable]) ;)
    unreachable
  end
  nop)
```

## Operand stack depth instrumentation

The `max_stack_height` runtime parameter (`262_144`) sets a stack budget in bytes for the whole
call stack. Each operand stack value uses 4 bytes (`i32`, `f32`) or 8 bytes (`i64`, `f64`,
references). Each active function call also uses its frame: 64 bytes plus the size of its locals.
From protocol version 84, a function whose operand stack can exceed
`max_operand_stack_bytes_per_function` (`8_192` bytes) is rejected at preparation.

The maximum operand stack size of each function, in bytes, is computed statically by simulating
the operand stack operations executed by each instruction. For example, `i32.const 1` pushes one
4-byte value on the operand stack, whereas `i32.add` pops two values and pushes one value containing
the result. The maximum operand stack size of the `if..else..end` control block is the larger of the
sizes for the two bodies of the conditional. Stack operations for each instruction are otherwise
specified in the section 4 of the WebAssembly core 1.0 specification and in the enabled extensions.

When a function starts, its maximum operand stack size plus its frame size is subtracted from the
remaining stack budget, which starts at `max_stack_height`. If the budget would go below zero, the
execution of the contract stops with a `MemoryAccessViolation` error. The amount is added back to
the budget when the function returns.

Both the operand stack sizes and the gas fees are computed from the code before any
instrumentation is added.

### Examples

**This section is not normative.**

Picking the example from the gas instrumentation section, without the gas charges, we can tell
that this function needs 4 bytes of operand stack. Let's annotate the operand stack operations at
each of the instructions:

```wat
(func
  i32.const 42             (; [] => [i32]       ;)
  if                       (; [i32] => []       ;)
    nop                      (; []                ;)
    nop                      (; []                ;)
  else
    unreachable              (; []                ;)
  end
  nop                      (; [] ;)
)
```

At no point does the operand stack contain more than one `i32` value. As a
result, the maximum operand stack size of this function is 4 bytes. When the function starts, the
runtime takes these 4 bytes plus the frame size of the function from the stack budget.

<!-- TODO: describe the ahead-of-time compilation details and the errors it may cause or limits it
     may impose -->
