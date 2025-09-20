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

A number of limits are imposed on the WebAssembly module that is being parsed:

* The length of the wasm code must not exceed `max_contract_size` genesis configuration parameter;
* The wasm module must be a valid module according to the WebAssembly core 1.0 specification (this
  means no extensions such as multi value returns or SIMD; this limitation may be relaxed in the
  future).
* The wasm module may contain no more than:
  * `1_000_000` distinct signatures;
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
* Signatures may not specify more than `1_000` results;
* Tables may not specify more than `10_000_000` entries;

If the contract code is invalid, the first violation in the binary encoding of the WebAssembly
module shall be reported. These additional requirements are imposed after the module is parsed:

* The wasm module may contain no more function imports and local definitions than specified in the
  `max_functions_number_per_contract` genesis configuration parameter; and

These additional requirements are imposed after the instrumentation, as documented in the later
sections:

* All imports may only import from the `env` module.

## Memory normalization

All near contracts have the same amount of memory made available for execution. The exact amount is
specified specified by the `initial_memory_pages` and `max_memory_pages` genesis configuration
parameters. In order to ensure a level playing field, any module-local memory definitions are
transparently replaced with an import of a standard memory instance from `env.memory`.

If the original memory instance definition specified limits different from those specified by the
genesis configuration parameters, the limits are reset to the configured parameters.

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
genesis parameter. Pseudo-instructions do not cause any fee to be charged. A sum of these fees is
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

The `max_stack_height` genesis parameter imposes a limit on the number of entries the wasm
operand stack may contain during the contract execution.

The maximum operand stack height required for a function to execute successfully is computed
statically by simulating the operand stack operations executed by each instruction. For example,
`i32.const 1` pushes 1 entry on the operand stack, whereas `i32.add` pops two entries and pushes 1
entry containing the result value. The maximum operand stack height of the `if..else..end` control
block is the larger of the heights for two bodies of the conditional. Stack operations for each
instruction are otherwise specified in the section 4 of the WebAssembly core 1.0 specification.

Before the `call` or `call_indirect` instructions are executed, the callee's required stack is
added to the current stack height counter and is compared with the `max_stack_height` parameter. A
trap is raised if the counter exceeds the limit. The instruction is executed, otherwise.

Note that the stack depth instrumentation runs after the gas instrumentation. At each point where
gas is charged one entry worth of operand stack space is considered to be used.

### Examples

**This section is not normative.**

Picking the example from the gas instrumentation section, we can tell that this function will use
just 1 operand slot. Lets annotate the operand stack operations at each of the instructions:

```wat
(func
  (; charge_gas(...) ;)    (; [] => [gas] => [] ;)
  i32.const 42             (; [] => [i32]       ;)
  if                       (; [i32] => []       ;)
    (; charge_gas(...) ;)    (; [] => [gas] => [] ;)
    nop                      (; []                ;)
    nop                      (; []                ;)
  else
    (; charge_gas(...) ;)    (; [] => [gas] => [] ;)
    unreachable              (; []                ;)
  end
  nop                      (; [] ;)
)
```

We can see that at no point in time the operand stack contained more than 1 entry. As a result,
the runtime will check that 1 entry is available in the operand stack before the function is
invoked.

<!-- TODO: describe the ahead-of-time compilation details and the errors it may cause or limits it
     may impose -->
