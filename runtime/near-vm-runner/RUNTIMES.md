This document describes the considerations that go into picking a Wasm runtime as well as the
conclusions we made after evaluating a number of VM backends and frontends.

# Criteria

A number criteria have been used to evaluate a Wasm runtime for use in the implementation of the
NEAR protocol. Some of the criteria are already listed in the [FAQ] document and this document
gives a more thorough look. Listed roughly in the order of importance:

* security – how well does the runtime deal with untrusted input;
* correctness – does the runtime do what it claims to do;
* reliability – how much confidence is there in the implementation of runtime;
* platform support – does the runtime support targets that we want to target;
* performance – how quickly can the runtime execute the requested operations;

[FAQ]: FAQ.md#practical-aspects-of-the-execution-engines

# Requirements

Within each criteria we have specific requirements relevant to the NEAR protocol.

## Security

NEAR protocol validators execute arbitrary Wasm code submitted by the participants of the network,
much like e.g. web browsers execute untrusted JavaScript code. It is critical that the runtime
executes Wasm code in its own isolated, sandboxed environment, satisfying at least the following
requirements:

* a contract must not be presented with an opportunity to access any resources outside of those
  intentionally exposed by the validator implementation;
* contracts must be isolated from each other. Never should there be a situation where it is
  possible for a contract to recover and extradite state of a different instance of any contract.

A bug in the runtime is most likely to take a form of some sort of memory management issue, where
the memory is reused by the contract without zeroing it, or where the memory accesses aren't
validated correctly and allow access outside of the memory regions allocated for the contract.

## Correctness

A VM implementation must implement the Wasm specification precisely in order for it to be
considered correct. Typically the Wasm code will be generated as an output by compiling a program
implemented in a higher level programming language. Often the compiler will optimize the produced
Wasm code with the understanding that Wasm instructions behave as specified in the specification.
Were a VM implementation to deviate from specification in some way, there's a non-negligible risk
that a contract would invoke behaviours not intended by the contract authors (for example allow
unauthorized users to transfer tokens.)

In addition to this, the NEAR protocol adds a requirement that all executions of a Wasm program
must be deterministic. In other words, it must not be possible to observe different execution
results based on the environment within which the VM runs.

The Wasm specification claims fast, safe and portable semantics to be one of its design goals. Even
though Wasm specification does go to great lengths to make it possible to enable deterministic and
reproducible execution, it is not one of the foundational goals. Indeed, there are some parts of
the specification that allow for non-deterministic, non-reproducible execution.

A particularly troublesome area of the specification in this regard relates to the floating point
support. The specification permits floating point operations to return a `NaN` result which the
specification also permits to have multiple different bit representations and goes out of its way
to specify the sign of the `NaN` result to be non-deterministic. The determinism and
reproducibility requirements of the NEAR protocol make it necessary for the runtime to ensure NaN
signs and payloads are always deterministic.

Another detail to keep an eye out is whether the floating point results are being rounded
correctly. It is well known that the x87 co-processor does not necessarily round the results
correctly, so a special attention to how a runtime implements the floating point operations on the
32-bit x86 targets is warranted.

## Reliability

The required and desirable properties of a runtime implementation must remain true while the
changes are made to it. Compilers and, by extension, runtimes are complex systems and a poor
architecture may result in even simple changes introducing correctness or security bugs, which
aren't necessarily easily discoverable.

A reliable code base in this sense should be architected in such a way that the algorithms they
implement and the ways they execute Wasm code remain correct by construction and are easy to
verify.

Size of the test suite, architecture of the code base, and the number of developers/users are all
reasonably correlated to the reliability one can expect from an implementation of a runtime.

## Platform support

The NEAR validator implementation currently targets `x86_64-linux`, both as an execution
environment as well as to establish the cost model. For those reasons stellar support for this
target is a hard requirement.

However, the contract developers may not necessarily have a `x86_64` machine to develop with, or
they might be using a device running a different operating system. With the recent surge in
popularity of devices based on the Arm CPU architecture chips and generally high popularity of the
macOS and Windows operating systems it would be a huge boon to their development experience if the
runtime used also supported these other targets.

## Performance

There are a couple different ways we can evaluate the performance of the runtime. How quickly can
the runtime execute the instructions (throughput), execute the first instruction (latency), prepare
the initial code for execution (compilation) and how efficient are miscellaneous operations such as
serialization and deserialization of the module.

In order to realistically evaluate the performance of a runtime it is important to understand how
the NEAR validator utilizes the runtime to implement its functionality. Somewhat simplified view
is:

1. A module containing Wasm code in binary format is parsed and instrumented;
2. Wasm code produced after instrumentation is supplied to the runtime for preparation
   (compilation);
3. The prepared module is serialized and written to a long term storage (database);
4. When the contract function call occurs, the serialized module is deserialized, loaded into
   memory and the execution of the desired function begins (latency);
5. The contract code is executed (throughput) and the results are returned to the validator.

It is important to remember that the NEAR protocol charges fees for an operation before the
operation is executed. For that reason, predictability of worst case execution time often matters
more than the execution time in the typical case. It is important that we are able to deduce ahead
of time what cost to assign to a given operation. Inability to do so can lead to significant
undercharging and break the properties underlying the protocol.

On a scale of trade-offs, performance is probably one of the less important metrics. Slower
execution of a contract code doesn't make NEAR protocol unsound as an idea and only serves to
impose stricter limits on what can be achieved on the network.

### Compilation

The NEAR protocol charges gas fees to deploy a contract. One part of a deployment involves
preparing the contract Wasm code for execution. This may involve instrumentation, compilation,
serialization of the machine code and other similar operations.

An estimate of the deployment cost will typically involve a function which uses the size of the
input Wasm code as its primary input. Such a function can only exist if we have a good knowledge of
the runtime’s time complexity properties are known. For our purposes a linear or `O(n log n)`
relationship between the input size and execution time is the highest we can accept.

### Latency

Whenever a function within a contract is invoked, the serialized form of the contract will be
instantiated: loaded into memory and prepared for execution (though processes such as e.g.
linking). Only then it is possible to start executing the contract code.

When executing a tiny function part of a larger contract these operations will dominate and
contribute greatly to the observed latency of the contract execution. These overheads contribute to
the fees paid by anybody using the protocol, making any unnecessary overhead a potential roadblock
in NEAR protocol's adoption.

### Throughput

The faster we can execute the contract code, the more elaborate are the ideas that developers can
express via contracts running on the NEAR protocol. The cheaper it is to execute contracts, the
more approachable and usable the protocol is, so runtime performance is a direct consequence of the
usability of the whole system as a whole.

One of the more costly operations that occurs in a typical contract quite often is accounting for
gas fees. It is quite important that the runtime gives sufficient flexibility to the NEAR validator
to implement such operations efficiently.

# Evaluation

We have evaluated a number of different implementations over time. Currently Wasmer with the
universal engine and the single-pass backend is the default.

## `wasmer-singlepass` codegen

The `wasmer-singlepass` code generator is an implementation of a straightforward, direct
translation from Wasm statements to machine code. This approach is linear over the input size by
construction satisfying the requirements for preparation performance.

A single-pass codegen implementation in general incurs a trade-off in runtime performance.
WebAssembly is modelling a stack machine and an obviously correct implementation of such a machine
would be to directly translate all the stack operations to native code as well. Targets running
this code, on the other hand, are most likely an implementation of register machine where stack
operations are not necessarily all that efficient.

In order to improve the quality of the generated machine code, `wasmer-singlepass` maintains some
state between the individual Wasm instructions. This state allows `wasmer-singlepass` to avoid some
stack operations, pick more efficient instructions and sometimes avoid some operations such as NaN
canonicalization altogether.

Presence of this state comes at a cost of testability of implementation. The codegen for a given
instruction can depend on prior Wasm instructions. Because of this there is a combinatorial
explosion of instruction sequences that need to be considered and verified before confidence in the
results can be established.

Relatedly, at times it may also be difficult to validate changes made to the `wasmer-singlepass`
compiler. The global state its implementation relies on is, at times, a source of
[spooky action at a distance][spooky] problems – changing code generation of a specific instruction
can affect the implementation of another unrelated instruction as well.

The `wasmer-singlepass` backend currently only supports `x86_64` with `AVX` on Linux and, since
very recently, Windows.

[spooky]: https://en.wikipedia.org/wiki/Action_at_a_distance

## Cranelift codegen

Cranelift is a code generator written in Rust. It aims to be a fast code generator that also
produces machine code of a reasonably high quality. Originally it was designed for use in Firefox
for compiling arbitrary Wasm binaries presented to the browser by websites. Since then Cranelift
can be used for code generation in both Wasmer and Wasmtime.

Cranelift is a multi-stage compiler. It consumes an intermediate representation called CLIF which
is later lowered to the VCode IR on which operations such as instruction selection and register
allocation are applied to produce the machine code. Refer to [this blog post][clift-cg-primer] for
an in-depth explanation of the Cranelift's architecture.

[clift-cg-primer]: https://blog.benj.me/2021/02/17/cranelift-codegen-primer/

A Wasm codegen implementation in a browser shares many of the requirements with those of the NEAR
protocol. Cranelift gets high scores for its Correctness, Reliability and Security properties.
It supports targeting the Arm, s390x and x86_64 architectures as well as a variety of
operating systems, netting it a high score on the platform support metric as well.

Upon a brief investigation in December of 2021 generating machine code with Cranelift should
broadly be `O(n)` over input size with the exception of register allocation, which due to their
soon-to-be replaced regalloc algorithm is currently `O(n²)`.

One detail where Cranelift may fall short is in the ability to produce super-optimized machine code
sequences for hot operations such as gas counting.
