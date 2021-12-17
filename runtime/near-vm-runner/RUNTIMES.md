This document describes the considerations that go into picking a Wasm runtime as well as the
conclusions we made after evaluating a number of VM backends and frontends.

# Criteria

There are a number criteria which have been used to evaluate a Wasm runtime for use in a NEAR
validator. Some of them are already listed in the
[FAQ](FAQ.md#practical-aspects-of-the-execution-engines) document. Listed roughly in the order of
importance:

* correctness – does the runtime do what it claims to do;
* security – how well does the runtime deal with untrusted input;
* reliability – how much confidence is there in the implementation of runtime;
* platform support – does the runtime support targets that we want to target;
* runtime performance – how quickly can the runtime execute the prepared Wasm code;
* preparation performance – how quickly can the runtime start running Wasm code;

# Requirements

Within each criteria we have specific requirements relevant to the NEAR validator.

## Correctness

A VM implementation must, first and foremost, implement the Wasm specification precisely in order
for it to be considered correct. Any deviations from the specification would render an
implementation incorrect.

In addition to this, the NEAR protocol adds a requirement that all executions of a Wasm program
must be deterministic. In other words, it must not be possible to observe different execution
results based on the environment within which the VM runs.

The Wasm specification specifies fast, safe and portable semantics to be one of its design goals.
Even though Wasm specification does go to great lengths to make it possible to enable deterministic
and reproducible execution, it is not one of the foundational goals. As thus, there are some parts
of the specification that allow for non-deterministic, non-reproducible execution.

A particularly troublesome area of the specification in this regard relates to the floating point
support. The specification permits floating point operations to return a `NaN` result which the
specification also permits to have multiple different bit representations and goes out of its way
to specify the sign of the `NaN` result to be non-deterministic. The determinism and
reproducibility requirements of a NEAR validator make it necessary for the runtime to ensure NaN
signs and payloads are always deterministic.

Another detail to keep an eye out is whether the floating point results are being rounded
correctly. It is well known that the x87 co-processor does not necessarily round the results
correctly, so a special attention to the implementation of the specification on the 32-bit x86
targets is warranted here.

## Security

NEAR protocol validators execute arbitrary Wasm code submitted by the participants of the network,
much like e.g. web browsers execute untrusted JavaScript code. It is critical that the runtime
executes the Wasm code in its own isolated environment. The contract code must not be presented
with an opportunity to access resources from the system running a validator node outside of what is
enabled via host functions exposed by the validator implementation itself.

Additionally, contracts should be isolated from each other as well. Never should there be a
situation where it is possible for a contract to recover and extradite state of a different
contract.

A bug in the runtime is most likely to take a form of some sort of memory management issue, where
the memory is reused by the contract without zeroing it, or where the memory accesses aren't
validated correctly and allow access outside of the memory regions allocated for the contract.

## Reliability

The required and desirable properties of a runtime implementation must remain true while the
changes are made to it. Compilers and, by extension, runtimes are complex systems and a poor
architecture may result in even simple changes introducing correctness or security bugs, which
aren't necessarily easily discovered.

A reliable code base in this sense should be architected in such a way that the algorithms they
implement and the way they execute Wasm code remains correct by construction and is easy to verify.

Size of a test suite, architecture of the code base and the number of developers/users are all
reasonably correlated signals as to the reliability which one can expect from a runtime.

## Platform support

The NEAR validator implementation currently targets `x86_64-linux`, so stellar support for this
target is a hard requirement.

However, developers may not necessarily have a `x86_64` machine to develop with, or they might be
using a device running a different operating system. With the recent surge in popularity of devices
based on the Arm CPU architecture chips and generally high popularity of the macOS and Windows
operating systems it would be a huge boon to the development experience if the runtime used also
supported these other targets.

## Runtime performance

The faster we can execute the contract code, the more elaborate are the ideas that developers can
express via contracts running on the NEAR protocol. The cheaper it is to execute contracts, the
more approachable and usable the protocol is, so runtime performance is a direct consequence of the
usability of the whole system as a whole.

One of more costly operations that occurs in a typical contract quite often is accounting for gas
costs, so it is quite important that the runtime gives sufficient flexibility to the NEAR
validator to implement such operations efficiently. And, of course, general code quality also
matters.

On a scale of trade-offs, runtime performance is probably one of the less important metrics. Slower
execution of a contract doesn't make NEAR protocol unsound as an idea, whereas something like a
non-deterministic execution outcome would.

## Preparation performance

The NEAR protocol charges gas fees to deploy a contract. One part of a deployment involves
preparing the contract Wasm code for execution. This may involve instrumentation, compilation,
serialization of the machine code and other similar operations.

The gas costs are charged before any work is done towards the deployment, so the cost of the
preparation, compilation and similar tasks must be predictable with a comparatively high precision.

An estimate of the deployment cost will typically involve a function which uses the size of the
input Wasm code as its primary input. Such a function can only exist if we have a good knowledge of
the runtime’s time complexity properties are known. For our purposes a linear or `O(n log n)`
relationship between the input size and execution time is the highest we can accept.

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

Presence of this state comes at a cost of testability of implementation. The behaviour of the
codegen can depend on prior Wasm instructions. Because of this there's a combinatorial explosion of
the test cases that need to be considered and verified before confidence in the results can be
established. Relatedly, at times it may also be difficult to validate changes made to the
implementation of the `wasmer-singlepass` codegen. The global state is definitely a source of
potential spooky action at a distance problems where changing code generation of a specific
instruction affects correctness and behaviour of another one.

The `wasmer-singlepass` backend currently only supports `x86_64` with `AVX` on Linux and, since
very recently, Windows.

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
validator. Cranelift gets high scores for its Correctness, Reliability and Security properties.
It supports targeting the Arm, s390x and x86_64 architectures as well as a variety of
operating systems, netting it a high score on the platform support metric as well.

Upon a brief investigation in December of 2021 generating machine code with Cranelift should
broadly be `O(n)` over input size with the exception of register allocation, which due to their
soon-to-be replaced regalloc algorithm is currently `O(n²)`.

One detail where Cranelift may fall short is in the ability to produce super-optimized machine code
sequences for hot operations such as gas counting.
