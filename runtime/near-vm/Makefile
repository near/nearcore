SHELL=/usr/bin/env bash


#####
#
# The Matrix
#
#####

# The matrix is the product of the following columns:
#
# |------------|-----------|----------|--------------|-------|
# | Compiler   ⨯ Engine    ⨯ Platform ⨯ Architecture ⨯ libc  |
# |------------|-----------|----------|--------------|-------|
# | Singlepass | Universal | Linux    | amd64        | glibc |
# |            |           | Darwin   | aarch64      | musl  |
# |            |           | Windows  |              |       |
# |------------|-----------|----------|--------------|-------|
#
# Here is what works and what doesn't:
#
# * Singlepass with the Universal engine works on Linux+Darwin/`amd64`, but
#   it doesn't work on */`aarch64` or Windows/*.
#
# * Singlepass with the Dylib engine doesn't work because it doesn't
#   know how to output object files for the moment.
#
# * Windows isn't tested on `aarch64`, that's why we consider it's not
#   working, but it might possibly be.


#####
#
# Define the “Platform” and “Architecture” columns of the matrix.
#
#####


IS_DARWIN := 0
IS_LINUX := 0
IS_WINDOWS := 0
IS_AMD64 := 0
IS_AARCH64 := 0

# Test Windows apart because it doesn't support `uname -s`.
ifeq ($(OS), Windows_NT)
	# We can assume it will likely be in amd64.
	IS_AMD64 := 1
	IS_WINDOWS := 1
else
	# Platform
	uname := $(shell uname -s)

	ifeq ($(uname), Darwin)
		IS_DARWIN := 1
	else ifeq ($(uname), Linux)
		IS_LINUX := 1
	else
		# We use spaces instead of tabs to indent `$(error)`
		# otherwise it's considered as a command outside a
		# target and it will fail.
                $(error Unrecognized platform, expect `Darwin`, `Linux` or `Windows_NT`)
	endif

	# Architecture
	uname := $(shell uname -m)

	ifeq ($(uname), x86_64)
		IS_AMD64 := 1
	else ifneq (, $(filter $(uname), aarch64 arm64))
		IS_AARCH64 := 1
	else
		# We use spaces instead of tabs to indent `$(error)`
		# otherwise it's considered as a command outside a
		# target and it will fail.
                $(error Unrecognized architecture, expect `x86_64`, `aarch64` or `arm64`)
	endif

	# Libc
	LIBC ?= $(shell ldd 2>&1 | grep -o musl | head -n1)
endif


#####
#
# Define the “Compiler” column of the matrix.
#
#####


# Variables that can be overriden by the users to force to enable or
# to disable a specific compiler.
ENABLE_SINGLEPASS ?=

# Which compilers we build. These have dependencies that may not be on the system.
compilers :=
exclude_tests :=

##
# Singlepass
##

# If the user didn't disable the Singlepass compiler…
ifneq ($(ENABLE_SINGLEPASS), 0)
	# … then maybe the user forced to enable the Singlepass compiler.
	ifeq ($(ENABLE_SINGLEPASS), 1)
		compilers += singlepass
	# … otherwise, we try to check whether Singlepass works on this host.
	else ifneq (, $(filter 1, $(IS_DARWIN) $(IS_LINUX) $(IS_WINDOWS)))
		ifeq ($(IS_AMD64), 1)
			compilers += singlepass
		endif
	endif
endif

ifneq (, $(findstring singlepass,$(compilers)))
	ENABLE_SINGLEPASS := 1
endif

##
# Clean the `compilers` variable.
##

compilers := $(strip $(compilers))


#####
#
# Define the “Engine” column of the matrix.
#
#####


# The engine is part of a pair of kind (compiler, engine). All the
# pairs are stored in the `compilers_engines` variable.
compilers_engines :=

##
# The Singlepass case.
##

ifeq ($(ENABLE_SINGLEPASS), 1)
	ifneq (, $(filter 1, $(IS_DARWIN) $(IS_LINUX)))
		ifeq ($(IS_AMD64), 1)
			compilers_engines += singlepass-universal
		endif
	endif
endif

# Clean the `compilers_engines` variable.
compilers_engines := $(strip $(compilers_engines))


#####
#
# Cargo features.
#
#####

# Small trick to define a space and a comma.
space := $() $()
comma := ,

# Define the compiler Cargo features for all crates.
compiler_features := --features $(subst $(space),$(comma),$(compilers))

#####
#
# Display information.
#
#####

ifneq (, $(filter 1, $(IS_DARWIN) $(IS_LINUX)))
	bold := $(shell tput bold 2>/dev/null || echo -n '')
	green := $(shell tput setaf 2 2>/dev/null || echo -n '')
	yellow := $(shell tput setaf 3 2>/dev/null || echo -n '')
	reset := $(shell tput sgr0 2>/dev/null || echo -n '')
endif

HOST_TARGET=$(shell rustup show | grep 'Default host: ' | cut -d':' -f2 | tr -d ' ')

TARGET_DIR := target/release

ifneq (, $(TARGET))
	TARGET_DIR := target/$(TARGET)/release
endif

$(info -----------)
$(info $(bold)$(green)INFORMATION$(reset))
$(info -----------)
$(info )
$(info Host Target: `$(bold)$(green)$(HOST_TARGET)$(reset)`.)
ifneq (, $(TARGET))
	# We use spaces instead of tabs to indent `$(info)`
	# otherwise it's considered as a command outside a
	# target and it will fail.
        $(info Build Target: $(bold)$(green)$(TARGET)$(reset) $(yellow)($(TARGET_DIR))$(reset))
endif
ifneq (, $(LIBC))
	# We use spaces instead of tabs to indent `$(info)`
	# otherwise it's considered as a command outside a
	# target and it will fail.
        $(info C standard library: $(bold)$(green)$(LIBC)$(reset))
endif
$(info Enabled Compilers: $(bold)$(green)$(subst $(space),$(reset)$(comma)$(space)$(bold)$(green),$(compilers))$(reset).)
$(info Testing the following compilers & engines:)
$(info   * API: $(bold)$(green)${compilers_engines}$(reset),)
$(info Cargo features:)
$(info   * Compilers: `$(bold)$(green)${compiler_features}$(reset)`.)
$(info Rust version: $(bold)$(green)$(shell rustc --version)$(reset).)
$(info NodeJS version: $(bold)$(green)$(shell node --version)$(reset).)
$(info )
$(info )
$(info --------------)
$(info $(bold)$(green)RULE EXECUTION$(reset))
$(info --------------)
$(info )
$(info )

#####
#
# Configure `sed -i` for a cross-platform usage.
#
#####

SEDI ?=

ifeq ($(IS_DARWIN), 1)
	SEDI := "-i ''"
else ifeq ($(IS_LINUX), 1)
	SEDI := "-i"
endif

#####
#
# Building.
#
#####

bench:
	cargo bench $(compiler_features)

# For best results ensure the release profile looks like the following
# in Cargo.toml:
# [profile.release]
# opt-level = 'z'
# debug = false
# debug-assertions = false
# overflow-checks = false
# lto = true
# panic = 'abort'
# incremental = false
# codegen-units = 1
# rpath = false
ifeq ($(IS_DARWIN), 1)
	strip -u target/$(HOST_TARGET)/release/wasmer-headless
else ifeq ($(IS_WINDOWS), 1)
	strip --strip-unneeded target/$(HOST_TARGET)/release/wasmer-headless.exe
else
	strip --strip-unneeded target/$(HOST_TARGET)/release/wasmer-headless
endif

#####
#
# Testing.
#
#####

test:
	cargo test --release --all $(compiler_features)

#####
#
# Packaging.
#
#####

package-docs: build-docs
	mkdir -p "package/docs/crates"
	cp -R target/doc/ package/docs/crates
	echo '<meta http-equiv="refresh" content="0; url=crates/wasmer/index.html">' > package/docs/index.html
	echo '<meta http-equiv="refresh" content="0; url=wasmer/index.html">' > package/docs/crates/index.html

distribution: package
	cp LICENSE package/LICENSE
	cp ATTRIBUTIONS.md package/ATTRIBUTIONS
	mkdir -p dist
ifeq ($(IS_WINDOWS), 1)
	iscc scripts/windows-installer/wasmer.iss
	cp scripts/windows-installer/WasmerInstaller.exe dist/
endif
	tar -C package -zcvf wasmer.tar.gz bin lib include LICENSE ATTRIBUTIONS
	mv wasmer.tar.gz dist/

#####
#
# Installating (for Distros).
#
#####

DESTDIR ?= /usr/local

install: install-wasmer install-pkgconfig install-misc

install-wasmer:
	install -Dm755 target/release/wasmer $(DESTDIR)/bin/wasmer

install-misc:
	install -Dm644 LICENSE "$(DESTDIR)"/share/licenses/wasmer/LICENSE

install-pkgconfig:
	# Make sure WASMER_INSTALL_PREFIX is set during build
	unset WASMER_DIR; \
	if pc="$$(target/release/wasmer config --pkg-config 1>/dev/null 2>/dev/null)"; then \
		echo "$$pc" | install -Dm644 /dev/stdin "$(DESTDIR)"/lib/pkgconfig/wasmer.pc; \
	else \
		echo 1>&2 "WASMER_INSTALL_PREFIX was not set during build, not installing wasmer.pc"; \
	fi

install-wasmer-headless-minimal:
	install -Dm755 target/release/wasmer-headless $(DESTDIR)/bin/wasmer-headless

#####
#
# Miscellaneous.
#
#####

# Updates the spectests from the repo
update-testsuite:
	git subtree pull --prefix tests/wast/spec https://github.com/WebAssembly/testsuite.git master --squash

lint-packages: RUSTFLAGS += -D dead-code -D nonstandard-style -D unused-imports -D unused-mut -D unused-variables -D unused-unsafe -D unreachable-patterns -D bad-style -D improper-ctypes -D unused-allocation -D unused-comparisons -D while-true -D unconditional-recursion -D bare-trait-objects # TODO: add `-D missing-docs` # TODO: add `-D function_item_references` (not available on Rust 1.47, try when upgrading)
lint-packages:
	RUSTFLAGS="${RUSTFLAGS}" cargo clippy --all $(exclude_tests)
	RUSTFLAGS="${RUSTFLAGS}" cargo clippy --manifest-path fuzz/Cargo.toml $(compiler_features)

lint-formatting:
	cargo fmt --all -- --check
	cargo fmt --manifest-path fuzz/Cargo.toml -- --check

lint: lint-formatting lint-packages

install-local: package
	tar -C ~/.wasmer -zxvf wasmer.tar.gz
