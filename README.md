## Setup

### Setup rust

```bash
$ curl https://sh.rustup.rs -sSf | sh
$ rustup component add clippy-preview
$ rustup component add rustfmt
```

### Setup git hooks

```bash
./scripts/setup_hooks.sh
```

### Setup rustfmt for your editor (optional)
Installation instructions [here](https://github.com/rust-lang-nursery/rustfmt#running-rustfmt-from-your-editor)

## Development

### Style and lints
We currently use [rustfmt](https://github.com/rust-lang-nursery/rustfmt)
and [clippy](https://github.com/rust-lang-nursery/rust-clippy) to enforce certain standards.
These checks are run automatically during CI builds, and in a `pre-commit`
hook. You can run do a clippy check with `./scripts/run_clippy.sh` and
automatically format your code with `cargo rustfmt --all`.
