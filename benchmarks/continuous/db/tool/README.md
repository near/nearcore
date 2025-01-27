# Requirements

<!-- cspell:ignore libpq pgpass -->

- An installation of [`libpq`](https://www.postgresql.org/docs/15/libpq.html).
  - The name of the package providing `libpq` differs across operating systems and package managers. For example on Ubuntu you can install `libpq-dev`.
- A `~/.pgpass` file with an entry matching the db URL (see [dbprofile](./dbprofile)). Wrong password file setup may lead to unintuitive error messages, therefore it is recommended to read [the docs](https://www.postgresql.org/docs/15/libpq-pgpass.html) to get the following two points right:
  - Format of password entries.
  - `.pgpass` file permissions.

## Running diesel commands

Additionally requires:

- An installation of the [`Diesel CLI`](https://diesel.rs/guides/getting-started.html).

# Usage

## CLI

```bash
# Display available commands with:
cargo run -p cli -- --help

# Show help for a specific <command>.
cargo run -p cli -- <command> --help
```

## Migrations

`diesel-cli` can be used from the [orm](./orm) directory to run migrations.

Generate the directories and files for a new migration with:

```
diesel migration generate <migration_name>
```

Write SQL in the generated `up.sql` and `down.sql`, then apply the migration with:

```
diesel migration run
```

Which executes the migration defined in `up.sql`. The file `down.sql` should contain SQL which reverts `up.sql`, to enable rolling the migration back, if needed.

Before running a migration, consider backing up the db in Cloud SQL to recover from a faulty migration.

More details can be found in Diesel's [getting started guide](https://diesel.rs/guides/getting-started).
