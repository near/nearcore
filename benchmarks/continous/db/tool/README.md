# Requirements

- `libpg`
- A `~/.pggass` file with an entry matching the db URL (see [dbprofile](./dbprofile)).

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
