use std::env;

use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::prelude::*;

pub mod models;
pub mod schema;
use models::NewFtTransfer;

pub fn establish_connection() -> anyhow::Result<PgConnection> {
    let database_url = env::var("DATABASE_URL_CLI")
        .expect("DATABASE_URL_CLI must be set. Consider sourcing the dbprofile file.");
    let connection = PgConnection::establish(&database_url)?;
    Ok(connection)
}

pub fn insert_ft_transfer(
    connection: &mut PgConnection,
    ft_transfer: &NewFtTransfer,
) -> anyhow::Result<()> {
    use crate::schema::ft_transfers;

    let num_inserted =
        diesel::insert_into(ft_transfers::table).values(ft_transfer).execute(connection)?;
    anyhow::ensure!(num_inserted == 1, "failed to insert ft_transfer");
    Ok(())
}

pub fn check_connection(connection: &mut PgConnection) -> anyhow::Result<()> {
    use crate::schema::ft_transfers::dsl::{ft_transfers, time};
    let _result = ft_transfers.select(time).limit(1).load::<DateTime<Utc>>(connection)?;
    Ok(())
}
