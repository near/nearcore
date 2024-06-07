use std::env;

use diesel::pg::PgConnection;
use diesel::prelude::*;
use dotenvy::dotenv;

pub mod models;
pub mod schema;
use models::NewFtTransfer;

pub fn establish_connection() -> anyhow::Result<PgConnection> {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
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
