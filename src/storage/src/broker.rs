use rusqlite::Connection;
use types::BrokerUpdateReq;

pub struct Broker {
    broker_hosts: Vec<u8>,
    connect_interval: u64,
    keep_alive: u64,
    statistics_interval: u64,
    local_ips: Option<Vec<u8>>,
}

pub fn create_table(conn: Connection) -> rusqlite::Result<()> {
    conn.execute(
        "CREATE TABLE broker (
            broker_hosts        TEXT NOT NULL,
            connect_interval    INTEGER NOT NULL,
            keep_alive          INTEGER NOT NULL,
            statistics_interval INTEGER NOT NULL,
            local_ips           TEXT
        )",
        (), // empty list of parameters.
    )?;

    Ok(())
}

pub fn insert(conn: Connection, info: BrokerUpdateReq) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare("INSERT INTO person (name, data) VALUES (?1, ?2)")?;
    // stmt.execute(&["Joe", rusqlite::types::Null])?;
    Ok(())
}

pub fn read(conn: Connection) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare("SELECT name, data FROM person")?;
    let person_iter = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
    })?;

    for person in person_iter {
        let person = person?;
        println!("Found person {:?}", person);
    }

    Ok(())
}

pub fn update(conn: Connection) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE person SET name = ?1 WHERE name = ?2",
        rusqlite::params!["Bob", "Joe"],
    )?;

    Ok(())
}
