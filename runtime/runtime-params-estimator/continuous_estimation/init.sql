CREATE TABLE gas_fee (
    date TEXT NOT NULL DEFAULT (datetime('now')),   -- when measurement has been taken
    name TEXT NOT NULL,                             -- enum variant
    gas REAL NOT NULL,                              -- gas cost
    wall_clock_time REAL,                           -- if time based estimation, the wall-clock time measured
    icount REAL,                                    -- if icount based estimation, the number of operations counted
    io_read REAL,                                   -- if icount based estimation, the number of IO read bytes counted
    io_write REAL,                                  -- if icount based estimation, the number of IO write bytes counted
    uncertain INTEGER NOT NULL DEFAULT 0,           -- set to 1 if the measurment has been marked as uncertain
    protocol_version INTEGER,                       -- if entry is based on a runtime conifguration for a specific protocol version
    commit_hash TEXT                                -- for estimations, which git commit this has been estimated on
);