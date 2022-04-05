CREATE TABLE IF NOT EXISTS estimation (
    date TEXT NOT NULL DEFAULT (datetime('now')),   -- when measurement has been taken
    name TEXT NOT NULL,                             -- enum variant
    gas REAL NOT NULL,                              -- gas cost
    parameter TEXT,                                 -- parameter for which this estimation is used (may be null)
    wall_clock_time REAL,                           -- if time based estimation, the wall-clock time measured
    icount REAL,                                    -- if icount based estimation, the number of operations counted
    io_read REAL,                                   -- if icount based estimation, the number of IO read bytes counted
    io_write REAL,                                  -- if icount based estimation, the number of IO write bytes counted
    uncertain_reason TEXT DEFAULT NULL,             -- set to a non-null value explaining the reason, if the measurment has been marked as uncertain
    commit_hash TEXT NOT NULL                       -- which git commit this has been estimated on
);
CREATE TABLE IF NOT EXISTS parameter (
    name TEXT NOT NULL,                             -- parameter name as recorded in runtime_config.json
    gas REAL NOT NULL,                              -- gas cost
    protocol_version INTEGER                       -- protocol version for which the parameter is valid
);
