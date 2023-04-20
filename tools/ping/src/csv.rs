use near_async::time;
use near_primitives::network::PeerId;
use near_primitives::types::AccountId;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::BufWriter;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub struct LatenciesCsv {
    out: BufWriter<File>,
}

const HEADER: &'static str = "timestamp,ID,latency\n";

impl LatenciesCsv {
    pub fn open<P: AsRef<Path>>(filename: P) -> io::Result<Self> {
        let mut f = OpenOptions::new().read(true).append(true).create(true).open(filename)?;

        let end = f.seek(SeekFrom::End(0))?;
        let start = f.seek(SeekFrom::Start(0))?;

        if end >= start + HEADER.len() as u64 {
            let mut buf = [0; HEADER.len()];
            f.read_exact(&mut buf)?;

            match std::str::from_utf8(&buf) {
                Ok(s) => {
                    if s != HEADER {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Does not appear to be a latencies CSV file",
                        ));
                    }
                }
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Does not appear to be a latencies CSV file. Contains non-UTF-8 data",
                    ))
                }
            }
        } else if end != start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Does not appear to be a latencies CSV file",
            ));
        }

        if end > start {
            f.seek(SeekFrom::End(-1))?;
            let mut buf = [0; 1];
            f.read_exact(&mut buf)?;
            let write_newline = match std::str::from_utf8(&buf) {
                Ok(s) => s != "\n",
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Does not appear to be a latencies CSV file. Contains non-UTF-8 data",
                    ))
                }
            };
            if write_newline {
                f.write_all("\n".as_bytes())?;
            }
        } else {
            f.write_all(HEADER.as_bytes())?;
        }

        Ok(Self { out: BufWriter::new(f) })
    }

    pub fn write(
        &mut self,
        peer_id: &PeerId,
        account_id: Option<&AccountId>,
        latency: time::Duration,
    ) -> io::Result<()> {
        let id = account_id.map_or_else(|| format!("{}", peer_id), |a| format!("{}", a));
        write!(
            self.out,
            "{:?},{},{}\n",
            chrono::offset::Utc::now(),
            id,
            latency.whole_microseconds()
        )?;
        Ok(())
    }

    pub fn write_timeout(
        &mut self,
        peer_id: &PeerId,
        account_id: Option<&AccountId>,
    ) -> io::Result<()> {
        let id = account_id.map_or_else(|| format!("{}", peer_id), |a| format!("{}", a));
        write!(self.out, "{:?},{},TIMEOUT\n", chrono::offset::Utc::now(), id)?;
        Ok(())
    }
}
