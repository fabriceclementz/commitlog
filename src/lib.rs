use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use byteorder::{BigEndian, WriteBytesExt};

const LEN_WIDTH: u8 = 8;

// TODO: add an mmaped inde

pub struct Config {
    segment: SegmentConfig,
}

struct SegmentConfig {
    max_bytes: u64,
}

pub struct Log {
    dir: PathBuf,
    active_segment: Segment,
    segments: Vec<Segment>,
    config: Config,
}

impl Log {
    pub fn new<P: Into<PathBuf>>(path: P, config: Config) -> io::Result<Self> {
        let dir = path.into();
        assert!(dir.is_dir());

        // TODO: load all segments

        let active_segment = Segment::new(&dir, 0)?;

        Ok(Self {
            dir,
            active_segment,
            segments: vec![],
            config,
        })
    }

    pub fn append(&mut self, data: &[u8]) -> Result<(), Box<dyn Error>> {
        self.active_segment.append(data)?;

        if self.active_segment_is_maxed() {
            self.roll_segment()?;
        }

        Ok(())
    }

    /// Creates a new segment and make it active.
    pub fn roll_segment(&mut self) -> io::Result<()> {
        // TODO: handle segment offset
        self.active_segment = Segment::new(&self.dir, 1)?;
        Ok(())
    }

    /// Returns whether the segment has reached its max size.
    /// The log uses this method to know it needs to create a new segment via roll_segment.
    fn active_segment_is_maxed(&self) -> bool {
        self.active_segment.size > self.config.segment.max_bytes
    }

    fn latest_segment(&self) -> io::Result<()> {
        // TODO: replace with a flatmap, filter on files and extension, remove expect
        let _latest_segment = fs::read_dir(&self.dir)?
            .map(|r| r.map(|entry| entry.path()))
            .collect::<Result<Vec<PathBuf>, _>>()?
            .iter()
            .max()
            .expect("no segments available");

        // TODO: maybe I should return a Segment or just the segment name? I'm not sure what I want to do for now?

        Ok(())
    }
}

struct Segment {
    path: PathBuf,
    writer: BufWriter<File>,
    pos: u64,
    size: u64,
}

impl Segment {
    fn new<P: Into<PathBuf>>(dir: P, offset: u64) -> io::Result<Self> {
        let filename = format!("{:08}.log", offset);
        let path = dir.into().join(filename);

        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .read(true)
            .open(&path)?;

        let size = file.metadata()?.len();
        let writer = BufWriter::new(file);

        Ok(Self {
            path,
            writer,
            pos: offset,
            size,
        })
    }

    fn append(&mut self, data: &[u8]) -> Result<(), Box<dyn Error>> {
        let encoded = rmp_serde::to_vec(data)?;
        let data_len = encoded.len();

        self.writer.write_u64::<BigEndian>(data_len as u64)?;
        let bytes_written = self.writer.write(&encoded)?;
        self.size += LEN_WIDTH as u64 + bytes_written as u64;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{thread::sleep, time::Duration};

    use tempdir::TempDir;

    use crate::{Config, Log, Segment, SegmentConfig};

    fn get_config() -> Config {
        Config {
            segment: SegmentConfig { max_bytes: 1024 },
        }
    }

    #[test]
    fn create_commit_log() {
        let dir = TempDir::new_in(".", "logs").expect("creating tempdir");
        // let path = dir.path().join("write.log");

        let mut log =
            Log::new(dir.as_ref().to_path_buf(), get_config()).expect("failed to build commit log");
        assert!(log.append(b"some data").is_ok());
        assert!(log.append(b"other data").is_ok());

        // sleep(Duration::from_secs(10));
    }

    #[test]
    fn roll_segment() {
        let dir = TempDir::new_in(".", "logs").expect("creating tempdir");

        let mut log =
            Log::new(dir.as_ref().to_path_buf(), get_config()).expect("failed to build commit log");
        assert_eq!(log.active_segment.path, dir.path().join("00000000.log"));

        log.roll_segment().expect("failed to roll segment");
        assert_eq!(log.active_segment.path, dir.path().join("00000001.log"));
    }
}
