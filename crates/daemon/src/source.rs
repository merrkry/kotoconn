use kotoconn_script::ModuleSource;
use std::{
    io,
    path::{Path, PathBuf},
};

pub(crate) struct Files {
    root: PathBuf,
}

impl Files {
    pub fn open(path: &Path) -> io::Result<(String, Self)> {
        let path = path.canonicalize()?;
        let entry = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| io::Error::other("policy filename must be UTF-8"))?
            .to_owned();
        let root = path
            .parent()
            .ok_or_else(|| io::Error::other("policy must have a parent directory"))?
            .to_owned();
        Ok((entry, Self { root }))
    }
}

impl ModuleSource for Files {
    fn read(&mut self, name: &str) -> io::Result<String> {
        let path = self.root.join(name).canonicalize()?;
        if !path.starts_with(&self.root) {
            return Err(io::Error::other("module is outside the source root"));
        }
        std::fs::read_to_string(path)
    }
}
