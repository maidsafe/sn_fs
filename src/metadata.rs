use std::ffi::{OsStr, OsString};
use std::iter;

// metadata for tree nodes of type dir/symlink that live under forest/root (not files)
#[derive(Debug, Clone)]
pub struct FsInodeMeta {
    pub name: OsString,
    pub size: u64,
    pub ctime: u32,
    pub mtime: u32,
    pub kind: DirentKind,
    pub symlink: OsString, // does not apply to dir.  maybe should be in own struct.
}

impl FsInodeMeta {
    pub fn new(name: OsString, kind: DirentKind) -> Self {
        Self {
            name,
            size: 0,
            ctime: 0,
            mtime: 0,
            kind,
            symlink: OsString::default(),
        }
    }
}

// metadata for tree nodes of type file that live under forest/root (not dirs/symlinks)
#[derive(Debug, Clone)]
pub struct FsRefMeta {
    pub name: OsString,
    pub inode_id: u64, // for looking up FsInodeMeta under /inodes
}

// metadata for tree nodes of type fileinode -- live under forest/fileinodes
#[derive(Debug, Clone)]
pub struct FsInodeFileMeta {
    pub size: u64,
    pub ctime: u32,
    pub mtime: u32,
    pub links: usize,
    // public $xorname;  for now, store data in content.
    pub content: Vec<u8>,
}

// enum possible kinds of inode.
#[derive(Debug, Clone, Copy)]
pub enum DirentKind {
    File,
    Directory,
    Symlink,
}

#[derive(Debug, Clone)]
pub enum FsMetadata {
    InodeRegular(FsInodeMeta),
    InodeFile(FsInodeFileMeta),
    RefFile(FsRefMeta),
    Empty,
}

impl FsMetadata {
    pub fn name(&self) -> &OsStr {
        match self {
            Self::InodeRegular(m) => &m.name,
            Self::RefFile(m) => &m.name,
            _ => &OsStr::new(""),
        }
    }

    pub fn dirent_kind(&self) -> DirentKind {
        match self {
            Self::InodeRegular(m) => m.kind,
            _ => DirentKind::File,
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            Self::InodeRegular(m) => m.size,
            Self::InodeFile(m) => m.size,
            _ => 0,
        }
    }

    pub fn set_size(&mut self, size: u64) {
        match self {
            Self::InodeRegular(m) => m.size = size,
            Self::InodeFile(m) => m.size = size,
            _ => {}
        }
    }

    pub fn inode_id(&self) -> Option<u64> {
        match self {
            Self::RefFile(m) => Some(m.inode_id),
            _ => None,
        }
    }

    pub fn set_name(&mut self, name: &OsStr) {
        match self {
            Self::InodeRegular(m) => m.name = name.to_os_string(),
            Self::RefFile(m) => m.name = name.to_os_string(),
            _ => {}
        }
    }

    pub fn symlink(&self) -> &OsStr {
        match self {
            Self::InodeRegular(m) if matches!(m.kind, DirentKind::Symlink) => &m.symlink,
            _ => &OsStr::new(""),
        }
    }

    pub fn set_symlink(&mut self, link: &OsStr) {
        match self {
            Self::InodeRegular(m) if matches!(m.kind, DirentKind::Symlink) => {
                m.symlink = link.to_os_string()
            }
            _ => {}
        }
    }

    pub fn links_dec(&mut self) -> usize {
        match self {
            Self::InodeFile(m) => {
                m.links -= 1;
                m.links
            }
            _ => 0,
        }
    }

    pub fn links_inc(&mut self) -> usize {
        match self {
            Self::InodeFile(m) => {
                m.links += 1;
                m.links
            }
            _ => 0,
        }
    }

    /*
        pub fn set_content(&mut self, content: Vec<u8>) {
            match self {
                Self::InodeFile(m) => { m.content = content },
                _ => {},
            }
        }
    */

    pub fn update_content(&mut self, new_bytes: &[u8], offset: i64) {
        let meta = match self {
            Self::InodeFile(m) => m,
            _ => {
                return;
            }
        };

        let offset: usize = offset as usize;

        if offset >= meta.content.len() {
            // extend with zeroes until we are at least at offset
            meta.content
                .extend(iter::repeat(0).take(offset - meta.content.len()));
        }

        if offset + new_bytes.len() > meta.content.len() {
            meta.content.splice(offset.., new_bytes.iter().cloned());
        } else {
            meta.content
                .splice(offset..offset + new_bytes.len(), new_bytes.iter().cloned());
        }
        println!(
            "update(): len of new bytes is {}, total len is {}, offset was {}",
            new_bytes.len(),
            meta.content.len(),
            offset
        );
        // new_bytes.len() as u64
    }

    pub fn truncate_content(&mut self, size: u64) {
        let meta = match self {
            Self::InodeFile(m) => m,
            _ => {
                return;
            }
        };
        meta.content.truncate(size as usize);
    }

    pub fn content(&self) -> Option<&Vec<u8>> {
        match self {
            Self::InodeFile(m) => Some(&m.content),
            _ => None,
        }
    }
}
