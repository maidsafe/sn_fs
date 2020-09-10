use std::ffi::{OsStr, OsString};
use std::iter;
use time::Timespec; // unix specific.
use log::warn;

#[derive(Debug, Clone, PartialEq)]
pub struct FsInodeCommon {
    pub size: u64,
    pub mtime: Timespec,   // modified time. (file contents changed)
    pub ctime: Timespec,   // changed time.  (metadata changed)
    pub crtime: Timespec,  // created time.  
    pub links: u32,
}

// metadata for tree nodes of type dir/symlink that live under forest/root (not files)
#[derive(Debug, Clone, PartialEq)]
pub struct FsInodeDirectory {
    pub common: FsInodeCommon,
    pub name: OsString,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FsInodeSymlink {
    pub common: FsInodeCommon,
    pub name: OsString,
    pub symlink: OsString,
}

// metadata for tree nodes of type fileinode -- live under forest/fileinodes
#[derive(Debug, Clone, PartialEq)]
pub struct FsInodeFile {
    pub common: FsInodeCommon,
    // public $xorname;  for now, store data in content.
    pub content: Vec<u8>,
}

// metadata for tree nodes of type file that live under forest/root (not dirs/symlinks)
#[derive(Debug, Clone, PartialEq)]
pub struct FsRefFile {
    pub name: OsString,
    pub inode_id: u64, // for looking up FsInodeMeta under /inodes
}

// enum possible kinds of inode.
#[derive(Debug, Clone, Copy)]
pub enum DirentKind {
    File,
    Directory,
    Symlink,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FsMetadata {
    InodeDirectory(FsInodeDirectory),
    InodeSymlink(FsInodeSymlink),
    InodeFile(FsInodeFile),
    RefFile(FsRefFile),
    Empty,  // Used when inode has been moved to trash.
}

impl FsMetadata {
    pub fn name(&self) -> &OsStr {
        match self {
            Self::InodeDirectory(m) => &m.name,
            Self::InodeSymlink(m) => &m.name,
            Self::RefFile(m) => &m.name,
            _ => &OsStr::new(""),
        }
    }

    #[allow(dead_code)]    
    pub fn is_inode_directory(&self) -> bool {
        match self {
            Self::InodeDirectory(_) => true,
            _ => false,
        }
    }

    #[allow(dead_code)]    
    pub fn is_inode_symlink(&self) -> bool {
        match self {
            Self::InodeSymlink(_) => true,
            _ => false,
        }
    }

    #[allow(dead_code)]    
    pub fn is_inode_file(&self) -> bool {
        match self {
            Self::InodeFile(_) => true,
            _ => false,
        }
    }

    #[allow(dead_code)]    
    pub fn is_ref_file(&self) -> bool {
        match self {
            Self::RefFile(_) => true,
            _ => false,
        }
    }

    #[allow(dead_code)]    
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Empty => true,
            _ => false,
        }
    }

    pub fn dirent_kind(&self) -> Option<DirentKind> {
        match self {
            Self::InodeDirectory(_) => Some(DirentKind::Directory),
            Self::InodeSymlink(_) => Some(DirentKind::Symlink),
            Self::InodeFile(_) => Some(DirentKind::File),
            Self::RefFile(_) => Some(DirentKind::File),
            Self::Empty => None,
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            Self::InodeDirectory(m) => m.common.size,
            Self::InodeSymlink(m) => m.common.size,
            Self::InodeFile(m) => m.common.size,
            _ => 0,
        }
    }

    pub fn mtime(&self) -> Timespec {
        match self {
            Self::InodeDirectory(m) => m.common.mtime,
            Self::InodeSymlink(m) => m.common.mtime,
            Self::InodeFile(m) => m.common.mtime,
            _ => {
                warn!("mtime not supported for {:?}", self);
                time::empty_tm().to_timespec()
            },
        }
    }

    pub fn set_mtime(&mut self, ts: Timespec) {
        match self {
            Self::InodeDirectory(m) => { m.common.mtime = ts; },
            Self::InodeSymlink(m) => { m.common.mtime = ts; },
            Self::InodeFile(m) => { m.common.mtime = ts; },
            _ => {
                warn!("mtime not supported for {:?}", self);
            },
        }
    }

    pub fn ctime(&self) -> Timespec {
        match self {
            Self::InodeDirectory(m) => m.common.mtime,
            Self::InodeSymlink(m) => m.common.mtime,
            Self::InodeFile(m) => m.common.mtime,
            _ => {
                warn!("ctime not supported for {:?}", self);
                time::empty_tm().to_timespec()
            },
        }
    }

    pub fn set_ctime(&mut self, ts: Timespec) {
        match self {
            Self::InodeDirectory(m) => { m.common.ctime = ts; },
            Self::InodeSymlink(m) => { m.common.ctime = ts; },
            Self::InodeFile(m) => { m.common.ctime = ts; },
            _ => {
                warn!("ctime not supported for {:?}", self);
            },
        }
    }

    pub fn crtime(&self) -> Timespec {
        match self {
            Self::InodeDirectory(m) => m.common.crtime,
            Self::InodeSymlink(m) => m.common.crtime,
            Self::InodeFile(m) => m.common.crtime,
            _ => {
                warn!("crtime not supported for {:?}", self);
                time::empty_tm().to_timespec()
            },
        }
    }

    pub fn set_crtime(&mut self, ts: Timespec) {
        match self {
            Self::InodeDirectory(m) => { m.common.crtime = ts; },
            Self::InodeSymlink(m) => { m.common.crtime = ts; },
            Self::InodeFile(m) => { m.common.crtime = ts; },
            _ => {
                warn!("crtime not supported for {:?}", self);
            },
        }
    }

    pub fn set_size(&mut self, size: u64) {
        match self {
            Self::InodeDirectory(m) => m.common.size = size,
            Self::InodeSymlink(m) => m.common.size = size,
            Self::InodeFile(m) => m.common.size = size,
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
            Self::InodeDirectory(m) => m.name = name.to_os_string(),
            Self::InodeSymlink(m) => m.name = name.to_os_string(),
            Self::RefFile(m) => m.name = name.to_os_string(),
            _ => {}
        }
    }

    pub fn symlink(&self) -> &OsStr {
        match self {
            Self::InodeSymlink(m) => &m.symlink,
            _ => &OsStr::new(""),
        }
    }

    #[allow(dead_code)]    
    pub fn set_symlink(&mut self, link: &OsStr) {
        match self {
            Self::InodeSymlink(m) => { m.symlink = link.to_os_string(); },
            _ => {}
        }
    }

    pub fn links_dec(&mut self) -> u32 {
        match self {
            Self::InodeDirectory(m) => {
                m.common.links -= 1;
                m.common.links
            },
            Self::InodeSymlink(m) => {
                m.common.links -= 1;
                m.common.links
            },
            Self::InodeFile(m) => {
                m.common.links -= 1;
                m.common.links
            },
            _ => 0,
        }
    }

    pub fn links_inc(&mut self) -> u32 {
        match self {
            Self::InodeDirectory(m) => {
                warn!("Attempted to increment link count on InodeDirectory");
                m.common.links
            },
            Self::InodeSymlink(m) => {
                warn!("Attempted to increment link count on InodeSymlink");
                m.common.links
            },
            Self::InodeFile(m) => {
                m.common.links += 1;
                m.common.links
            },
            _ => {
                warn!("Attempted to increment link count on {:?}", self);
                0
            }
        }
    }

    pub fn links(&self) -> u32 {
        match self {
            Self::InodeDirectory(m) => {
                m.common.links
            },
            Self::InodeSymlink(m) => {
                m.common.links
            },
            Self::InodeFile(m) => {
                m.common.links
            },
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
