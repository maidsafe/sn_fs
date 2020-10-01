// Copyright 2020 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// http://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

use log::warn;
use std::ffi::{OsStr, OsString};
//use std::iter;
use time::Timespec; // unix specific.

// Note:  here is a useful article about FileSystem attributes
//        by OS:   https://en.wikipedia.org/wiki/File_attribute

/// Represents Inode attributes that are Posix specific.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FsInodePosix {
    pub perm: u16,
    pub uid: u32,
    pub gid: u32,
    pub flags: u32,
}

/// Represents Inode attributes that are Windows specific.
#[derive(Debug, Clone, PartialEq)]
pub struct FsInodeWindows {
    pub archive: bool,
    pub hidden: bool,
    pub system: bool,
    pub readonly: bool,
    pub compressed: bool,
    pub encrypted: bool,
    pub notcontentindexed: bool,
    pub reparsepoint: bool,
    pub notindexed: bool,
    pub offline: bool,
    pub sparse: bool,
    pub temporary: bool,
}

/// An enum for storing OS specific attributes, if any.
/// For example:
///  a file created on Linux, BSD or Mac would have Posix attributes.
///  a file created on Windows would have Windows attributes
///  a file created directly on the Safe network (eg via API) would not have any OS attributes.
#[derive(Debug, Clone, PartialEq)]
pub enum FsInodeOs {
    Posix(FsInodePosix),
    #[allow(dead_code)]
    Windows(FsInodeWindows),
    #[allow(dead_code)]
    Empty, // no OS specific attrs.
}

/// inode attributes that are common to symlinks, files, directories.
#[derive(Debug, Clone, PartialEq)]
pub struct FsInodeCommon {
    pub size: u64,
    pub mtime: Timespec,  // modified time. (file contents changed)
    pub ctime: Timespec,  // changed time.  (metadata changed)
    pub crtime: Timespec, // created time.
    pub links: u32,
    pub osattrs: FsInodeOs,
}

/// inode attributes for Directories
#[derive(Debug, Clone, PartialEq)]
pub struct FsInodeDirectory {
    pub common: FsInodeCommon,
    pub name: OsString,
}

/// inode attributes for Symlinks
#[derive(Debug, Clone, PartialEq)]
pub struct FsInodeSymlink {
    pub common: FsInodeCommon,
    pub name: OsString,
    pub symlink: OsString,
}

/// inode attributes for Files
#[derive(Debug, Clone, PartialEq)]
pub struct FsInodeFile {
    pub common: FsInodeCommon,
    // public xorname;   In the future, we expect to store file content in SafeNetwork, referenced by XorName.
    // pub content: SparseBuf,
}

/// inode attributes for File References.   (hard links)
#[derive(Debug, Clone, PartialEq)]
pub struct FsRefFile {
    pub name: OsString,
    pub inode_id: u64, // for looking up FsInodeFile under /inodefiles
}

/// enum possible kinds of inode visible to FUSE/OS.
#[derive(Debug, Clone, Copy)]
pub enum DirentKind {
    File,
    Directory,
    Symlink,
}

/// enum possible kinds of metadata that can be stored in a TreeNode.
/// In pther words, each node in the tree stores metadata of type FsMetadata.
#[derive(Debug, Clone, PartialEq)]
pub enum FsMetadata {
    InodeDirectory(FsInodeDirectory),
    InodeSymlink(FsInodeSymlink),
    InodeFile(FsInodeFile),
    RefFile(FsRefFile),
    Empty, // Used when inode has been moved to trash.  (symlinks and directores only)
}

impl FsInodeOs {
    pub fn posix(&self) -> Option<&FsInodePosix> {
        match self {
            Self::Posix(p) => Some(p),
            _ => None,
        }
    }

    pub fn posix_mut(&mut self) -> Option<&mut FsInodePosix> {
        match self {
            Self::Posix(p) => Some(p),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn windows(&mut self) -> Option<&mut FsInodeWindows> {
        match self {
            Self::Windows(w) => Some(w),
            _ => None,
        }
    }
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
            }
        }
    }

    pub fn set_mtime(&mut self, ts: Timespec) {
        match self {
            Self::InodeDirectory(m) => {
                m.common.mtime = ts;
            }
            Self::InodeSymlink(m) => {
                m.common.mtime = ts;
            }
            Self::InodeFile(m) => {
                m.common.mtime = ts;
            }
            _ => {
                warn!("mtime not supported for {:?}", self);
            }
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
            }
        }
    }

    pub fn set_ctime(&mut self, ts: Timespec) {
        match self {
            Self::InodeDirectory(m) => {
                m.common.ctime = ts;
            }
            Self::InodeSymlink(m) => {
                m.common.ctime = ts;
            }
            Self::InodeFile(m) => {
                m.common.ctime = ts;
            }
            _ => {
                warn!("ctime not supported for {:?}", self);
            }
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
            }
        }
    }

    pub fn set_crtime(&mut self, ts: Timespec) {
        match self {
            Self::InodeDirectory(m) => {
                m.common.crtime = ts;
            }
            Self::InodeSymlink(m) => {
                m.common.crtime = ts;
            }
            Self::InodeFile(m) => {
                m.common.crtime = ts;
            }
            _ => {
                warn!("crtime not supported for {:?}", self);
            }
        }
    }

    pub fn set_size(&mut self, size: u64) {
        match self {
            Self::InodeDirectory(m) => m.common.size = size,
            Self::InodeSymlink(m) => m.common.size = size,
            Self::InodeFile(m) => m.common.size = size,
            _ => {
                warn!("size not supported for {:?}", self);
            }
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
            _ => {
                warn!("name not supported for {:?}", self);
            }
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
        if let Self::InodeSymlink(m) = self {
            m.symlink = link.to_os_string();
        }
    }

    pub fn links_dec(&mut self) -> u32 {
        match self {
            Self::InodeDirectory(m) => {
                m.common.links -= 1;
                m.common.links
            }
            Self::InodeSymlink(m) => {
                m.common.links -= 1;
                m.common.links
            }
            Self::InodeFile(m) => {
                m.common.links -= 1;
                m.common.links
            }
            _ => {
                warn!("Attempted to decrement link count on {:?}", self);
                0
            }
        }
    }

    pub fn links_inc(&mut self) -> u32 {
        match self {
            Self::InodeDirectory(m) => {
                warn!("Attempted to increment link count on InodeDirectory");
                m.common.links
            }
            Self::InodeSymlink(m) => {
                warn!("Attempted to increment link count on InodeSymlink");
                m.common.links
            }
            Self::InodeFile(m) => {
                m.common.links += 1;
                m.common.links
            }
            _ => {
                warn!("Attempted to increment link count on {:?}", self);
                0
            }
        }
    }

    pub fn links(&self) -> u32 {
        match self {
            Self::InodeDirectory(m) => m.common.links,
            Self::InodeSymlink(m) => m.common.links,
            Self::InodeFile(m) => m.common.links,
            _ => {
                warn!("Attempted to read links on {:?}", self);
                0
            }
        }
    }

    pub fn posix(&self) -> Option<&FsInodePosix> {
        match self {
            Self::InodeFile(m) => m.common.osattrs.posix(),
            Self::InodeDirectory(m) => m.common.osattrs.posix(),
            Self::InodeSymlink(m) => m.common.osattrs.posix(),
            _ => None,
        }
    }

    pub fn posix_mut(&mut self) -> Option<&mut FsInodePosix> {
        match self {
            Self::InodeFile(m) => m.common.osattrs.posix_mut(),
            Self::InodeDirectory(m) => m.common.osattrs.posix_mut(),
            Self::InodeSymlink(m) => m.common.osattrs.posix_mut(),
            _ => None,
        }
    }
}
