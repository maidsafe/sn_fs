extern crate env_logger;
extern crate fuse;
extern crate libc;
extern crate time;

mod fs_tree_types;
mod metadata;
mod tree_replica;

use log::{debug, error};
use nix::unistd::{getgid, getuid};

use fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, Request,
};
use libc::{EINVAL, ENOENT, ENOTEMPTY, EEXIST};
use std::env;
use std::ffi::OsStr;
use std::os::raw::c_int;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use time::Timespec; // unix specific.

use fs_tree_types::{FsOpMove, FsTreeNode};
use metadata::{
    DirentKind, FsInodeCommon, FsInodeDirectory, FsInodeFile, FsInodeSymlink, FsMetadata, FsRefFile, FsInodeOs, FsInodePosix
};
use tree_replica::TreeReplica;

const TTL: Timespec = Timespec { sec: 1, nsec: 0 }; // 1 second

struct SafeFS {
    replica: TreeReplica,
}

struct InoMerge;

impl InoMerge {
    pub fn combine(a: u64, b: u64) -> u64 {
        a << 48 | b
    }
    // pub fn get_a(c: u64) -> u64 {
    //     c >> 48
    // }

    // pub fn get_b(c: u64) -> u64 {
    //     c & 0xFFFFFFFFFFFF
    // }
}

impl SafeFS {
    const FOREST: u64 = 0;
    const ROOT: u64 = 1;
    const FILEINODES: u64 = 2;
    const TRASH: u64 = 3;

    #[inline]
    fn new() -> Self {
        Self {
            replica: TreeReplica::new(),
        }
    }

    #[inline]
    fn forest() -> u64 {
        Self::FOREST
    }

    #[inline]
    fn root() -> u64 {
        Self::ROOT
    }

    #[inline]
    fn fileinodes() -> u64 {
        Self::FILEINODES
    }

    #[inline]
    fn trash() -> u64 {
        Self::TRASH
    }

    // Get child of a directory by name.
    // fn children_with_name(&self, parent: u64, name: &OsStr) -> Result<Vec<(u64, &FsTreeNode)>> {
    //     let mut matches = Vec<(u64, FsTreeNode)>;
    //     let t = self.replica.state().tree();
    //     for child_id in t.children(&parent) {
    //         if let Some(node) = t.find(&child_id) {
    //             if node.metadata().name() == name {
    //                 matches.push( (child_id, &node);
    //             }
    //         }
    //     }
    //     matches
    // }

    fn child_by_name(&self, parent: u64, name: &OsStr) -> Option<(u64, &FsTreeNode)> {
        let t = self.replica.state().tree();
        for child_id in t.children(&parent) {
            if let Some(node) = t.find(&child_id) {
                if node.metadata().name() == name {
                    return Some((child_id, &node));
                }
            }
        }
        None
    }

    fn new_opmove_new_child(&mut self, parent: u64, metadata: FsMetadata) -> FsOpMove {
        let ts = self.replica.tick();
        let child = InoMerge::combine(*ts.actor_id(), ts.counter());
        // let child = self.new_ino();
        FsOpMove::new(ts, parent, metadata, child)
    }

    #[inline]
    fn new_opmove(&mut self, parent: u64, metadata: FsMetadata, child: u64) -> FsOpMove {
        FsOpMove::new(self.replica.tick(), parent, metadata, child)
    }

    #[inline]
    fn now() -> Timespec {
        time::now().to_timespec()
    }

    #[inline]
    fn verify_directory(&self, parent: u64) -> Option<&FsTreeNode> {
        match self.replica.state().tree().find(&parent) {
            Some(node) if node.metadata().is_inode_directory() => Some(node),
            _ => None,
        }
    }
}

impl Filesystem for SafeFS {
    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        let meta = FsMetadata::InodeDirectory(FsInodeDirectory {
            name: OsStr::new("root").to_os_string(),
            common: FsInodeCommon {
                size: 0,
                links: 1,
                ctime: Self::now(),
                crtime: Self::now(),
                mtime: Self::now(),
                osattrs: FsInodeOs::Posix(FsInodePosix {
                    perm: 0o744,
                    uid: getuid().as_raw() as u32,
                    gid: getuid().as_raw() as u32,
                    rdev: 0,
                    flags: 0,
                })
            },
        });
        let op = self.new_opmove(Self::forest(), meta, Self::root());
        self.replica.apply_op(op);

        // self.replica = TreeReplica::new();
        Ok(())
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!("lookup -- parent: {},  name: {:?}", parent, name);

        if let Some((child, node)) = self.child_by_name(parent, name) {
            let kind: FileType = match node.metadata() {
                FsMetadata::InodeDirectory(_) => FileType::Directory,
                FsMetadata::InodeSymlink(_) => FileType::Symlink,
                FsMetadata::RefFile(_) => FileType::RegularFile,
                _ => {
                    reply.error(EINVAL);
                    return;
                }
            };

            let mut ino = child;
            let mut meta = node.metadata();

            if let Some(inode_id) = node.metadata().inode_id() {
                if let Some(inode) = self.replica.state().tree().find(&inode_id) {
                    ino = inode_id;
                    meta = inode.metadata();
                }
            }

            let attr = FileAttr {
                ino,
                size: meta.size(),
                blocks: 1,
                atime: meta.crtime(),
                mtime: meta.mtime(),
                ctime: meta.ctime(),
                crtime: meta.crtime(),
                kind,
                nlink: meta.links(),
                perm: meta.posix_perm().unwrap_or(0o644),
                uid: meta.posix_uid().unwrap_or(getuid().as_raw() as u32),
                gid: meta.posix_gid().unwrap_or(getgid().as_raw() as u32),
                rdev: meta.posix_rdev().unwrap_or(0),
                flags: meta.posix_flags().unwrap_or(0),
            };
            reply.entry(&TTL, &attr, 0);
        } else {
            reply.error(ENOENT);
        }
        // note:  we do not increment lookup count.
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("getattr -- ino: {}", ino);

        if let Some(node) = self.replica.state().tree().find(&ino) {
            let kind: FileType = match node.metadata().dirent_kind() {
                Some(DirentKind::Directory) => FileType::Directory,
                Some(DirentKind::File) => FileType::RegularFile,
                Some(DirentKind::Symlink) => FileType::Symlink,
                None => {
                    reply.error(EINVAL);
                    return;
                }
            };

            let meta = node.metadata();

            let attr = FileAttr {
                ino,
                size: meta.size(),
                blocks: 1,
                atime: meta.crtime(),
                mtime: meta.mtime(),
                ctime: meta.ctime(),
                crtime: meta.crtime(),
                kind,
                nlink: meta.links(),
                perm: meta.posix_perm().unwrap_or(0o644),
                uid: meta.posix_uid().unwrap_or(getuid().as_raw() as u32),
                gid: meta.posix_gid().unwrap_or(getgid().as_raw() as u32),
                rdev: meta.posix_rdev().unwrap_or(0),
                flags: meta.posix_flags().unwrap_or(0),
            };

            reply.attr(&TTL, &attr);
        } else {
            reply.error(ENOENT);
        }
    }

    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<Timespec>,
        mtime: Option<Timespec>,
        _fh: Option<u64>,
        crtime: Option<Timespec>,
        ctime: Option<Timespec>,
        _bkuptime: Option<Timespec>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        debug!(
            "setattr -- ino: {}, mode: {:?}, uid: {:?}, gid: {:?}, flags: {:?}",
            ino, mode, uid, gid, flags
        );

        if let Some(node) = self.replica.state().tree().find(&ino) {
            let kind: FileType = match node.metadata().dirent_kind() {
                Some(DirentKind::Directory) => FileType::Directory,
                Some(DirentKind::File) => FileType::RegularFile,
                Some(DirentKind::Symlink) => FileType::Symlink,
                None => {
                    reply.error(EINVAL);
                    return;
                }
            };

            let mut meta = node.metadata().clone();

            if let Some(new_mtime) = mtime {
                debug!(
                    "setattr -- old_mtime={:?}, new_mtime={:?})",
                    meta.mtime(),
                    new_mtime
                );
                meta.set_mtime(new_mtime);
            }
            if let Some(new_crtime) = crtime {
                debug!(
                    "setattr -- old_crtime={:?}, new_crtime={:?})",
                    meta.crtime(),
                    new_crtime
                );
                meta.set_crtime(new_crtime);
            }
            if let Some(new_ctime) = ctime {
                debug!(
                    "setattr -- old_ctime={:?}, new_ctime={:?})",
                    meta.ctime(),
                    new_ctime
                );
                meta.set_ctime(new_ctime);
            }

            if let Some(new_size) = size {
                if kind == FileType::RegularFile {
                    debug!(
                        "setattr -- old_size={}, new_size={})",
                        meta.size(),
                        new_size
                    );
                    meta.truncate_content(new_size);
                    meta.set_size(new_size);
                } else {
                    reply.error(EINVAL);
                    return;
                }
            }

            if let Some(new_uid) = uid {
                debug!(
                    "setattr -- old_uid={:?}, new_uid={:?})",
                    meta.posix_uid(),
                    new_uid
                );
                if let Some(p) = meta.posix() {
                    p.uid = new_uid;
                } else {
                    reply.error(EINVAL);
                    return;
                }                
            }

            if let Some(new_gid) = gid {
                debug!(
                    "setattr -- old_gid={:?}, new_gid={:?})",
                    meta.posix_gid(),
                    new_gid
                );
                if let Some(p) = meta.posix() {
                    p.gid = new_gid;
                } else {
                    reply.error(EINVAL);
                    return;
                }                
            }

            if let Some(new_perm) = mode {
                debug!(
                    "setattr -- old_perm={:?}, new_perm={:?})",
                    meta.posix_perm(),
                    new_perm
                );
                if let Some(p) = meta.posix() {
                    p.perm = new_perm as u16;
                } else {
                    reply.error(EINVAL);
                    return;
                }                
            }

            if let Some(new_flags) = flags {
                debug!(
                    "setattr -- old_flags={:?}, new_flags={:?})",
                    meta.posix_flags(),
                    new_flags
                );
                if let Some(p) = meta.posix() {
                    p.flags = new_flags;
                } else {
                    reply.error(EINVAL);
                    return;
                }                
            }

            let attr = FileAttr {
                ino,
                size: meta.size(),
                blocks: 1,
                atime: meta.crtime(),
                mtime: meta.mtime(),
                ctime: meta.ctime(),
                crtime: meta.crtime(),
                kind,
                nlink: meta.links(),
                perm: meta.posix_perm().unwrap_or(0o644),
                uid: meta.posix_uid().unwrap_or(getuid().as_raw() as u32),
                gid: meta.posix_gid().unwrap_or(getgid().as_raw() as u32),
                rdev: meta.posix_rdev().unwrap_or(0),
                flags: meta.posix_flags().unwrap_or(0),
            };

            // If metadata changed, we need to generate a Move op.
            if meta != *node.metadata() {
                let parent_id = *node.parent_id();
                let op = self.new_opmove(parent_id, meta, ino);
                self.replica.apply_op(op);
            }

            reply.attr(&TTL, &attr);
        } else {
            reply.error(ENOENT);
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("rmdir -- parent: {}, name: {:?}", parent, name);

        // find child of parent that matches $name
        if let Some((ino, node)) = self.child_by_name(parent, name) {
            if !matches!(node.metadata().dirent_kind(), Some(DirentKind::Directory)) {
                reply.error(ENOENT);
                return;
            }

            let children = self.replica.state().tree().children(&ino);
            if !children.is_empty() {
                reply.error(ENOTEMPTY);
                return;
            }

            // Generate op to move dir node to trash.
            let op = self.new_opmove(Self::trash(), FsMetadata::Empty, ino);
            self.replica.apply_op(op);

            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    fn symlink(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        link: &Path,
        reply: ReplyEntry,
    ) {
        debug!(
            "symlink -- parent: {}, name: {:?}, link: {}",
            parent,
            name,
            link.display()
        );

        if let Some(_node) = self.replica.state().tree().find(&parent) {
            let meta = FsMetadata::InodeSymlink(FsInodeSymlink {
                name: name.to_os_string(),
                symlink: link.as_os_str().to_os_string(),
                common: FsInodeCommon {
                    size: 0,
                    links: 1,
                    ctime: Self::now(),
                    crtime: Self::now(),
                    mtime: Self::now(),
                    osattrs: FsInodeOs::Posix(FsInodePosix {
                        perm: 0o777,
                        uid: req.uid(),
                        gid: req.gid(),
                        rdev: 0,
                        flags: 0,
                    })                    
                },
            });

            let op = self.new_opmove_new_child(parent, meta.clone());
            let ino = *op.child_id();

            let attr = FileAttr {
                ino,
                size: meta.size(),
                blocks: 1,
                atime: meta.crtime(),
                mtime: meta.mtime(),
                ctime: meta.ctime(),
                crtime: meta.crtime(),
                kind: FileType::Symlink,
                nlink: meta.links(),
                perm: meta.posix_perm().unwrap_or(0o644),
                uid: meta.posix_uid().unwrap_or(getuid().as_raw() as u32),
                gid: meta.posix_gid().unwrap_or(getgid().as_raw() as u32),
                rdev: meta.posix_rdev().unwrap_or(0),
                flags: meta.posix_flags().unwrap_or(0),
            };

            self.replica.apply_op(op);

            reply.entry(&TTL, &attr, 1);
        } else {
            reply.error(ENOENT);
        }
    }

    fn readlink(&mut self, _req: &Request, ino: u64, reply: ReplyData) {
        debug!("readlink -- ino: {}", ino);

        // find parent dir (under /root/)
        if let Some(node) = self.replica.state().tree().find(&ino) {
            // Ensure this node is a symlink
            if !matches!(node.metadata().dirent_kind(), Some(DirentKind::Symlink)) {
                debug!("readlink -- einval -- ino: {}", ino);
                reply.error(EINVAL);
                return;
            }

            debug!("readlink -- success -- ino: {}", ino);
            reply.data(node.metadata().symlink().as_bytes());
        } else {
            debug!("readlink -- enoent -- ino: {}", ino);
            reply.error(ENOENT);
        }
    }

    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEmpty,
    ) {
        debug!(
            "rename -- parent: {}, name: {:?}, newparent: {}, newname: {:?}",
            parent, name, newparent, newname
        );

        let mut ops: Vec<FsOpMove> = Vec::new();

        // find child of parent that matches $name
        if let Some((child, node)) = self.child_by_name(parent, name) {
            let mut newmeta = node.metadata().clone();
            newmeta.set_name(newname);

            // If there is an existing node in target location, it is moved to trash.
            if let Some((old_ino, node)) = self.child_by_name(newparent, newname) {
                debug!("rename -- moving old `{:?}` to trash", newname);
                let meta = node.metadata().clone();
                ops.push(self.new_opmove(Self::trash(), meta, old_ino));
            }

            // move child to new location/name
            ops.push(self.new_opmove(newparent, newmeta, child));
            self.replica.apply_ops(&ops);

            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    fn mkdir(&mut self, req: &Request, parent: u64, name: &OsStr, _mode: u32, reply: ReplyEntry) {

        // check if parent exists and is a directory.
        if !self.verify_directory(parent).is_some() {
            // not a directory
            reply.error(EINVAL);
            return;
        }

        // create tree node under /root/../parent_id
        let meta = FsMetadata::InodeDirectory(FsInodeDirectory {
            name: name.to_os_string(),
            common: FsInodeCommon {
                size: 0,
                links: 1,
                ctime: Self::now(),
                crtime: Self::now(),
                mtime: Self::now(),
                osattrs: FsInodeOs::Posix(FsInodePosix {
                    perm: 0o744,
                    uid: req.uid(),
                    gid: req.gid(),
                    rdev: 0,
                    flags: 0,
                })                
            },
        });

        let op = self.new_opmove_new_child(parent, meta.clone());
        let ino = *op.child_id();

        self.replica.apply_op(op);

        let attr = FileAttr {
            ino,
            size: meta.size(),
            blocks: 1,
            atime: meta.crtime(),
            mtime: meta.mtime(),
            ctime: meta.ctime(),
            crtime: meta.crtime(),
            kind: FileType::Directory,
            nlink: meta.links(),
            perm: meta.posix_perm().unwrap_or(0o644),
            uid: meta.posix_uid().unwrap_or(getuid().as_raw() as u32),
            gid: meta.posix_gid().unwrap_or(getgid().as_raw() as u32),
            rdev: meta.posix_rdev().unwrap_or(0),
            flags: meta.posix_flags().unwrap_or(0),
        };

        reply.entry(&TTL, &attr, 1);

        debug!(
            "mkdir {:?} completed. parent: {}, child: {}",
            name, parent, ino
        );
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!("entering readdir.  ino: {}, offset: {}", ino, offset);

        // find children after offset.
        // let to_skip = if offset == 0 { offset } else { offset + 1 } as usize;
        let children = self.replica.state().tree().children(&ino);
        for (i, child_ino) in children.iter().enumerate().skip(offset as usize) {
            if let Some(node) = self.replica.state().tree().find(child_ino) {
                let k = node.metadata().dirent_kind();

                debug!("meta: {:#?}, k: {:?}", node.metadata(), k);

                let filetype: FileType = match k {
                    Some(DirentKind::Directory) => FileType::Directory,
                    Some(DirentKind::File) => FileType::RegularFile,
                    Some(DirentKind::Symlink) => FileType::Symlink,
                    None => {
                        error!("Encountered unexpected DirentKind: {:?} in readdir", k);
                        reply.error(EINVAL);
                        return;
                    }
                };

                let next_ino = *child_ino;

                // ino, offset, filetype, name
                // reply.add returns true if it is full.
                if reply.add(next_ino, (i + 1) as i64, filetype, node.metadata().name()) {
                    break;
                }
                debug!(
                    "added child.  ino: {}, name: {:?}, filetype: {:?}",
                    next_ino,
                    node.metadata().name(),
                    filetype
                );
            }
        }
        reply.ok();

        debug!("leaving readdir.  ino: {}, offset: {}", ino, offset);
    }

    fn link(
        &mut self,
        _req: &Request,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        debug!(
            "link -- ino: {}, newparent: {}, newname: {:?}",
            ino, newparent, newname
        );

        // check if newname entry already exists.  (return EEXIST)
        if self.child_by_name(newparent, newname).is_some() {
            reply.error(EEXIST);
            return;
        }

        let mut ops: Vec<FsOpMove> = Vec::new();

        if let Some(node) = self.replica.state().tree().find(&ino) {
            let mut meta = node.metadata().clone();
            meta.links_inc();

            let attr = FileAttr {
                ino,
                size: meta.size(),
                blocks: 1,
                atime: meta.crtime(),
                mtime: meta.mtime(),
                ctime: meta.ctime(),
                crtime: meta.crtime(),
                kind: FileType::RegularFile,
                nlink: meta.links(),
                perm: meta.posix_perm().unwrap_or(0o644),
                uid: meta.posix_uid().unwrap_or(getuid().as_raw() as u32),
                gid: meta.posix_gid().unwrap_or(getgid().as_raw() as u32),
                rdev: meta.posix_rdev().unwrap_or(0),
                flags: meta.posix_flags().unwrap_or(0),
            };

            let parent_id = *node.parent_id();
            ops.push(self.new_opmove(parent_id, meta, ino));

            let file_ref_meta = FsRefFile {
                name: newname.to_os_string(),
                inode_id: ino,
            };

            let meta_ref = FsMetadata::RefFile(file_ref_meta);
            let op_ref = self.new_opmove_new_child(newparent, meta_ref);
            // let _ref_id = *op_ref.child_id();
            ops.push(op_ref);

            self.replica.apply_ops(&ops);

            reply.entry(&TTL, &attr, 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("unlink -- parent: {}, name: {:?}", parent, name);

        if let Some((child, node)) = self.child_by_name(parent, name) {
            let mut ops: Vec<FsOpMove> = Vec::new();

            match node.metadata() {
                FsMetadata::InodeSymlink(_) => {
                    ops.push(self.new_opmove(Self::trash(), FsMetadata::Empty, child));
                }
                FsMetadata::RefFile(m) => {
                    // move the inode reference to trash
                    let inode_id = m.inode_id;
                    ops.push(self.new_opmove(Self::trash(), FsMetadata::Empty, child));

                    // lookup the inode.
                    if let Some(inode) = self.replica.state().tree().find(&inode_id) {
                        let mut meta = inode.metadata().clone();
                        meta.set_ctime(Self::now());
                        let cnt = meta.links_dec();

                        if cnt > 0 {
                            // reference(s) still exist, so we need to update the inode's link count.
                            debug!("unlink -- links: {}, preserving inode {}", cnt, inode_id);
                            let parent_id = *inode.parent_id();
                            ops.push(self.new_opmove(parent_id, meta, inode_id));
                        } else {
                            // when link count has dropped to zero, move the inode to trash
                            // we must preserve the metadata because some process(es) may still
                            // have the file open for reading/writing.
                            debug!("unlink -- links: {}, removing inode {}.", cnt, inode_id);
                            ops.push(self.new_opmove(Self::trash(), meta, inode_id));
                        }
                    }
                }
                _ => {
                    reply.error(EINVAL);
                    return;
                }
            }

            self.replica.apply_ops(&ops);
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    fn create(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: u32,
        reply: ReplyCreate,
    ) {
        debug!(
            "create -- parent={}, name={:?}, mode={}, flags={})",
            parent, name, mode, flags
        );

        // check if parent exists and is a directory.
        if !self.verify_directory(parent).is_some() {
            // not a directory
            reply.error(EINVAL);
            return;
        }

        // check if already existing
        if self.child_by_name(parent, name).is_some() {
            debug!("create -- already exists!  bailing out.");
            reply.error(EINVAL);
            return;
        }

        let mut ops: Vec<FsOpMove> = Vec::new();        

        // 3. create tree node under /inodes/<x>
        let file_inode_meta = FsInodeFile {
            content: Vec::<u8>::new(),
            common: FsInodeCommon {
                size: 0,
                ctime: Self::now(),
                crtime: Self::now(),
                mtime: Self::now(),
                links: 1,
                osattrs: FsInodeOs::Posix(FsInodePosix {
                    perm: 0o644,
                    uid: req.uid(),
                    gid: req.gid(),
                    rdev: 0,
                    flags,
                })
            },
        };
        let meta = FsMetadata::InodeFile(file_inode_meta);

        let op = self.new_opmove_new_child(Self::fileinodes(), meta.clone());
        let inode_id = *op.child_id();

        ops.push(op);

        // 5. create tree entry under /root/../parent_id
        let file_ref_meta = FsRefFile {
            name: name.to_os_string(),
            inode_id,
        };

        let meta_ref = FsMetadata::RefFile(file_ref_meta);
        let op_ref = self.new_opmove_new_child(parent, meta_ref);
        // let ref_id = *op_ref.child_id();
        ops.push(op_ref);

        self.replica.apply_ops(&ops);

        let attr = FileAttr {
            ino: inode_id,
            size: meta.size(),
            blocks: 1,
            atime: meta.crtime(),
            mtime: meta.mtime(),
            ctime: meta.ctime(),
            crtime: meta.crtime(),
            kind: FileType::RegularFile,
            nlink: meta.links(),
            perm: meta.posix_perm().unwrap_or(0o644),
            uid: meta.posix_uid().unwrap_or(getuid().as_raw() as u32),
            gid: meta.posix_gid().unwrap_or(getgid().as_raw() as u32),
            rdev: meta.posix_rdev().unwrap_or(0),
            flags: meta.posix_flags().unwrap_or(0),
        };

        reply.created(&TTL, &attr, 1, 0 as u64, 0 as u32);
    }

    fn flush(&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        //edebug!("Filesystem::flush(ino={}, fh={})", ino, fh);

        reply.ok();
    }

    fn release(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        flush: bool,
        reply: ReplyEmpty,
    ) {
        debug!("release -- ino={}, fh={}, flush={}", ino, fh, flush);

        reply.ok();
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        flags: u32,
        reply: ReplyWrite,
    ) {
        debug!(
            "write -- ino={}, fh={}, offset={}, flags={}",
            ino, fh, offset, flags
        );

        // find tree node from inode_id
        if let Some(inode) = self.replica.state().tree().find(&ino) {
            let mut meta = inode.metadata().clone();
            debug!("write -- found metadata: {:?}", meta);
            meta.update_content(&data, offset);
            let size = meta.content().unwrap().len() as u64; // fixme: unwrap.
            meta.set_size(size);
            meta.set_mtime(Self::now());

            // Generate op for updating the tree_node metadata
            let parent_id = *inode.parent_id();
            let op = self.new_opmove(parent_id, meta, ino);

            self.replica.apply_op(op);

            reply.written(data.len() as u32);
        } else {
            reply.error(ENOENT);
        }
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        debug!("open -- ino={}, flags={}", ino, flags);
        reply.opened(0, 0);
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        _size: u32,
        reply: ReplyData,
    ) {
        if let Some(inode) = self.replica.state().tree().find(&ino) {
            if let Some(content) = inode.metadata().content().cloned() {
                reply.data(&content[offset as usize..]);
            } else {
                reply.error(ENOENT);
            }
        } else {
            reply.error(ENOENT);
        }
    }
}

fn main() {
    env_logger::init();
    let mountpoint = env::args_os().nth(1).unwrap();
    // Notes: 
    //  1. todo: these options should come from command line.
    //  2. allow_other enables other users to read/write.  Required for testing chown.
    //  3. allow_other requires that `user_allow_other` is in /etc/fuse.conf.
    //    let options = ["-o", "ro", "-o", "fsname=safefs"]    // -o ro = mount read only
    let options = ["-o", "fsname=safe_fs,allow_other"]
        .iter()
        .map(|o| o.as_ref())
        .collect::<Vec<&OsStr>>();
    fuse::mount(SafeFS::new(), &mountpoint, &options).unwrap();
}
