extern crate env_logger;
extern crate fuse;
extern crate libc;
extern crate time;

// Note: see for helpful description of inode fields and when/how to update them.
// https://man7.org/linux/man-pages/man7/inode.7.html

mod fs_tree_types;
mod metadata;
mod tree_replica;
mod sparse_buf;

//use log::{debug, warn, error};
use log::{debug, error};
//use nix::unistd::{getgid, getuid};
use std::sync::Arc;
use std::sync::Mutex;
use sparse_buf::SparseBuf;

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
    replica: Arc<Mutex<TreeReplica>>,
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
            replica: Arc::new(Mutex::new(TreeReplica::new())),
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
    //     let t = replica.state().tree();
    //     for child_id in t.children(&parent) {
    //         if let Some(node) = t.find(&child_id) {
    //             if node.metadata().name() == name {
    //                 matches.push( (child_id, &node);
    //             }
    //         }
    //     }
    //     matches
    // }

    fn child_by_name<'r>(&self, replica: &'r TreeReplica, parent: u64, name: &OsStr) -> Option<(u64, &'r FsTreeNode)> {
        let t = replica.state().tree();
        for child_id in t.children(&parent) {
            if let Some(node) = t.find(&child_id) {
                if node.metadata().name() == name {
                    return Some((child_id, &node));
                }
            }
        }
        None
    }

    fn new_opmove_new_child(&self, replica: &mut TreeReplica, parent: u64, metadata: FsMetadata) -> FsOpMove {
        let ts = replica.tick();
        let child = InoMerge::combine(*ts.actor_id(), ts.counter());
        // let child = self.new_ino();
        FsOpMove::new(ts, parent, metadata, child)
    }

    #[inline]
    fn new_opmove(&self, replica: &mut TreeReplica, parent: u64, metadata: FsMetadata, child: u64) -> FsOpMove {
        FsOpMove::new(replica.tick(), parent, metadata, child)
    }

    #[inline]
    fn now() -> Timespec {
        time::now().to_timespec()
    }

    #[inline]
    fn verify_directory<'r>(&self, replica: &'r TreeReplica, parent: u64) -> Option<&'r FsTreeNode> {
        match replica.state().tree().find(&parent) {
            Some(node) if node.metadata().is_inode_directory() => Some(node),
            _ => None,
        }
    }

    fn mk_file_attr(ino: u64, kind: FileType, meta: &FsMetadata) -> FileAttr {
        let d = FsInodePosix::default();
        let posix = meta.posix().unwrap_or(&d);

        FileAttr {
            ino,
            size: meta.size(),
            blocks: 1,
            atime: meta.crtime(),
            mtime: meta.mtime(),
            ctime: meta.ctime(),
            crtime: meta.crtime(),
            kind: kind,
            nlink: meta.links(),
            perm: posix.perm,
            uid: posix.uid,
            gid: posix.gid,
            flags: posix.flags,
            rdev: 0,
        }
    }
}

impl Filesystem for SafeFS {
    fn init(&mut self, req: &Request) -> Result<(), c_int> {
        let mut replica = self.replica.lock().unwrap();

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
                    uid: req.uid(),
                    gid: req.gid(),
                    flags: 0,
                })
            },
        });
        let op = self.new_opmove(&mut replica, Self::forest(), meta, Self::root());
        replica.apply_op(op);

        // self.replica = TreeReplica::new();
        Ok(())
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!("lookup -- parent: {},  name: {:?}", parent, name);
        let replica = self.replica.lock().unwrap();

        if let Some((child, node)) = self.child_by_name(&replica, parent, name) {
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
                if let Some(inode) = replica.state().tree().find(&inode_id) {
                    ino = inode_id;
                    meta = inode.metadata();
                }
            }

            let attr = Self::mk_file_attr(ino, kind, &meta);
            reply.entry(&TTL, &attr, 0);
        } else {
            reply.error(ENOENT);
        }
        // note:  we do not increment lookup count.
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("getattr -- ino: {}", ino);
        let replica = self.replica.lock().unwrap();

        if let Some(node) = replica.state().tree().find(&ino) {
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
            let attr = Self::mk_file_attr(ino, kind, &meta);
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
        let mut replica = self.replica.lock().unwrap();

        if let Some(node) = replica.state().tree().find(&ino) {

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
            let posix = meta.posix().cloned().unwrap_or_default();

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
                    posix.uid,
                    new_uid
                );
                if let Some(p) = meta.posix_mut() {
                    p.uid = new_uid;
                } else {
                    reply.error(EINVAL);
                    return;
                }                
            }

            if let Some(new_gid) = gid {
                debug!(
                    "setattr -- old_gid={:?}, new_gid={:?})",
                    posix.gid,
                    new_gid
                );
                if let Some(p) = meta.posix_mut() {
                    p.gid = new_gid;
                } else {
                    reply.error(EINVAL);
                    return;
                }                
            }

            if let Some(new_perm) = mode {
                debug!(
                    "setattr -- old_perm={:?}, new_perm={:?})",
                    posix.perm,
                    new_perm
                );
                if let Some(p) = meta.posix_mut() {
                    p.perm = new_perm as u16;
                } else {
                    reply.error(EINVAL);
                    return;
                }                
            }

            if let Some(new_flags) = flags {
                debug!(
                    "setattr -- old_flags={:?}, new_flags={:?})",
                    posix.flags,
                    new_flags
                );
                if let Some(p) = meta.posix_mut() {
                    p.flags = new_flags;
                } else {
                    reply.error(EINVAL);
                    return;
                }                
            }

            let attr = Self::mk_file_attr(ino, kind, &meta);

            // If metadata changed, we need to generate a Move op.
            if meta != *node.metadata() {
                let parent_id = *node.parent_id();
                let op = self.new_opmove(&mut replica, parent_id, meta, ino);
                replica.apply_op(op);
            }

            reply.attr(&TTL, &attr);
        } else {
            reply.error(ENOENT);
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("rmdir -- parent: {}, name: {:?}", parent, name);
        let mut replica = self.replica.lock().unwrap();

        // find child of parent that matches $name
        if let Some((ino, node)) = self.child_by_name(&replica, parent, name) {
            if !matches!(node.metadata().dirent_kind(), Some(DirentKind::Directory)) {
                reply.error(ENOENT);
                return;
            }

            let children = replica.state().tree().children(&ino);
            if !children.is_empty() {
                reply.error(ENOTEMPTY);
                return;
            }

            // Generate op to move dir node to trash.
            let op = self.new_opmove(&mut replica, Self::trash(), FsMetadata::Empty, ino);
            replica.apply_op(op);

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
        let mut replica = self.replica.lock().unwrap();

        if let Some(_node) = replica.state().tree().find(&parent) {
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
                        flags: 0,
                    })                    
                },
            });

            let op = self.new_opmove_new_child(&mut replica, parent, meta.clone());
            let ino = *op.child_id();

            replica.apply_op(op);

            let attr = Self::mk_file_attr(ino, FileType::Symlink, &meta);
            reply.entry(&TTL, &attr, 1);
        } else {
            reply.error(ENOENT);
        }
    }

    fn readlink(&mut self, _req: &Request, ino: u64, reply: ReplyData) {
        debug!("readlink -- ino: {}", ino);
        let replica = self.replica.lock().unwrap();

        // find parent dir (under /root/)
        if let Some(node) = replica.state().tree().find(&ino) {
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
        let mut replica = self.replica.lock().unwrap();

        let mut ops: Vec<FsOpMove> = Vec::new();

        // find child of parent that matches $name
        if let Some((child, node)) = self.child_by_name(&replica, parent, name) {
            let mut newmeta = node.metadata().clone();
            newmeta.set_name(newname);

            // If there is an existing node in target location, it is moved to trash.
            if let Some((old_ino, node)) = self.child_by_name(&replica, newparent, newname) {
                debug!("rename -- moving old `{:?}` to trash", newname);
                let meta = node.metadata().clone();
                ops.push(self.new_opmove(&mut replica, Self::trash(), meta, old_ino));
            }

            // move child to new location/name
            ops.push(self.new_opmove(&mut replica, newparent, newmeta, child));
            replica.apply_ops(&ops);

            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    fn mkdir(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry) {
        debug!("mkdir -- parent: {}, name: {:?}, mode: {}", parent, name, mode);
        let mut replica = self.replica.lock().unwrap();

        // check if parent exists and is a directory.
        if !self.verify_directory(&replica, parent).is_some() {
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
                    perm: mode as u16,
                    uid: req.uid(),
                    gid: req.gid(),
                    flags: 0,
                })                
            },
        });

        let op = self.new_opmove_new_child(&mut replica, parent, meta.clone());
        let ino = *op.child_id();

        replica.apply_op(op);

        let attr = Self::mk_file_attr(ino, FileType::Directory, &meta);
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
        let replica = self.replica.lock().unwrap();

        // find children after offset.
        // let to_skip = if offset == 0 { offset } else { offset + 1 } as usize;
        let children = replica.state().tree().children(&ino);
        for (i, child_ino) in children.iter().enumerate().skip(offset as usize) {
            if let Some(node) = replica.state().tree().find(child_ino) {
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
        let mut replica = self.replica.lock().unwrap();

        // check if newname entry already exists.  (return EEXIST)
        if self.child_by_name(&replica, newparent, newname).is_some() {
            reply.error(EEXIST);
            return;
        }

        let mut ops: Vec<FsOpMove> = Vec::new();

        if let Some(node) = replica.state().tree().find(&ino) {
            let mut meta = node.metadata().clone();
            meta.links_inc();

            let attr = Self::mk_file_attr(ino, FileType::RegularFile, &meta);

            let parent_id = *node.parent_id();
            ops.push(self.new_opmove(&mut replica, parent_id, meta, ino));

            let file_ref_meta = FsRefFile {
                name: newname.to_os_string(),
                inode_id: ino,
            };

            let meta_ref = FsMetadata::RefFile(file_ref_meta);
            let op_ref = self.new_opmove_new_child(&mut replica, newparent, meta_ref);
            // let _ref_id = *op_ref.child_id();
            ops.push(op_ref);

            replica.apply_ops(&ops);

            reply.entry(&TTL, &attr, 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("unlink -- parent: {}, name: {:?}", parent, name);
        let mut replica = self.replica.lock().unwrap();

        if let Some((child, node)) = self.child_by_name(&replica, parent, name) {
            let mut ops: Vec<FsOpMove> = Vec::new();

            match node.metadata() {
                FsMetadata::InodeSymlink(_) => {
                    ops.push(self.new_opmove(&mut replica, Self::trash(), FsMetadata::Empty, child));
                }
                FsMetadata::RefFile(m) => {
                    // move the inode reference to trash
                    let inode_id = m.inode_id;
                    ops.push(self.new_opmove(&mut replica, Self::trash(), FsMetadata::Empty, child));

                    // lookup the inode.
                    if let Some(inode) = replica.state().tree().find(&inode_id) {
                        let mut meta = inode.metadata().clone();
                        meta.set_ctime(Self::now());
                        let cnt = meta.links_dec();

                        if cnt > 0 {
                            // reference(s) still exist, so we need to update the inode's link count.
                            debug!("unlink -- links: {}, preserving inode {}", cnt, inode_id);
                            let parent_id = *inode.parent_id();
                            ops.push(self.new_opmove(&mut replica, parent_id, meta, inode_id));
                        } else {
                            // when link count has dropped to zero, move the inode to trash
                            // we must preserve the metadata because some process(es) may still
                            // have the file open for reading/writing.
                            debug!("unlink -- links: {}, removing inode {}.", cnt, inode_id);
                            ops.push(self.new_opmove(&mut replica, Self::trash(), meta, inode_id));
                        }
                    }
                }
                _ => {
                    reply.error(EINVAL);
                    return;
                }
            }

            replica.apply_ops(&ops);
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
        let mut replica = self.replica.lock().unwrap();

        // check if parent exists and is a directory.
        if !self.verify_directory(&replica, parent).is_some() {
            // not a directory
            reply.error(EINVAL);
            return;
        }

        // check if already existing
        if self.child_by_name(&replica, parent, name).is_some() {
            debug!("create -- already exists!  bailing out.");
            reply.error(EINVAL);
            return;
        }

        let mut ops: Vec<FsOpMove> = Vec::new();        

        // 3. create tree node under /inodes/<x>
        let file_inode_meta = FsInodeFile {
            content: SparseBuf::new(),
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
                    flags,
                })
            },
        };
        let meta = FsMetadata::InodeFile(file_inode_meta);

        let op = self.new_opmove_new_child(&mut replica, Self::fileinodes(), meta.clone());
        let inode_id = *op.child_id();

        ops.push(op);

        // 5. create tree entry under /root/../parent_id
        let file_ref_meta = FsRefFile {
            name: name.to_os_string(),
            inode_id,
        };

        let meta_ref = FsMetadata::RefFile(file_ref_meta);
        let op_ref = self.new_opmove_new_child(&mut replica, parent, meta_ref);
        let ref_id = *op_ref.child_id();
        ops.push(op_ref);

        replica.apply_ops(&ops);

        debug!("create -- ref_id={}, inode_id={}, name={:?}", ref_id, inode_id, name);

        let attr = Self::mk_file_attr(inode_id, FileType::RegularFile, &meta);
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
        let mut replica = self.replica.lock().unwrap();

        // find tree node from inode_id
        if let Some(inode) = replica.state().tree().find(&ino) {
            let mut meta = inode.metadata().clone();
            // debug!("write -- found metadata: {:?}", meta);
            let old_size = meta.size();
            meta.update_content(&data, offset as u64);
            let size = meta.content().unwrap().size as u64; // fixme: unwrap.
            if size > old_size {
                meta.set_size(size);
            }
            meta.set_mtime(Self::now());

            // Generate op for updating the tree_node metadata
            let parent_id = *inode.parent_id();
            let op = self.new_opmove(&mut replica, parent_id, meta, ino);

            replica.apply_op(op);
            replica.truncate_log();

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
        size: u32,
        reply: ReplyData,
    ) {
        debug!("read -- ino={}, offset={}, size={}", ino, offset, size);
        let replica = self.replica.lock().unwrap();

        if let Some(inode) = replica.state().tree().find(&ino) {
            if let Some(content) = inode.metadata().content() {
                let data = content.read(offset as u64, size as u64);
                reply.data(&data);
            } else {
                error!("read -- content not found in metadata");
                reply.error(ENOENT);
            }
        } else {
            error!("read -- inode not found");
            reply.error(ENOENT);
        }
    }
}

fn main() {
    env_logger::builder()
        .default_format_timestamp_nanos(true)
        .init();
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
