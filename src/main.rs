// Copyright 2020 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// http://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

extern crate env_logger;
extern crate fuse;
extern crate libc;
extern crate time;

/// sn_fs: A prototype FUSE filesystem that uses crdt_tree
/// for storing directory structure metadata.
///
/// This prototype operates only as a local filesystem.
/// The plan is to make it into a network filesystem
/// utilizing the CRDT properties to ensure that replicas
/// sync/converge correctly.
///
/// In this implementation the contents of each file are stored in
/// a corresponding file in the underlying filesystem whose name
/// is the inode identifier.  These inode content files are
/// all located in the mountpoint directory and are deleted when
/// sn_fs is unmounted.  Thus, they are never directly visible
/// to other processes.
///
/// In a networked implementation, the above mechanism could be used
/// as a method to implement a local cache.
// Note: see for helpful description of inode fields and when/how to update them.
// https://man7.org/linux/man-pages/man7/inode.7.html
mod fs_tree_types;
mod metadata;
mod tree_replica;

use log::{debug, error};
use openat::{Dir, SimpleType};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::sync::Mutex;

use fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, Request,
};
use libc::{mode_t, EEXIST, EINVAL, ENOENT, ENOTEMPTY};
use std::env;
use std::ffi::OsStr;
use std::os::raw::c_int;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use time::Timespec; // unix specific.

use fs_tree_types::{FsOpMove, FsTreeNode};
use metadata::{
    DirentKind, FsInodeCommon, FsInodeDirectory, FsInodeFile, FsInodeOs, FsInodePosix,
    FsInodeSymlink, FsMetadata, FsRefFile,
};
use tree_replica::TreeReplica;

const TTL: Timespec = Timespec { sec: 1, nsec: 0 }; // 1 second

struct SnFs {
    replica: Arc<Mutex<TreeReplica>>,
    mountpoint: Dir,
}

struct InoMerge;

/// Routines for merging 16 bit replica identifier with 48 bit inode id
/// into a single 64 bit integer.  (ino)
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

/// Some filesystem utility/helper methods.
impl SnFs {
    const FOREST: u64 = 0; // top-most node.  NOT in the tree.
    const ROOT: u64 = 1; // root of filesystem
    const FILEINODES: u64 = 2; // holds fileinodes
    const TRASH: u64 = 3; // holds deleted inodes

    const INO_FILE_PERM: mode_t = 0o600; // permissions for on-disk file-contents.

    #[inline]
    fn new(mountpoint: Dir) -> Self {
        Self {
            replica: Arc::new(Mutex::new(TreeReplica::new())),
            mountpoint,
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

    fn child_by_name<'r>(
        &self,
        replica: &'r TreeReplica,
        parent: u64,
        name: &OsStr,
    ) -> Option<(u64, &'r FsTreeNode)> {
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

    /// generate move operation that creates a new child node.
    fn new_opmove_new_child(
        &self,
        replica: &mut TreeReplica,
        parent: u64,
        metadata: FsMetadata,
    ) -> FsOpMove {
        let ts = replica.tick();
        let child = InoMerge::combine(*ts.actor_id(), ts.counter());
        // let child = self.new_ino();
        FsOpMove::new(ts, parent, metadata, child)
    }

    /// generate move operation that moves an existing node.
    #[inline]
    fn new_opmove(
        &self,
        replica: &mut TreeReplica,
        parent: u64,
        metadata: FsMetadata,
        child: u64,
    ) -> FsOpMove {
        FsOpMove::new(replica.tick(), parent, metadata, child)
    }

    #[inline]
    fn now() -> Timespec {
        time::now().to_timespec()
    }

    // Verify that a given node in the tree is a directory, according to its inode metadata.
    #[inline]
    fn verify_directory<'r>(
        &self,
        replica: &'r TreeReplica,
        parent: u64,
    ) -> Option<&'r FsTreeNode> {
        match replica.state().tree().find(&parent) {
            Some(node) if node.metadata().is_inode_directory() => Some(node),
            _ => None,
        }
    }

    // Create a FileAttr from ino, FileType, and FsMetadata
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
            kind,
            nlink: meta.links(),
            perm: posix.perm,
            uid: posix.uid,
            gid: posix.gid,
            flags: posix.flags,
            rdev: 0,
        }
    }
}

/// Here we implement the Fuse FileSystem trait/api.
impl Filesystem for SnFs {
    /// Initialize filesystem. Called before any other filesystem method.
    fn init(&mut self, req: &Request) -> Result<(), c_int> {
        let mut replica = self.replica.lock().unwrap();

        // Create metadata for root directory tree node
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
                }),
            },
        });
        // create and apply move operation for root node with parent forest.
        let op = self.new_opmove(&mut replica, Self::forest(), meta, Self::root());
        replica.apply_op(op);

        Ok(())
    }

    /// Clean up filesystem. Called on filesystem exit.
    fn destroy(&mut self, _req: &Request) {
        println!("destroy called");

        // note: destroy doesn't seem to get called.
        // filed issue: https://github.com/zargony/fuse-rs/issues/151
        // for now, moved code into main().
    }

    /// Look up a directory entry by name and get its attributes.
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!("lookup -- parent: {},  name: {:?}", parent, name);
        let replica = self.replica.lock().unwrap();

        // Find tree node under parent matching name.
        if let Some((child, node)) = self.child_by_name(&replica, parent, name) {
            // Determine/validate the type of FS inode.
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

            // if inode_id() is_some() then this is a RefFile and we need to
            // dereference it to find the real File Inode.
            if let Some(inode_id) = node.metadata().inode_id() {
                if let Some(inode) = replica.state().tree().find(&inode_id) {
                    ino = inode_id;
                    meta = inode.metadata();
                }
            }

            // Generate FileAttr and reply
            let attr = Self::mk_file_attr(ino, kind, &meta);
            reply.entry(&TTL, &attr, 0);
        } else {
            reply.error(ENOENT);
        }
        // note:  we do not increment lookup count.  It is optional.
    }

    /// Get file attributes.
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("getattr -- ino: {}", ino);
        let replica = self.replica.lock().unwrap();

        // Find node in tree
        if let Some(node) = replica.state().tree().find(&ino) {
            // Determine/validate the type of FS inode.
            // Fixme: should distinguish between FsMetadata::RefFile and FsMetadata::InodeFile.
            //        as lookup() does.  We seem to get away without it because lookup already
            //        returns the InodeFile ino, so we are passed it here.
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

    /// Set file attributes.
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

        // Find node in tree
        if let Some(node) = replica.state().tree().find(&ino) {
            // Determine/validate the type of FS inode.
            // fixme comment in getattr() applies here also.
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

            // Now we go through each parameter and check for any
            // updates.

            // mtime
            if let Some(new_mtime) = mtime {
                debug!(
                    "setattr -- old_mtime={:?}, new_mtime={:?})",
                    meta.mtime(),
                    new_mtime
                );
                meta.set_mtime(new_mtime);
            }
            // crtime
            if let Some(new_crtime) = crtime {
                debug!(
                    "setattr -- old_crtime={:?}, new_crtime={:?})",
                    meta.crtime(),
                    new_crtime
                );
                meta.set_crtime(new_crtime);
            }
            // ctime
            if let Some(new_ctime) = ctime {
                debug!(
                    "setattr -- old_ctime={:?}, new_ctime={:?})",
                    meta.ctime(),
                    new_ctime
                );
                meta.set_ctime(new_ctime);
            }

            // size
            if let Some(new_size) = size {
                if kind == FileType::RegularFile {
                    debug!(
                        "setattr -- old_size={}, new_size={})",
                        meta.size(),
                        new_size
                    );
                    // update content on disk.
                    let file = match self
                        .mountpoint
                        .update_file(&ino.to_string(), Self::INO_FILE_PERM)
                    {
                        Ok(f) => f,
                        Err(_) => {
                            reply.error(EINVAL);
                            return;
                        }
                    };
                    if file.set_len(new_size).is_err() {
                        reply.error(EINVAL);
                        return;
                    }
                    meta.set_size(new_size);
                } else {
                    reply.error(EINVAL);
                    return;
                }
            }

            // uid
            if let Some(new_uid) = uid {
                debug!("setattr -- old_uid={:?}, new_uid={:?})", posix.uid, new_uid);
                if let Some(p) = meta.posix_mut() {
                    p.uid = new_uid;
                } else {
                    reply.error(EINVAL);
                    return;
                }
            }

            // gid
            if let Some(new_gid) = gid {
                debug!("setattr -- old_gid={:?}, new_gid={:?})", posix.gid, new_gid);
                if let Some(p) = meta.posix_mut() {
                    p.gid = new_gid;
                } else {
                    reply.error(EINVAL);
                    return;
                }
            }

            // perm
            if let Some(new_perm) = mode {
                debug!(
                    "setattr -- old_perm={:?}, new_perm={:?})",
                    posix.perm, new_perm
                );
                if let Some(p) = meta.posix_mut() {
                    p.perm = new_perm as u16;
                } else {
                    reply.error(EINVAL);
                    return;
                }
            }

            // flags
            if let Some(new_flags) = flags {
                debug!(
                    "setattr -- old_flags={:?}, new_flags={:?})",
                    posix.flags, new_flags
                );
                if let Some(p) = meta.posix_mut() {
                    p.flags = new_flags;
                } else {
                    reply.error(EINVAL);
                    return;
                }
            }

            // create FileAttr for reply
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

    /// Remove a directory.
    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("rmdir -- parent: {}, name: {:?}", parent, name);
        let mut replica = self.replica.lock().unwrap();

        // find child of parent that matches name
        if let Some((ino, node)) = self.child_by_name(&replica, parent, name) {
            // must be a directory.
            if !matches!(node.metadata().dirent_kind(), Some(DirentKind::Directory)) {
                reply.error(ENOENT);
                return;
            }

            // directory must be empty.
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

    /// Create a symbolic link.
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

        // find parent node
        if let Some(_node) = replica.state().tree().find(&parent) {
            // note: we do not need to check if name entry already exists
            //       because fuse does it and doesn't call symlink in that case.

            // create node metadata for new symlink
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
                    }),
                },
            });

            // create new child node for symlink, generate move op, and apply it.
            let op = self.new_opmove_new_child(&mut replica, parent, meta.clone());
            let ino = *op.child_id();

            replica.apply_op(op);

            // generate FileAttr and reply with it.
            let attr = Self::mk_file_attr(ino, FileType::Symlink, &meta);
            reply.entry(&TTL, &attr, 1);
        } else {
            reply.error(ENOENT);
        }
    }

    /// Read symbolic link.
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

    /// Rename a file.
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

        // we may require two ops, so we make a list.
        let mut ops: Vec<FsOpMove> = Vec::with_capacity(2);

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

    /// Create a directory.
    fn mkdir(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry) {
        debug!(
            "mkdir -- parent: {}, name: {:?}, mode: {}",
            parent, name, mode
        );
        let mut replica = self.replica.lock().unwrap();

        // check if parent exists and is a directory.
        if self.verify_directory(&replica, parent).is_none() {
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
                }),
            },
        });

        // create child node, generate move op, and apply.
        let op = self.new_opmove_new_child(&mut replica, parent, meta.clone());
        let ino = *op.child_id();

        replica.apply_op(op);

        // generate FileAttr and reply with it.
        let attr = Self::mk_file_attr(ino, FileType::Directory, &meta);
        reply.entry(&TTL, &attr, 1);

        debug!(
            "mkdir {:?} completed. parent: {}, child: {}",
            name, parent, ino
        );
    }

    /// Read directory. Send a buffer filled using buffer.fill(), with size not exceeding
    /// the requested size. Send an empty buffer on end of stream. fh will contain the
    /// value set by the opendir method, or will be undefined if the opendir method didn't
    /// set any value.
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
        let children = replica.state().tree().children(&ino);
        for (i, child_ino) in children.iter().enumerate().skip(offset as usize) {
            if let Some(node) = replica.state().tree().find(child_ino) {
                let k = node.metadata().dirent_kind();

                debug!("meta: {:#?}, k: {:?}", node.metadata(), k);

                // determine filetype of child and validate.
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

    /// Create a hard link.
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

        if let Some(node) = replica.state().tree().find(&ino) {
            // We will have two ops.
            let mut ops: Vec<FsOpMove> = Vec::with_capacity(2);

            let mut meta = node.metadata().clone();
            meta.links_inc();

            let attr = Self::mk_file_attr(ino, FileType::RegularFile, &meta);

            // generate op that increments link count in InodeFile
            let parent_id = *node.parent_id();
            ops.push(self.new_opmove(&mut replica, parent_id, meta, ino));

            let file_ref_meta = FsRefFile {
                name: newname.to_os_string(),
                inode_id: ino,
            };

            // generate op that creates a new RefFile (hard link) to existing InodeFile
            let meta_ref = FsMetadata::RefFile(file_ref_meta);
            let op_ref = self.new_opmove_new_child(&mut replica, newparent, meta_ref);
            ops.push(op_ref);

            // apply both ops
            replica.apply_ops(&ops);

            reply.entry(&TTL, &attr, 0);
        } else {
            reply.error(ENOENT);
        }
    }

    /// Remove a file.
    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("unlink -- parent: {}, name: {:?}", parent, name);
        let mut replica = self.replica.lock().unwrap();

        // lookup child by name
        if let Some((child, node)) = self.child_by_name(&replica, parent, name) {
            // we may need 0, 1 or 2 ops.
            let mut ops: Vec<FsOpMove> = Vec::with_capacity(2);

            match node.metadata() {
                FsMetadata::InodeSymlink(_) => {
                    ops.push(self.new_opmove(
                        &mut replica,
                        Self::trash(),
                        FsMetadata::Empty,
                        child,
                    ));
                }
                FsMetadata::RefFile(m) => {
                    // move the inode reference to trash
                    let inode_id = m.inode_id;
                    ops.push(self.new_opmove(
                        &mut replica,
                        Self::trash(),
                        FsMetadata::Empty,
                        child,
                    ));

                    // lookup the inode. (dereference RefFile hard link)
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

            // apply ops.
            replica.apply_ops(&ops);
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    /// Create and open a file. If the file does not exist, first create it with the specified mode,
    /// and then open it. Open flags (with the exception of O_NOCTTY) are available in flags.
    /// Filesystem may store an arbitrary file handle (pointer, index, etc) in fh, and use
    /// this in other all other file operations (read, write, flush, release, fsync). There
    /// are also some flags (direct_io, keep_cache) which the filesystem may set, to change
    /// the way the file is opened. See fuse_file_info structure in for more details. If this
    /// method is not implemented or under Linux kernel versions earlier than 2.6.15, the
    /// mknod() and open() methods will be called instead.
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
        if self.verify_directory(&replica, parent).is_none() {
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

        // we will use two ops.
        let mut ops: Vec<FsOpMove> = Vec::with_capacity(2);

        // create tree node under /inodes/<x>
        let file_inode_meta = FsInodeFile {
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
                }),
            },
        };
        let meta = FsMetadata::InodeFile(file_inode_meta);

        let op = self.new_opmove_new_child(&mut replica, Self::fileinodes(), meta.clone());
        let inode_id = *op.child_id();

        // create file on disk.
        if self
            .mountpoint
            .write_file(&inode_id.to_string(), Self::INO_FILE_PERM)
            .is_err()
        {
            reply.error(EINVAL);
            return;
        };

        ops.push(op);

        // create tree entry under /root/../parent_id
        let file_ref_meta = FsRefFile {
            name: name.to_os_string(),
            inode_id,
        };

        // Here we create the RefFile hard link to the InodeFile created above.
        let meta_ref = FsMetadata::RefFile(file_ref_meta);
        let op_ref = self.new_opmove_new_child(&mut replica, parent, meta_ref);
        let ref_id = *op_ref.child_id();
        ops.push(op_ref);

        // apply operations.
        replica.apply_ops(&ops);

        debug!(
            "create -- ref_id={}, inode_id={}, name={:?}",
            ref_id, inode_id, name
        );

        // generate FileAttr and reply.
        let attr = Self::mk_file_attr(inode_id, FileType::RegularFile, &meta);
        reply.created(&TTL, &attr, 1, 0 as u64, 0 as u32);
    }

    /// Flush method. This is called on each close() of the opened file. Since file descriptors
    /// can be duplicated (dup, dup2, fork), for one open call there may be many flush calls.
    /// Filesystems shouldn't assume that flush will always be called after some writes, or
    /// that if will be called at all. fh will contain the value set by the open method, or
    /// will be undefined if the open method didn't set any value. NOTE: the name of the method
    /// is misleading, since (unlike fsync) the filesystem is not forced to flush pending writes.
    /// One reason to flush data, is if the filesystem wants to return write errors. If the
    /// filesystem supports file locking operations (setlk, getlk) it should remove all locks
    /// belonging to 'lock_owner'.
    fn flush(&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        //edebug!("Filesystem::flush(ino={}, fh={})", ino, fh);

        // Presently we are stateless, so we don't implement this.
        reply.ok();
    }

    /// Release an open file. Release is called when there are no more references to an open file:
    /// all file descriptors are closed and all memory mappings are unmapped. For every open call
    /// there will be exactly one release call. The filesystem may reply with an error, but error
    /// values are not returned to close() or munmap() which triggered the release. fh will contain
    /// the value set by the open method, or will be undefined if the open method didn't set any
    /// value. flags will contain the same flags as for open.
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

        // Presently we are stateless, so we don't implement this.
        reply.ok();
    }

    /// Write data. Write should return exactly the number of bytes requested except on error.
    /// An exception to this is when the file has been opened in 'direct_io' mode, in which
    /// case the return value of the write system call will reflect the return value of this
    /// operation. fh will contain the value set by the open method, or will be undefined if
    /// the open method didn't set any value.
    ///
    /// Our approach here is to open (or create) a file in the underlying filesystem
    /// beneath the mountpoint directory.  The ino's string representation is used as
    /// the file's name.  We then resize the file as needed, and seek to appropriate
    /// position for writing.
    ///
    /// This impl is relatively simple and easy because it is stateless. The file is re-opened
    /// and closed for each call to write() or read().
    ///
    /// TODO: An improvement would be to actually create the on-disk file in create() or open()
    /// it in open and pass the filehandle around, so that write() and read() do not need to open
    /// and close.  This should be considerably faster.
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

            // verify we are an InodeFile
            if !meta.is_inode_file() {
                reply.error(EINVAL);
                return;
            }

            debug!("write -- found metadata: {:?}", meta);
            let old_size = meta.size();

            // open "real" file on disk for update (or create)
            let mut file = match self
                .mountpoint
                .update_file(&ino.to_string(), Self::INO_FILE_PERM)
            {
                Ok(f) => f,
                Err(e) => {
                    error!("write -- can't open passthrough file {}: {:?}", ino, e);
                    reply.error(EINVAL);
                    return;
                }
            };
            if file.seek(SeekFrom::Start(offset as u64)).is_err() || file.write(data).is_err() {
                reply.error(EINVAL);
                return;
            }

            let size = match file.metadata() {
                Ok(m) => m.len(),
                Err(_) => {
                    reply.error(EINVAL);
                    return;
                }
            };
            // increase filesize if necessary.
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

    /// Open a file. Open flags (with the exception of O_CREAT, O_EXCL, O_NOCTTY and O_TRUNC)
    /// are available in flags. Filesystem may store an arbitrary file handle (pointer, index, etc)
    /// in fh, and use this in other all other file operations (read, write, flush, release, fsync).
    /// Filesystem may also implement stateless file I/O and not store anything in fh. There are
    /// also some flags (direct_io, keep_cache) which the filesystem may set, to change the way
    /// the file is opened. See fuse_file_info structure in for more details.
    fn open(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        debug!("open -- ino={}, flags={}", ino, flags);

        // Presently we are stateless, so we don't implement this.
        reply.opened(0, 0);
    }

    /// Read data. Read should send exactly the number of bytes requested except on EOF or error,
    /// otherwise the rest of the data will be substituted with zeroes. An exception to this is when
    /// the file has been opened in 'direct_io' mode, in which case the return value of the read
    /// system call will reflect the return value of this operation. fh will contain the value set
    /// by the open method, or will be undefined if the open method didn't set any value.
    ///
    /// Our approach here is to open a file in the underlying filesystem
    /// beneath the mountpoint directory.  The ino's string representation is used as
    /// the file's name.  We then seek to offset, read data, and close the file.
    ///
    /// This impl is relatively simple and easy because it is stateless. The file is re-opened
    /// and closed for each call to write() or read().
    ///
    /// TODO: An improvement would be to actually create the on-disk file in create() or open()
    /// it in open and pass the filehandle around, so that write() and read() do not need to open
    /// and close.  This should be considerably faster.
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
            let meta = inode.metadata();

            // verify node is a InodeFile
            if !meta.is_inode_file() {
                reply.error(EINVAL);
                return;
            }

            // open file
            let mut file = match self.mountpoint.open_file(&ino.to_string()) {
                Ok(f) => f,
                Err(e) => {
                    error!("read -- can't open passthrough file {}: {:?}", ino, e);
                    reply.error(EINVAL);
                    return;
                }
            };

            // seek to position
            if file.seek(SeekFrom::Start(offset as u64)).is_err() {
                reply.error(EINVAL);
                return;
            }

            // read data and return it.
            let mut buf = vec![0; size as usize];
            let result = file.read(&mut buf);

            if let Ok(_bytes_read) = result {
                reply.data(&buf);
            } else {
                error!("read -- content not found");
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
    let mountpoint = match env::args_os().nth(1) {
        Some(v) => v,
        None => {
            print_usage();
            return;
        }
    };

    // We use Dir::open() to get access to the mountpoint directory
    // before the mount occurs.  This handle enables us to later create/write/read
    // "real" files beneath the mountpoint even though other processes will only
    // see the filesystem view that our SnFs provides.
    let mountpoint_fd = match Dir::open(Path::new(&mountpoint)) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Unable to open {:?}.  {:?}", mountpoint, e);
            return;
        }
    };

    // Notes:
    //  1. todo: these options should come from command line.
    //  2. allow_other enables other users to read/write.  Required for testing chown.
    //  3. allow_other requires that `user_allow_other` is in /etc/fuse.conf.
    //    let options = ["-o", "ro", "-o", "fsname=safefs"]    // -o ro = mount read only
    let options = ["-o", "fsname=sn_fs"]
        .iter()
        .map(|o| o.as_ref())
        .collect::<Vec<&OsStr>>();
    if let Err(e) = fuse::mount(SnFs::new(mountpoint_fd), &mountpoint, &options) {
        eprintln!("Mount failed.  {:?}", e);
    }

    // Delete all "real" files (each file representing content of 1 inode) under mount point.
    // this code should be in SnFs::destroy(), but its not getting called.
    // Seems like a fuse bug/issue.
    let mountpoint_fd = Dir::open(Path::new(&mountpoint)).unwrap();
    if let Ok(entries) = mountpoint_fd.list_dir(".") {
        for result in entries {
            if let Ok(entry) = result {
                if entry.simple_type() == Some(SimpleType::File)
                    && mountpoint_fd
                        .remove_file(Path::new(entry.file_name()))
                        .is_err()
                {
                    error!("Unable to remove file {:?}", entry.file_name());
                }
            }
        }
    }
}

fn print_usage() {
    eprintln!("Usage: sn_fs <mountpoint_path>");
}
