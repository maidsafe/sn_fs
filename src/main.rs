extern crate env_logger;
extern crate fuse;
extern crate libc;
extern crate time;

mod fs_tree_types;
mod metadata;
mod tree_replica;

use log::{error};
use nix::unistd::{getuid, getgid};

use fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, Request,
};
use libc::{EINVAL, ENOENT, ENOTEMPTY};
use std::env;
use std::ffi::OsStr;
use std::os::raw::c_int;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use time::Timespec; // unix specific.

use fs_tree_types::{FsOpMove, FsTreeNode};
use metadata::{DirentKind, FsInodeDirectory, FsInodeSymlink, FsInodeFile, FsInodeCommon, FsMetadata, FsRefFile};
use tree_replica::TreeReplica;

const TTL: Timespec = Timespec { sec: 1, nsec: 0 }; // 1 second

const CREATE_TIME: Timespec = Timespec {
    sec: 1381237736,
    nsec: 0,
}; // 2013-10-08 08:56

/*
const HELLO_DIR_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: CREATE_TIME,
    mtime: CREATE_TIME,
    ctime: CREATE_TIME,
    crtime: CREATE_TIME,
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 1000,
    gid: 20,
    rdev: 0,
    flags: 0,
};


const HELLO_TXT_CONTENT: &'static str = "Hello World!\n";

const HELLO_TXT_ATTR: FileAttr = FileAttr {
    ino: 2,
    size: 13,
    blocks: 1,
    atime: CREATE_TIME,
    mtime: CREATE_TIME,
    ctime: CREATE_TIME,
    crtime: CREATE_TIME,
    kind: FileType::RegularFile,
    perm: 0o644,
    nlink: 1,
    uid: 1000,
    gid: 20,
    rdev: 0,
    flags: 0,
};

*/

struct SafeFS {
    replica: TreeReplica,
}

struct InoMerge;

impl InoMerge {
    pub fn combine(a: u64, b: u64) -> u64 {
        a << 48 | b
    }
    /*
        pub fn get_a(c: u64) -> u64 {
            c >> 48
        }

        pub fn get_b(c: u64) -> u64 {
            c & 0xFFFFFFFFFFFF
        }
    */
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
}

impl Filesystem for SafeFS {
    // Get child of a directory by name.
    /*
        fn children_with_name(&self, parent: u64, name: &OsStr) -> Result<Vec<(u64, &FsTreeNode)>> {
            let mut matches = Vec<(u64, FsTreeNode)>;
            let t = self.replica.state().tree();
            for child_id in t.children(&parent) {
                if let Some(node) = t.find(&child_id) {
                    if node.metadata().name() == name {
                        matches.push( (child_id, &node);
                    }
                }
            }
            matches
        }
    */

    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        let meta = FsMetadata::InodeDirectory(FsInodeDirectory {
            name: OsStr::new("root").to_os_string(),
            common: FsInodeCommon {
                size: 0,
                links: 1,
                ctime: Self::now(),
                mtime: Self::now(),
            }
        });
        let op = self.new_opmove(Self::forest(), meta, Self::root());
        self.replica.apply_op(op);

        // self.replica = TreeReplica::new();
        Ok(())
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        println!("lookup -- parent: {},  name: {:?}", parent, name);

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
                atime: CREATE_TIME,
                mtime: meta.mtime(),
                ctime: meta.ctime(),
                crtime: CREATE_TIME,
                kind,
                perm: 0o644,
                nlink: meta.links(),
                uid: getuid().as_raw() as u32,
                gid: getgid().as_raw() as u32,
                rdev: 0,
                flags: 0,
            };
            reply.entry(&TTL, &attr, 0);
        } else {
            reply.error(ENOENT);
        }
        // note:  we do not increment lookup count.
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        println!("getattr -- ino: {}", ino);

        /*
                if ino == 1 {
                    reply.attr(&TTL, &HELLO_DIR_ATTR);
                    return;
                }
        */

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
                atime: CREATE_TIME,
                mtime: meta.mtime(),
                ctime: meta.ctime(),
                crtime: CREATE_TIME,
                kind,
                perm: 0o644,
                nlink: meta.links(),
                uid: getuid().as_raw() as u32,
                gid: getgid().as_raw() as u32,
                rdev: 0,
                flags: 0,
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
        _mtime: Option<Timespec>,
        _fh: Option<u64>,
        _crtime: Option<Timespec>,
        _chgtime: Option<Timespec>,
        _bkuptime: Option<Timespec>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        println!(
            "setattr -- ino: {}, mode: {:?}, uid: {:?}, gid: {:?}",
            ino, mode, uid, gid
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

            if let Some(new_size) = size {
                if kind == FileType::RegularFile {
                    println!("setattr -- size={}, new_size={})", meta.size(), new_size);
                    meta.truncate_content(new_size);
                    meta.set_size(new_size);
                    meta.set_mtime(Self::now());
                    let parent_id = *node.parent_id();

                    let op = self.new_opmove(parent_id, meta.clone(), ino);
                    self.replica.apply_op(op);

                } else {
                    reply.error(EINVAL);
                    return;
                }
            }

            let attr = FileAttr {
                ino,
                size: meta.size(),
                blocks: 1,
                atime: CREATE_TIME,
                mtime: meta.mtime(),
                ctime: meta.ctime(),
                crtime: CREATE_TIME,
                kind,
                perm: mode.unwrap_or(0o644) as u16,
                nlink: meta.links(),
                uid: getuid().as_raw() as u32,
                gid: getgid().as_raw() as u32,
                rdev: 0,
                flags: flags.unwrap_or(0),
            };
            reply.attr(&TTL, &attr);

        } else {
            reply.error(ENOENT);
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        println!("rmdir -- parent: {}, name: {:?}", parent, name);

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
        _req: &Request,
        parent: u64,
        name: &OsStr,
        link: &Path,
        reply: ReplyEntry,
    ) {
        println!(
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
                    mtime: Self::now(),
                }
            });

            let op = self.new_opmove_new_child(parent, meta.clone());
            let ino = *op.child_id();

            let attr = FileAttr {
                ino,
                size: meta.size(),
                blocks: 1,
                atime: CREATE_TIME,
                mtime: meta.mtime(),
                ctime: meta.ctime(),
                crtime: CREATE_TIME,
                kind: FileType::Symlink,
                perm: 0o644,
                nlink: meta.links(),
                uid: getuid().as_raw() as u32,
                gid: getgid().as_raw() as u32,
                rdev: 0,
                flags: 0,
            };

            self.replica.apply_op(op);

            reply.entry(&TTL, &attr, 1);
        } else {
            reply.error(ENOENT);
        }
    }

    fn readlink(&mut self, _req: &Request, ino: u64, reply: ReplyData) {
        println!("readlink -- ino: {}", ino);

        // find parent dir (under /root/)
        if let Some(node) = self.replica.state().tree().find(&ino) {
            // Ensure this node is a symlink
            if !matches!(node.metadata().dirent_kind(), Some(DirentKind::Symlink)) {
                println!("readlink -- einval -- ino: {}", ino);
                reply.error(EINVAL);
                return;
            }

            println!("readlink -- success -- ino: {}", ino);
            reply.data(node.metadata().symlink().as_bytes());
        } else {
            println!("readlink -- enoent -- ino: {}", ino);
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
        println!(
            "rename -- parent: {}, name: {:?}, newparent: {}, newname: {:?}",
            parent, name, newparent, newname
        );

        let mut ops: Vec<FsOpMove> = Vec::new();

        // find child of parent that matches $name
        if let Some((child, node)) = self.child_by_name(parent, name) {
            let mut meta = node.metadata().clone();
            meta.set_name(newname);

            // If there is an existing node in target location, it is moved to trash.
            if let Some((old_ino, ..)) = self.child_by_name(newparent, newname) {
                ops.push(self.new_opmove(Self::trash(), FsMetadata::Empty, old_ino));
            }

            // move child to new location/name
            ops.push(self.new_opmove(newparent, meta, child));
            self.replica.apply_ops(&ops);

            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, _mode: u32, reply: ReplyEntry) {
        // 2. find parent dir (under /root/)
        let result = self.replica.state().tree().find(&parent);

        // fixme: check if node is a directory.
        if result.is_none() && parent != 1 {
            reply.error(ENOENT);
            return;
        }

        // 3. create tree node under /root/../parent_id
        let meta =
            FsMetadata::InodeDirectory(FsInodeDirectory {
                name: name.to_os_string(),
                common: FsInodeCommon {
                    size: 0,
                    links: 1,
                    ctime: Self::now(),
                    mtime: Self::now(),
                },
            });

        let op = self.new_opmove_new_child(parent, meta.clone());
        let ino = *op.child_id();

        self.replica.apply_op(op);

        let attr = FileAttr {
            ino,
            size: meta.size(),
            blocks: 1,
            atime: CREATE_TIME,
            mtime: meta.mtime(),
            ctime: meta.ctime(),
            crtime: CREATE_TIME,
            kind: FileType::Directory,
            perm: 0o644,
            nlink: meta.links(),
            uid: getuid().as_raw() as u32,
            gid: getgid().as_raw() as u32,
            rdev: 0,
            flags: 0,
        };

        reply.entry(&TTL, &attr, 1);

        println!(
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
        println!("entering readdir.  ino: {}, offset: {}", ino, offset);

        // if ino != 1 {
        //     reply.error(ENOENT);
        //     return;
        // }

        /*
                if ino == 1 {
                    let entries = vec![
                        (1, FileType::Directory, "."),
                        (1, FileType::Directory, ".."),
                    ];
                    for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
                        // i + 1 means the index of the next entry
                        reply.add(entry.0, (i + 1) as i64, entry.1, entry.2);
                    }
        //            reply.ok();
        //            return;
                }
        */

        // find children after offset.
        // let to_skip = if offset == 0 { offset } else { offset + 1 } as usize;
        let children = self.replica.state().tree().children(&ino);
        for (i, child_ino) in children.iter().enumerate().skip(offset as usize) {
            if let Some(node) = self.replica.state().tree().find(child_ino) {
                let k = node.metadata().dirent_kind();

                println!("meta: {:#?}, k: {:?}", node.metadata(), k);

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

                /*
                let next_ino = match node.metadata().inode_id() {
                    Some(id) => id,
                    None => *child_ino
                };
                */
                let next_ino = *child_ino;

                // ino, offset, filetype, name
                // reply.add returns true if it is full.
                if reply.add(next_ino, (i + 1) as i64, filetype, node.metadata().name()) {
                    break;
                }
                println!(
                    "added child.  ino: {}, name: {:?}, filetype: {:?}",
                    next_ino,
                    node.metadata().name(),
                    filetype
                );
            }
        }
        reply.ok();

        println!("leaving readdir.  ino: {}, offset: {}", ino, offset);
    }

    fn link(
        &mut self,
        _req: &Request,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        println!(
            "link -- ino: {}, newparent: {}, newname: {:?}",
            ino, newparent, newname
        );

        let mut ops: Vec<FsOpMove> = Vec::new();

        if let Some(node) = self.replica.state().tree().find(&ino) {
            let mut meta = node.metadata().clone();
            meta.links_inc();

            // fixme: check if newname entry already exists.  (return EEXIST)

            let attr = FileAttr {
                ino,
                size: meta.size(),
                blocks: 1,
                atime: CREATE_TIME,
                mtime: meta.mtime(),
                ctime: meta.ctime(),
                crtime: CREATE_TIME,
                kind: FileType::RegularFile,
                perm: 0o644,
                nlink: meta.links(),
                uid: getuid().as_raw() as u32,
                gid: getgid().as_raw() as u32,
                rdev: 0,
                flags: 0,
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
        println!("unlink -- parent: {}, name: {:?}", parent, name);

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
                        let cnt = meta.links_dec();

                        if cnt > 0 {
                            // reference(s) still exist, so we need to update the inode's link count.
                            println!("unlink -- links: {}, preserving inode {}", cnt, inode_id);
                            let parent_id = *inode.parent_id();
                            ops.push(self.new_opmove(parent_id, meta, inode_id));
                        } else {
                            // when link count has dropped to zero, move the inode to trash
                            println!("unlink -- links: {}, removing inode {}.", cnt, inode_id);
                            ops.push(self.new_opmove(Self::trash(), FsMetadata::Empty, inode_id));
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
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: u32,
        reply: ReplyCreate,
    ) {
        println!(
            "create -- parent={}, name={:?}, mode={}, flags={})",
            parent, name, mode, flags
        );

        // check if already existing
        if self.child_by_name(parent, name).is_some() {
            println!("create -- already exists!  bailing out.");
            reply.error(EINVAL);
            return;
        }

        let mut ops: Vec<FsOpMove> = Vec::new();

        // fixme: verify parent ino is a directory

        // 3. create tree node under /inodes/<x>
        let file_inode_meta = FsInodeFile {
            content: Vec::<u8>::new(),
            common: FsInodeCommon {
                size: 0,
                ctime: Self::now(),
                mtime: Self::now(),
                links: 1,
            }
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
            atime: CREATE_TIME,
            mtime: meta.mtime(),
            ctime: meta.ctime(),
            crtime: CREATE_TIME,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: meta.links(),
            uid: getuid().as_raw() as u32,
            gid: getgid().as_raw() as u32,
            rdev: 0,
            flags: 0,
        };

        reply.created(&TTL, &attr, 1, 0 as u64, 0 as u32);
    }

    fn flush(&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        //eprintln!("Filesystem::flush(ino={}, fh={})", ino, fh);

        /*match util::fsync(fh as i32) {
            Err(errno) => { reply.error(errno); return; }
            Ok(_) => { reply.ok(); return; }
        }*/

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
        eprintln!("release -- ino={}, fh={}, flush={}", ino, fh, flush);

        /*if flush {
            match util::fsync(fh as i32) {
                Err(errno) => { reply.error(errno); return; }
                Ok(_) => { }
            }
        }*/

        reply.ok();
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _flags: u32,
        reply: ReplyWrite,
    ) {
        // find tree node from inode_id
        if let Some(inode) = self.replica.state().tree().find(&ino) {
            let mut meta = inode.metadata().clone();
            meta.update_content(&data, offset);
            let size = meta.content().unwrap().len() as u64;
            meta.set_size(size);

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
        println!("open -- ino={}, flags={}", ino, flags);
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
    //    let options = ["-o", "ro", "-o", "fsname=safefs"]    // -o ro = mount read only
    let options = ["-o", "fsname=safefs"]
        .iter()
        .map(|o| o.as_ref())
        .collect::<Vec<&OsStr>>();
    fuse::mount(SafeFS::new(), &mountpoint, &options).unwrap();
}
