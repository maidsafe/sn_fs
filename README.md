# sn_fs

Proof of concept.  A FUSE filesystem implemented atop [crdt_tree](<https://github.com/dan-da/crdt_tree>).

[MaidSafe website](http://maidsafe.net) | [SAFE Network Forum](https://safenetforum.org/)
:-------------------------------------: | :---------------------------------------------:

# About

**EXPERIMENTAL: For testing purposes only**

### Important: Any data stored in the filesystem is wiped upon unmount.

The sn_fs filesystem demonstrates use of the crdt-tree data type to store filesystem
metadata such as directory structure, hard and soft links, filenames, and file attributes.  

File content is stored on disk in the underlying filesystem beneath the mountpoint, but 
these files are not visible to other processes.

At present, the filesystem functions as an in-memory filesystem.  All metadata is lost
upon unmount.  Also, the on-disk files containing file content are wiped, to leave the
mountpoint in original (empty) condition as when it was mounted.

Thus far, sn_fs has only been tested on Linux.

For some background, see [this thread](https://forum.safedev.org/t/filetree-crdt-for-safe-network/2833).

# Functionality

- [x] Directories (mkdir, rmdir)
- [x] Files
- [x] Symlinks (ln -s)
- [x] Hard links (ln)
- [x] Ownership (uid/gid) changes (chown, chgrp)
- [x] Mode changes (chmod)
- [x] Timestamps (mtime, ctime, crtime)

# High Level Roadmap

- [x] Phase 1: Implement crdt_tree.
- [x] Phase 2: Demonstrate a local fuse filesystem built atop crdt_tree.
- [ ] Phase 3: Resolve how best to store file content as CRDT operation.
- [ ] Phase 4: Integrate with Safe Network
- [ ] Phase 5: Local cache that persists between mounts.
- [ ] Phase 6: Implement an API for developers to read/write files to Safe Network without requiring a local mount.

# Building

1. You need rust 1.46.0 or higher installed.
2. You need fuse development package installed, eg `libfuse-dev` on Ubuntu.
3. `$ git clone https://github.com/maidsafe/sn_fs`
4. `$ cd sn_fs && cargo build`

# Usage

You must have FUSE installed.   This is normally installed by default on Linux distributions.

## Mount

`$ cargo run /path/to/mountpoint`

You can also run sn_fs directly:

`$ sn_fs /path/to/mountpoint`

Note that the directory `mountpoint` must be empty.

Once the filesystem is mounted, you can perform regular file operations within it including
chown, chmod, mkdir, link, ln, rm, tree, etc.


## Unmount

In another terminal/shell, run:

`$ fusermount -u /path/to/mountpoint`

## License

This SAFE Network software is dual-licensed under the Modified BSD (<LICENSE-BSD> <https://opensource.org/licenses/BSD-3-Clause>) or the MIT license (<LICENSE-MIT> <https://opensource.org/licenses/MIT>) at your option.

## Contributing

Want to contribute? Great :tada:

There are many ways to give back to the project, whether it be writing new code, fixing bugs, or just reporting errors. All forms of contributions are encouraged!

For instructions on how to contribute, see our [Guide to contributing](https://github.com/maidsafe/QA/blob/master/CONTRIBUTING.md).
