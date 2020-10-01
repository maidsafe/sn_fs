// Copyright 2020 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// http://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

use crate::metadata::FsMetadata;

/// Define some concrete types for working with crdt_tree.

pub type ActorType = u64;
pub type TreeIdType = u64;
pub type TreeMetaType = FsMetadata;

pub type FsState = crdt_tree::State<TreeIdType, TreeMetaType, ActorType>;
pub type FsClock = crdt_tree::Clock<ActorType>;
pub type FsOpMove = crdt_tree::OpMove<TreeIdType, TreeMetaType, ActorType>;
// pub type FsLogOpMove = crdt_tree::LogOpMove<TreeIdType, TreeMetaType, ActorType>;   // for future.
pub type FsTreeNode = crdt_tree::TreeNode<TreeIdType, TreeMetaType>;
