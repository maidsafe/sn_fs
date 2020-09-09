use crate::metadata::FsMetadata;

pub type ActorType = u64;
pub type TreeIdType = u64;
pub type TreeMetaType = FsMetadata;

pub type FsState = crdt_tree::State<TreeIdType, TreeMetaType, ActorType>;
pub type FsClock = crdt_tree::Clock<ActorType>;
pub type FsOpMove = crdt_tree::OpMove<TreeIdType, TreeMetaType, ActorType>;
// pub type FsLogOpMove = crdt_tree::LogOpMove<TreeIdType, TreeMetaType, ActorType>;
pub type FsTreeNode = crdt_tree::TreeNode<TreeIdType, TreeMetaType>;
