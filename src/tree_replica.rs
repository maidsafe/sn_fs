// Copyright 2020 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// http://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

use log::debug;
use std::collections::HashMap;

use crate::fs_tree_types::{ActorType, FsClock, FsOpMove, FsState};

pub struct TreeReplica {
    #[allow(dead_code)]
    actor_id: ActorType,
    state: FsState,
    time: FsClock,

    latest_time_by_replica: HashMap<ActorType, FsClock>,
}

impl TreeReplica {
    pub fn new() -> Self {
        let actor_id = rand::random::<ActorType>();
        TreeReplica {
            actor_id,
            state: FsState::new(),
            time: FsClock::new(actor_id, None),
            latest_time_by_replica: HashMap::new(),
        }
    }

    #[allow(dead_code)]
    pub fn actor_id(&self) -> &ActorType {
        &self.actor_id
    }

    pub fn state(&self) -> &FsState {
        &self.state
    }

    pub fn apply_op(&mut self, op: FsOpMove) {
        let actor_id = *op.timestamp().actor_id();

        // store latest timestamp for this actor.
        let result = self.latest_time_by_replica.get(&actor_id);
        match result {
            Some(latest) if op.timestamp() > latest => {
                self.latest_time_by_replica
                    .insert(actor_id, op.timestamp().clone());
            }
            None => {
                self.latest_time_by_replica
                    .insert(actor_id, op.timestamp().clone());
            }
            _ => {}
        }

        self.time = self.time.merge(op.timestamp());
        self.state.apply_op(op);
    }

    pub fn apply_ops(&mut self, ops: &[FsOpMove]) {
        for op in ops {
            self.apply_op(op.clone());
        }
    }

    #[allow(dead_code)]
    pub fn causally_stable_threshold(&self) -> Option<&FsClock> {
        debug!("latest_time_by_replica: {:?}", self.latest_time_by_replica);
        // The minimum of latest timestamp from each replica
        // is the causally stable threshold.
        let mut oldest: Option<&FsClock> = None;
        for (_actor_id, timestamp) in self.latest_time_by_replica.iter() {
            match oldest {
                Some(o) if timestamp < o => {
                    oldest = Some(&timestamp);
                }
                None => {
                    oldest = Some(&timestamp);
                }
                _ => {}
            };
        }
        oldest
    }

    #[allow(dead_code)]
    pub fn truncate_log(&mut self) -> bool {
        if let Some(t) = self.causally_stable_threshold() {
            let tt = t.clone();
            debug!("truncating log before {:?}", tt);
            self.state.truncate_log_before(&tt)
        } else {
            false
        }
    }

    pub fn tick(&mut self) -> FsClock {
        self.time.tick()
    }
}
