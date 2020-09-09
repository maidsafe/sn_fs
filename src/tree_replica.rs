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
            self.state.truncate_log_before(&tt)
        } else {
            false
        }
    }

    pub fn tick(&mut self) -> FsClock {
        self.time.tick()
    }
}
