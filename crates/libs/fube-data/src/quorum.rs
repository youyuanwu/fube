// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Source: Reliability/Replication/ReplicaManager.cpp:2328-2526 and
// Helpers.cpp, 3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d.

//! Current/previous apply-quorum and receive-release calculations. Missing
//! members are missing votes, never a smaller manufactured configuration.

use fube_runtime::extensions::NativeReplicaSetConfiguration as ReplicaSetConfiguration;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PeerProgress {
    pub received: i64,
    pub applied: i64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConfigurationProgress {
    pub quorum: i64,
    pub all_applied: i64,
    pub all_received: i64,
}

pub fn configuration_progress(
    configuration: &ReplicaSetConfiguration,
    latest: i64,
    previous: bool,
    mut peer: impl FnMut(i64) -> Option<PeerProgress>,
) -> Option<ConfigurationProgress> {
    let remote_pc_one =
        previous && configuration.write_quorum == 1 && !configuration.replicas.is_empty();
    // Resolve the native NonInitializedLSN sentinel to the assigned tail.
    // PC q1 with a remote is a distinct APPLY-gated exception below.
    if configuration.write_quorum <= 1 && !remote_pc_one {
        return Some(ConfigurationProgress {
            quorum: latest,
            all_applied: latest,
            all_received: latest,
        });
    }
    let remote: Vec<_> = configuration
        .replicas
        .iter()
        .map(|r| {
            if r.up && r.ready {
                peer(r.replica_id)
            } else {
                None
            }
        })
        .collect();
    let available = remote.iter().filter(|p| p.is_some()).count();
    // Native assumes an immediately ACKing primary even when not in PC.
    if available < configuration.write_quorum.saturating_sub(1) {
        return None;
    }
    let mut applies: Vec<_> = remote
        .iter()
        .flatten()
        .map(|p| p.applied.min(latest))
        .collect();
    let all_applied = remote
        .iter()
        .map(|p| p.map_or(0, |p| p.applied))
        .min()
        .unwrap_or(latest)
        .min(latest);
    let all_received = remote
        .iter()
        .map(|p| p.map_or(0, |p| p.received))
        .min()
        .unwrap_or(latest)
        .min(latest);
    // Native previous quorum=1 with a remote PC member cannot self-commit,
    // including the local-primary-not-in-PC I/P,S/N transition.
    let quorum = if remote_pc_one {
        all_applied
    } else {
        applies.sort_unstable_by(|a, b| b.cmp(a));
        *applies.get(configuration.write_quorum - 2)?
    };
    Some(ConfigurationProgress {
        quorum,
        all_applied,
        all_received: if remote_pc_one {
            all_applied
        } else {
            all_received.min(quorum)
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fube_runtime::extensions::{
        NativeReplicaDescriptor as ReplicaInformation, ReplicationProgress,
    };
    use fube_runtime::{Epoch, ReplicaRole};
    fn config(local: bool, quorum: usize, n: usize) -> ReplicaSetConfiguration {
        ReplicaSetConfiguration {
            epoch: Epoch::INVALID,
            write_quorum: quorum,
            local_member: local,
            replicas: (1..=n)
                .map(|id| ReplicaInformation {
                    node_id: [id as u8; 16],
                    node_instance: 1,
                    replica_id: id as i64,
                    incarnation: 1,
                    role: ReplicaRole::ActiveSecondary,
                    previous_role: ReplicaRole::ActiveSecondary,
                    up: true,
                    ready: true,
                    address: String::new(),
                    progress: ReplicationProgress::EMPTY,
                    must_catch_up: false,
                })
                .collect(),
        }
    }
    #[test]
    fn apply_not_receive_controls_quorum_and_previous_one_excludes_self() {
        let peer = |_| {
            Some(PeerProgress {
                received: 10,
                applied: 3,
            })
        };
        assert_eq!(
            configuration_progress(&config(true, 2, 2), 10, false, peer).unwrap(),
            ConfigurationProgress {
                quorum: 3,
                all_applied: 3,
                all_received: 3
            }
        );
        assert_eq!(
            configuration_progress(&config(true, 1, 1), 10, false, peer)
                .unwrap()
                .quorum,
            10
        );
        assert_eq!(
            configuration_progress(&config(false, 1, 1), 10, true, peer)
                .unwrap()
                .quorum,
            3
        );
        assert_eq!(
            configuration_progress(&config(true, 1, 1), 10, true, peer)
                .unwrap()
                .quorum,
            3
        );
        assert_eq!(
            configuration_progress(&config(false, 2, 2), 10, true, |id| if id == 1 {
                peer(id)
            } else {
                None
            })
            .unwrap()
            .quorum,
            3
        );
    }
    #[test]
    fn sentinel_pc_order_statistic_and_apply_release_are_independent() {
        let cc = configuration_progress(&config(true, 1, 2), 10, false, |_| None).unwrap();
        assert_eq!(
            cc,
            ConfigurationProgress {
                quorum: 10,
                all_applied: 10,
                all_received: 10
            }
        );
        let pc = config(false, 2, 2);
        let p = configuration_progress(&pc, 10, true, |id| {
            Some(PeerProgress {
                received: 10,
                applied: if id == 1 { 10 } else { 0 },
            })
        })
        .unwrap();
        assert_eq!(cc.quorum.min(p.quorum), 10);
        assert_eq!(p.all_received, 10);
        assert!(!pc.local_member);
        assert!(configuration_progress(&pc, 10, true, |_| None).is_none());
        let one = configuration_progress(&config(false, 1, 1), 10, true, |_| {
            Some(PeerProgress {
                received: 10,
                applied: 0,
            })
        })
        .unwrap();
        assert_eq!((one.quorum, one.all_received), (0, 0));
    }
    #[test]
    fn current_and_previous_are_separate_not_union_majority() {
        let current = configuration_progress(&config(true, 2, 2), 9, false, |_| {
            Some(PeerProgress {
                received: 9,
                applied: 9,
            })
        })
        .unwrap();
        let previous = configuration_progress(&config(false, 2, 2), 9, true, |_| {
            Some(PeerProgress {
                received: 9,
                applied: 2,
            })
        })
        .unwrap();
        assert_eq!(current.quorum.min(previous.quorum), 2);
        assert!(
            configuration_progress(&config(true, 3, 1), 9, false, |_| Some(
                PeerProgress::default()
            ))
            .is_none()
        );
    }
}
