// Copyright 2026 The Matrix.org Foundation C.I.C.
// SPDX-License-Identifier: Apache-2.0

//! Deterministic receiver scheduling, compiled only for unit tests. The
//! production selection policy and update/recovery handlers stay in tasks.rs.

use super::FocusedUpdate;
use matrix_sdk::event_cache::{RoomEventCacheSubscriber, TimelineVectorDiffs};
use tokio::sync::{
    broadcast::{
        Receiver,
        error::{RecvError, TryRecvError},
    },
    mpsc, oneshot,
};

pub(super) struct FocusedTestScheduler {
    commands: Option<mpsc::UnboundedReceiver<FocusedTestCommand>>,
    completed: Option<(FocusedTestLane, oneshot::Sender<usize>)>,
}

impl FocusedTestScheduler {
    pub(super) fn new(commands: Option<mpsc::UnboundedReceiver<FocusedTestCommand>>) -> Self {
        Self { commands, completed: None }
    }

    pub(super) async fn next(
        &mut self,
        room: &mut RoomEventCacheSubscriber,
        focus: &mut Receiver<TimelineVectorDiffs>,
    ) -> Option<FocusedUpdate> {
        let commands = self.commands.as_mut()?;
        loop {
            let Some(command) = commands.recv().await else {
                return Some(FocusedUpdate::Room(Err(RecvError::Closed)));
            };
            let update = match command.lane {
                FocusedTestLane::Room => room.try_recv().map(|u| FocusedUpdate::Room(Ok(u))),
                FocusedTestLane::Focus => focus.try_recv().map(|u| FocusedUpdate::Focus(Ok(u))),
            };
            let update = match update {
                Ok(update) => update,
                Err(TryRecvError::Empty) => {
                    let _ = command.completed.send(0);
                    continue;
                }
                Err(error) => {
                    let error = match error {
                        TryRecvError::Closed => RecvError::Closed,
                        TryRecvError::Lagged(n) => RecvError::Lagged(n),
                        TryRecvError::Empty => unreachable!(),
                    };
                    match command.lane {
                        FocusedTestLane::Room => FocusedUpdate::Room(Err(error)),
                        FocusedTestLane::Focus => FocusedUpdate::Focus(Err(error)),
                    }
                }
            };
            self.completed = Some((command.lane, command.completed));
            return Some(update);
        }
    }

    pub(super) fn complete(
        &mut self,
        room: &RoomEventCacheSubscriber,
        focus: &Receiver<TimelineVectorDiffs>,
    ) {
        if let Some((lane, completed)) = self.completed.take() {
            let pending = match lane {
                FocusedTestLane::Room => room.len(),
                FocusedTestLane::Focus => focus.len(),
            };
            let _ = completed.send(pending);
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(in crate::timeline) enum FocusedTestLane {
    Room,
    Focus,
}

/// Deterministically choose the next receiver in regression tests. Production
/// uses select!; the mutation and recovery handlers below it are identical.
#[derive(Debug)]
pub(in crate::timeline) struct FocusedTestCommand {
    pub lane: FocusedTestLane,
    pub completed: oneshot::Sender<usize>,
}
