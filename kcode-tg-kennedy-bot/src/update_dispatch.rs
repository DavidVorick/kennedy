use std::{
    collections::{HashMap, VecDeque},
    panic::AssertUnwindSafe,
    sync::Arc,
};

use futures::{FutureExt, future::BoxFuture};
use tokio::sync::{Mutex, Semaphore};

use super::*;

const PER_PRINCIPAL_QUEUE_CAPACITY: usize = 32;
const MAX_ACTIVE_PRINCIPAL_WORKERS: usize = 256;
const GLOBAL_PROCESSING_CONCURRENCY: usize = 16;

type UpdateProcessor = Arc<dyn Fn(Update) -> BoxFuture<'static, anyhow::Result<()>> + Send + Sync>;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum UpdateQueueKey {
    Private(i64),
    GroupUser(i64, i64),
    GroupControl(i64),
    Other,
}

struct DispatcherInner {
    processor: UpdateProcessor,
    workers: Mutex<HashMap<UpdateQueueKey, VecDeque<Update>>>,
    concurrency: Arc<Semaphore>,
}

#[derive(Clone)]
pub(super) struct UpdateDispatcher {
    inner: Arc<DispatcherInner>,
}

impl UpdateDispatcher {
    pub(super) fn new(bot: Bot, state: AppState) -> Self {
        let processor: UpdateProcessor = Arc::new(move |update| {
            let bot = bot.clone();
            let state = state.clone();
            Box::pin(async move { process_update(&bot, &state, update).await })
        });
        Self::with_processor(processor)
    }

    fn with_processor(processor: UpdateProcessor) -> Self {
        Self {
            inner: Arc::new(DispatcherInner {
                processor,
                workers: Mutex::new(HashMap::new()),
                concurrency: Arc::new(Semaphore::new(GLOBAL_PROCESSING_CONCURRENCY)),
            }),
        }
    }

    pub(super) async fn enqueue(&self, update: Update) -> anyhow::Result<()> {
        let update_id = i64::from(update.id.0);
        let key = update_queue_key(&update);
        let spawn_worker = {
            let mut workers = self.inner.workers.lock().await;
            if let Some(queue) = workers.get_mut(&key) {
                if queue.len() >= PER_PRINCIPAL_QUEUE_CAPACITY {
                    tracing::warn!(
                        update_id,
                        "dropping Telegram update because its principal queue is full"
                    );
                    return Ok(());
                }
                queue.push_back(update);
                false
            } else {
                if workers.len() >= MAX_ACTIVE_PRINCIPAL_WORKERS {
                    tracing::warn!(
                        update_id,
                        active_principals = workers.len(),
                        "dropping Telegram update because the principal-worker limit is full"
                    );
                    return Ok(());
                }
                workers.insert(key.clone(), VecDeque::from([update]));
                true
            }
        };

        if spawn_worker {
            tokio::spawn(run_worker(self.inner.clone(), key));
        }
        Ok(())
    }
}

async fn remove_worker(inner: &DispatcherInner, key: &UpdateQueueKey) {
    inner.workers.lock().await.remove(key);
}

async fn run_worker(inner: Arc<DispatcherInner>, key: UpdateQueueKey) {
    loop {
        let update = {
            let mut workers = inner.workers.lock().await;
            let Some(queue) = workers.get_mut(&key) else {
                return;
            };
            if let Some(update) = queue.pop_front() {
                update
            } else {
                workers.remove(&key);
                return;
            }
        };

        let update_id = i64::from(update.id.0);
        let permit = match inner.concurrency.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                remove_worker(&inner, &key).await;
                return;
            }
        };
        match AssertUnwindSafe((inner.processor)(update))
            .catch_unwind()
            .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(
                    update_id,
                    error_class = telegram_requests::anyhow_error_class(&error),
                    "Telegram update failed locally; later work will continue"
                );
            }
            Err(_) => {
                tracing::error!(
                    update_id,
                    "Telegram update processor panicked; later work will continue"
                );
            }
        }
        drop(permit);
    }
}

fn update_queue_key(update: &Update) -> UpdateQueueKey {
    match &update.kind {
        UpdateKind::Message(message) | UpdateKind::EditedMessage(message) => {
            if message.chat.is_private() {
                message
                    .from
                    .as_ref()
                    .and_then(|user| i64::try_from(user.id.0).ok())
                    .map(UpdateQueueKey::Private)
                    .unwrap_or(UpdateQueueKey::Other)
            } else if message.chat.is_group() || message.chat.is_supergroup() {
                if message.migrate_to_chat_id().is_some()
                    || message.migrate_from_chat_id().is_some()
                    || is_group_authored_message(message)
                {
                    UpdateQueueKey::GroupControl(message.chat.id.0)
                } else {
                    message
                        .from
                        .as_ref()
                        .and_then(|user| i64::try_from(user.id.0).ok())
                        .map(|user_id| UpdateQueueKey::GroupUser(message.chat.id.0, user_id))
                        .unwrap_or(UpdateQueueKey::GroupControl(message.chat.id.0))
                }
            } else {
                UpdateQueueKey::Other
            }
        }
        UpdateKind::ChatMember(change) | UpdateKind::MyChatMember(change) => {
            UpdateQueueKey::GroupControl(change.chat.id.0)
        }
        _ => UpdateQueueKey::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;
    use teloxide::types::UpdateId;
    use tokio::sync::mpsc;

    fn private_message(message_id: i32, user_id: i64, text: &str) -> Message {
        serde_json::from_value(json!({
            "message_id":message_id,
            "date":1629404938,
            "from":{"id":user_id,"is_bot":false,"first_name":"User"},
            "chat":{"id":user_id,"first_name":"User","type":"private"},
            "text":text
        }))
        .unwrap()
    }

    fn private_update(update_id: u32, user_id: i64) -> Update {
        Update {
            id: UpdateId(update_id),
            kind: UpdateKind::Message(private_message(
                i32::try_from(update_id).unwrap(),
                user_id,
                "test",
            )),
        }
    }

    fn edited_private_update(update_id: u32, user_id: i64) -> Update {
        Update {
            id: UpdateId(update_id),
            kind: UpdateKind::EditedMessage(private_message(1, user_id, "edited")),
        }
    }

    #[tokio::test]
    async fn principals_overlap_while_each_principal_remains_ordered() {
        let slow_gate = Arc::new(Semaphore::new(0));
        let completed = Arc::new(StdMutex::new(Vec::new()));
        let (started_sender, mut started_receiver) = mpsc::unbounded_channel();
        let (completion_sender, mut completion_receiver) = mpsc::unbounded_channel();

        let processor: UpdateProcessor = {
            let slow_gate = slow_gate.clone();
            let completed = completed.clone();
            Arc::new(move |update| {
                let slow_gate = slow_gate.clone();
                let completed = completed.clone();
                let started_sender = started_sender.clone();
                let completion_sender = completion_sender.clone();
                Box::pin(async move {
                    let update_id = i64::from(update.id.0);
                    if update_id == 1 {
                        let _ = started_sender.send(());
                        let permit = slow_gate.acquire().await?;
                        permit.forget();
                    }
                    completed.lock().unwrap().push(update_id);
                    let _ = completion_sender.send(update_id);
                    Ok(())
                })
            })
        };
        let dispatcher = UpdateDispatcher::with_processor(processor);

        dispatcher.enqueue(private_update(1, 42)).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), started_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        dispatcher.enqueue(private_update(3, 42)).await.unwrap();
        dispatcher.enqueue(private_update(2, 77)).await.unwrap();

        assert_eq!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                completion_receiver.recv()
            )
            .await
            .unwrap(),
            Some(2)
        );
        assert_eq!(*completed.lock().unwrap(), vec![2]);

        slow_gate.add_permits(1);
        assert_eq!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                completion_receiver.recv()
            )
            .await
            .unwrap(),
            Some(1)
        );
        assert_eq!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                completion_receiver.recv()
            )
            .await
            .unwrap(),
            Some(3)
        );
        assert_eq!(*completed.lock().unwrap(), vec![2, 1, 3]);
    }

    #[tokio::test]
    async fn processor_panics_are_local_to_one_update() {
        let (completion_sender, mut completion_receiver) = mpsc::unbounded_channel();
        let processor: UpdateProcessor = Arc::new(move |update| {
            let completion_sender = completion_sender.clone();
            Box::pin(async move {
                let update_id = i64::from(update.id.0);
                if update_id == 1 {
                    panic!("test processor panic");
                }
                let _ = completion_sender.send(update_id);
                Ok(())
            })
        });
        let dispatcher = UpdateDispatcher::with_processor(processor);

        dispatcher.enqueue(private_update(1, 42)).await.unwrap();
        dispatcher.enqueue(private_update(2, 42)).await.unwrap();

        assert_eq!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                completion_receiver.recv()
            )
            .await
            .unwrap(),
            Some(2)
        );
    }

    #[tokio::test]
    async fn a_drained_principal_is_removed_before_a_successor_worker_starts() {
        let completed = Arc::new(StdMutex::new(Vec::new()));
        let (completion_sender, mut completion_receiver) = mpsc::unbounded_channel();
        let processor: UpdateProcessor = {
            let completed = completed.clone();
            Arc::new(move |update| {
                let completed = completed.clone();
                let completion_sender = completion_sender.clone();
                Box::pin(async move {
                    let update_id = i64::from(update.id.0);
                    completed.lock().unwrap().push(update_id);
                    let _ = completion_sender.send(update_id);
                    Ok(())
                })
            })
        };
        let dispatcher = UpdateDispatcher::with_processor(processor);

        dispatcher.enqueue(private_update(1, 42)).await.unwrap();
        assert_eq!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                completion_receiver.recv()
            )
            .await
            .unwrap(),
            Some(1)
        );

        loop {
            if dispatcher.inner.workers.lock().await.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }

        dispatcher.enqueue(private_update(2, 42)).await.unwrap();
        assert_eq!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                completion_receiver.recv()
            )
            .await
            .unwrap(),
            Some(2)
        );
        assert_eq!(*completed.lock().unwrap(), vec![1, 2]);
    }

    #[test]
    fn edits_share_their_source_principals_queue() {
        let original = private_update(10, 42);
        let edited = edited_private_update(11, 42);

        assert_eq!(update_queue_key(&original), update_queue_key(&edited));
        assert_ne!(
            update_queue_key(&original),
            update_queue_key(&private_update(12, 77))
        );
    }

    #[test]
    fn unkeyed_updates_share_one_bounded_worker_class() {
        let first = Update {
            id: UpdateId(1),
            kind: UpdateKind::Error("unknown".into()),
        };
        let second = Update {
            id: UpdateId(2),
            kind: UpdateKind::Error("unknown".into()),
        };

        assert_eq!(update_queue_key(&first), UpdateQueueKey::Other);
        assert_eq!(update_queue_key(&first), update_queue_key(&second));
    }
}
