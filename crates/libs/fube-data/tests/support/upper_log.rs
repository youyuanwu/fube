//! Offline provider-contract fixture, not an A0 transaction/log kernel.
//! Stores actual record bytes; the lower never manufactures this fixture's tail.
use super::data_contract::{
    Cancellation, Epoch, Error, IOperationDataStream, IStateProvider, OperationData, Result,
};
use fube_runtime::{ApiFuture, ErrorKind};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

type Records = Vec<(i64, Vec<Vec<u8>>)>;
pub struct UpperLog {
    records: Mutex<Records>,
    epoch: Mutex<Epoch>,
    pub getters: AtomicUsize,
    pub updates: AtomicUsize,
    pub constructed: AtomicBool,
    pub queried_before_return: AtomicBool,
    pub horizons: Mutex<Vec<i64>>,
    pub fail_next_getter: AtomicBool,
    pub bad_next_getter: AtomicBool,
    pub fail_next_epoch: AtomicBool,
}
impl UpperLog {
    pub fn new(epoch: Epoch, tail: i64) -> Arc<Self> {
        assert!(tail >= 1);
        // Source-shaped zero/one bootstrap record bytes. These are fixture
        // markers, not production native record serialization/recovery.
        let records = vec![
            (0, vec![b"index/epoch/begin-checkpoint".to_vec()]),
            (
                1,
                vec![b"barrier/end-checkpoint/complete-checkpoint".to_vec()],
            ),
        ];
        let log = Arc::new(Self {
            records: Mutex::new(records),
            epoch: Mutex::new(epoch),
            getters: AtomicUsize::new(0),
            updates: AtomicUsize::new(0),
            constructed: AtomicBool::new(false),
            queried_before_return: AtomicBool::new(false),
            horizons: Mutex::new(Vec::new()),
            fail_next_getter: AtomicBool::new(false),
            bad_next_getter: AtomicBool::new(false),
            fail_next_epoch: AtomicBool::new(false),
        });
        for sequence in 2..=tail {
            log.append(
                sequence,
                &OperationData::new(vec![vec![sequence as u8]]).unwrap(),
            );
        }
        log
    }
    pub fn tail(&self) -> i64 {
        self.records.lock().unwrap().last().unwrap().0
    }
    pub fn append(&self, sequence: i64, data: &OperationData) {
        let mut records = self.records.lock().unwrap();
        if let Some((_, old)) = records.iter().find(|(lsn, _)| *lsn == sequence) {
            assert_eq!(
                old,
                &data.to_segments(),
                "overlap must preserve actual original bytes"
            );
            return;
        }
        assert_eq!(sequence, records.last().unwrap().0 + 1);
        records.push((sequence, data.to_segments()));
    }
    pub fn snapshot(&self) -> OperationData {
        let epoch = self.epoch.lock().unwrap();
        let records = self.records.lock().unwrap();
        OperationData::new(vec![serde_json::to_vec(&(
            [epoch.data_loss_number, epoch.configuration_number],
            &*records,
        ))
        .unwrap()])
        .unwrap()
    }
    pub fn apply_snapshot(&self, data: &OperationData) {
        let (epoch, records): ([i64; 2], Records) =
            serde_json::from_slice(&data.segments()[0]).unwrap();
        assert_eq!(records[0].0, 0);
        assert_eq!(records[1].0, 1);
        assert!(records.windows(2).all(|pair| pair[1].0 == pair[0].0 + 1));
        let mut represented = self.epoch.lock().unwrap();
        let mut destination = self.records.lock().unwrap();
        *represented = Epoch {
            data_loss_number: epoch[0],
            configuration_number: epoch[1],
        };
        *destination = records;
    }
}
struct Payloads(Mutex<VecDeque<OperationData>>);
impl IOperationDataStream for Payloads {
    fn get_next(&self, cancellation: Cancellation) -> ApiFuture<'_, Option<OperationData>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(Error::Closed);
            }
            Ok(self.0.lock().unwrap().pop_front())
        })
    }
}
impl IStateProvider for UpperLog {
    fn get_last_committed_sequence_number(&self) -> Result<i64> {
        self.getters.fetch_add(1, Ordering::AcqRel);
        if self.fail_next_getter.swap(false, Ordering::AcqRel) {
            return Err(Error::new(ErrorKind::InvalidState, "unopened upper log"));
        }
        if self.bad_next_getter.swap(false, Ordering::AcqRel) {
            return Ok(-1);
        }
        if !self.constructed.load(Ordering::Acquire) {
            self.queried_before_return.store(true, Ordering::Release);
        }
        Ok(self.tail())
    }
    fn update_epoch(
        &self,
        epoch: Epoch,
        previous: i64,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(Error::Closed);
            }
            self.updates.fetch_add(1, Ordering::AcqRel);
            if self.fail_next_epoch.swap(false, Ordering::AcqRel) {
                return Err(Error::new(
                    ErrorKind::InvalidState,
                    "controlled initial epoch failure",
                ));
            }
            let mut represented = self.epoch.lock().unwrap();
            if epoch == *represented {
                return Ok(());
            }
            if epoch < *represented || previous != self.tail() {
                return Err(Error::new(
                    ErrorKind::InvalidState,
                    "new idle-copy upper epoch boundary mismatch",
                ));
            }
            *represented = epoch;
            Ok(())
        })
    }
    fn on_data_loss(&self, _: Cancellation) -> ApiFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }
    fn get_copy_context(&self) -> Result<Arc<dyn IOperationDataStream>> {
        Ok(Arc::new(Payloads(Mutex::new(VecDeque::new()))))
    }
    fn get_copy_state(
        &self,
        horizon: i64,
        context: Option<Arc<dyn IOperationDataStream>>,
    ) -> Result<Arc<dyn IOperationDataStream>> {
        assert!(context.is_none());
        self.horizons.lock().unwrap().push(horizon);
        assert!(horizon <= self.tail());
        // Model the native upper's checkpoint extension beyond the lower's
        // inclusive requested horizon, without modifying lower progress.
        Ok(Arc::new(Payloads(Mutex::new(VecDeque::from([
            self.snapshot()
        ])))))
    }
}
