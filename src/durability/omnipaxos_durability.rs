use super::*;

use omnipaxos::{macros::Entry, OmniPaxos, util::LogEntry};
use omnipaxos_storage::memory_storage::MemoryStorage;

#[derive(Clone, Debug, Entry)]
pub struct TxLogEntry {
    tx_offset: TxOffset,
    tx_data: TxData,
}

type OmniPaxosLE = OmniPaxos<TxLogEntry, MemoryStorage<TxLogEntry>>;

/// OmniPaxosDurability is a OmniPaxos node that should provide the replicated
/// implementation of the DurabilityLayer trait required by the Datastore.
pub struct OmniPaxosDurability {
    pub omni_paxos: OmniPaxosLE,
}

impl OmniPaxosDurability {
    pub fn get_decided_entries(&self) -> Option<Vec<TxLogEntry>> {
        let decided_log_entries = self.omni_paxos.read_decided_suffix(0);
        if let Some(log_entries) = decided_log_entries {
            Some(
                log_entries
                .iter()
                .map(|log_entry| match log_entry {
                    LogEntry::Decided(entry) => entry.clone(),
                    _ => panic!("Unexpected log entry type"),
                })
                .collect()
            )
        } else {
            None
        }
    }
}

impl DurabilityLayer for OmniPaxosDurability {
    fn iter(&self) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        let decided_entries = self.get_decided_entries();
        if let Some(entries) = decided_entries {
            Box::new(
                entries
                .into_iter()
                .map(|entry| (entry.tx_offset, entry.tx_data))
            )
        } else {
            Box::new(std::iter::empty())
        }
    }
    /// offset is exclusive!!!!!!!
    fn iter_starting_from_offset(
        &self,
        offset: TxOffset,
    ) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        let decided_entries = self.get_decided_entries();
        if let Some(entries) = decided_entries {
            Box::new(
                entries
                .into_iter()
                .filter(move |entry| entry.tx_offset.0 > offset.0)
                .map(|entry| (entry.tx_offset, entry.tx_data))
            )
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn append_tx(&mut self, tx_offset: TxOffset, tx_data: TxData) {
        self.omni_paxos
        .append(TxLogEntry {tx_offset,tx_data,})
        .expect("Failed to append");
    }

    fn get_durable_tx_offset(&self) -> Option<TxOffset> {
        let decided_idx = self.omni_paxos.get_decided_idx();
        if decided_idx == 0 {
            None
        } else {
            let last_decided_entry = self.omni_paxos.read(decided_idx - 1);
            if let Some(LogEntry::Decided(entry)) = last_decided_entry {
                Some(entry.tx_offset)
            } else {
                panic!("Unexpected log entry type")
            }
        }
    }
}
