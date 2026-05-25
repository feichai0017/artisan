use dashmap::DashMap;

use crate::layout::BlobGuid;

pub(super) struct RouteResidency {
    budget: usize,
    entries: DashMap<BlobGuid, u64>,
}

impl RouteResidency {
    pub(super) fn new(cache_capacity: usize) -> Self {
        Self {
            budget: route_resident_budget(cache_capacity),
            entries: DashMap::new(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn contains(&self, guid: BlobGuid) -> bool {
        self.entries.contains_key(&guid)
    }

    pub(super) fn remove(&self, guid: BlobGuid) {
        self.entries.remove(&guid);
    }

    pub(super) fn mark(&self, guid: BlobGuid, tick: u64) -> usize {
        if self.budget == 0 {
            return 0;
        }
        if let Some(mut entry) = self.entries.get_mut(&guid) {
            *entry = tick;
            return 0;
        }
        self.entries.insert(guid, tick);

        let mut demotions = 0;
        while self.entries.len() > self.budget {
            if !self.demote_oldest() {
                break;
            }
            demotions += 1;
        }
        demotions
    }

    fn demote_oldest(&self) -> bool {
        let mut victim: Option<(BlobGuid, u64)> = None;
        for kv in &self.entries {
            let guid = *kv.key();
            let tick = *kv.value();
            match victim {
                None => victim = Some((guid, tick)),
                Some((_, vmin)) if tick < vmin => victim = Some((guid, tick)),
                _ => {}
            }
        }
        if let Some((guid, _)) = victim {
            self.entries.remove(&guid);
            true
        } else {
            false
        }
    }
}

fn route_resident_budget(capacity: usize) -> usize {
    if capacity < 4 {
        0
    } else {
        (capacity / 4).min(4096)
    }
}
