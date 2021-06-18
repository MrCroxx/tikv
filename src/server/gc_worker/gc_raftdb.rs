// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::raw::{new_compaction_filter_raw, CompactionFilter, CompactionFilterFactory};
use engine_rocks::RAFT_LOG_GC_INDEXES;
use std::{collections::HashMap, ffi::CString};
use tikv_util::info;

pub struct RaftLogCompactionFilterFactory {}

impl RaftLogCompactionFilterFactory {}

impl CompactionFilterFactory for RaftLogCompactionFilterFactory {
    fn create_compaction_filter(
        &self,
        context: &engine_rocks::raw::CompactionFilterContext,
    ) -> *mut engine_rocks::raw::DBCompactionFilter {
        let (start_key, end_key) = context.key_range();
        let (start_region, _) = keys::decode_raft_log_key(start_key).unwrap();
        let (end_region, _) = keys::decode_raft_log_key(end_key).unwrap();

        let mut map = HashMap::new();
        let files = context.file_numbers();

        let indexes = RAFT_LOG_GC_INDEXES.read().unwrap();
        indexes.iter().for_each(|(rid, idx)| {
            if start_region <= *rid && *rid <= end_region {
                map.insert(*rid, *idx);
            }
        });
        drop(indexes);
        info!(
            "raft log gc on compaction";
            "files"=>format!("{:?}",&files),
            "progress"=>format!("{:?}",&map),
        );

        let filter = Box::new(RaftLogCompactionFilter::new(map));
        let name = CString::new("").unwrap();
        unsafe { new_compaction_filter_raw(name, filter) }
    }
}

struct RaftLogCompactionFilter {
    map: HashMap<u64, u64>,
}

impl RaftLogCompactionFilter {
    fn new(map: HashMap<u64, u64>) -> Self {
        RaftLogCompactionFilter { map }
    }
}

impl CompactionFilter for RaftLogCompactionFilter {
    fn filter(
        &mut self,
        _level: usize,
        _key: &[u8],
        _value: &[u8],
        _new_value: &mut Vec<u8>,
        _value_changed: &mut bool,
    ) -> bool {
        let (rid, idx) = keys::decode_raft_log_key(_key).unwrap();
        match self.map.get(&rid) {
            Some(compact_idx) => idx < *compact_idx,
            None => false,
        }
    }
}

// TODO(MrCroxx): add tests.
