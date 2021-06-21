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
        let start_key = context.start_key();
        let end_key = context.end_key();
        let start_region =
            keys::extract_region_id(keys::REGION_RAFT_PREFIX_KEY, start_key).unwrap();
        let end_region = keys::extract_region_id(keys::REGION_RAFT_PREFIX_KEY, end_key).unwrap();

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
        // TODO(MrCroxx): need to delete stat key?
        match keys::decode_raft_log_key(_key) {
            Ok((rid, idx)) => match self.map.get(&rid) {
                Some(compact_idx) => idx < *compact_idx,
                None => false,
            },
            Err(_) => false,
        }
    }
}

// TODO(MrCroxx): add tests.
