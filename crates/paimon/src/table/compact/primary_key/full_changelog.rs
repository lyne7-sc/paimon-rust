// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Changelog generation between successive full-compaction states.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use super::lookup::{live_row, result_row, source_row, ChangelogGenerator};
use crate::spec::DataField;
use crate::table::kv_file_reader::KeyValueReadConfig;
use crate::table::sort_merge::{BufferedBatch, MergeFunction, MergeResult, MergeRow};
use crate::{Error, Result};

struct FullChangelogMergeFunction {
    inner: Box<dyn MergeFunction>,
    max_level: i32,
    changelog: ChangelogGenerator,
}

pub(super) fn new_merge_function(
    config: &KeyValueReadConfig,
    fields: &[DataField],
    max_level: i32,
) -> Result<Box<dyn MergeFunction>> {
    Ok(Box::new(FullChangelogMergeFunction {
        // Preserve the compaction reducer's live-state replay guards while
        // allowing full output to discard states consisting only of retracts.
        inner: super::new_merge_function(config, fields, max_level)?,
        max_level,
        changelog: ChangelogGenerator::new(config, fields)?,
    }))
}

impl MergeFunction for FullChangelogMergeFunction {
    fn merge(
        &self,
        rows: &[MergeRow],
        batches: &[BufferedBatch],
        columns: &[usize],
        schema: &SchemaRef,
    ) -> Result<MergeResult> {
        self.merge_with_changelog(rows, batches, columns, schema)
            .map(|(result, _)| result)
    }

    fn merge_with_changelog(
        &self,
        rows: &[MergeRow],
        batches: &[BufferedBatch],
        columns: &[usize],
        schema: &SchemaRef,
    ) -> Result<(MergeResult, Option<RecordBatch>)> {
        let mut previous = None;
        for row in rows {
            if row.level(batches)? == self.max_level {
                if previous.is_some() {
                    return Err(Error::DataInvalid {
                        message: "Full compaction found multiple highest-level rows for one key"
                            .into(),
                        source: None,
                    });
                }
                previous = Some(row);
            }
        }
        let result = self.inner.merge(rows, batches, columns, schema)?;
        let after = live_row(result_row(&result, batches, columns, schema)?)?;
        let result = if after.is_some() {
            result
        } else {
            MergeResult::Omit
        };
        // A highest-level singleton is already published state. In particular,
        // default non-deduplicating changelogs must not report it as an update.
        if previous.is_some() && rows.len() == 1 {
            return Ok((result, None));
        }
        let before = live_row(
            previous
                .map(|row| source_row(row.batch_idx, row.row_idx, batches, columns, schema))
                .transpose()?,
        )?;
        Ok((result, self.changelog.generate(before, after, schema)?))
    }
}
