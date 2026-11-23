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

use super::lookup_changelog_tests::events;
use super::*;

async fn full_table(options: &[(&str, &str)]) -> Table {
    let mut configured = vec![("changelog-producer", "full-compaction")];
    configured.extend_from_slice(options);
    let path = format!("memory:/full_changelog/{}", uuid::Uuid::new_v4());
    let (io, table) = common::incremental_helpers::memory_table(&path, pk_schema(&configured));
    setup_dirs(&io, &path).await;
    persist_table_schema(&io, &path, table.schema()).await;
    table
}

async fn compact(table: &Table) -> (i64, Vec<CompactResult>) {
    let builder = table
        .new_compact_builder()
        .with_commit_user("full-changelog-test")
        .unwrap();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(table, &plan).await;
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results.clone())
        .await
        .unwrap();
    let id = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap();
    (id, results)
}

fn expected(events: &[(&str, i32, i32)]) -> Vec<(String, i32, i32)> {
    let mut events = events
        .iter()
        .map(|(kind, key, value)| (kind.to_string(), *key, *value))
        .collect::<Vec<_>>();
    events.sort();
    events
}

#[tokio::test]
async fn full_compaction_publishes_successive_states_and_keeps_unchanged_keys_silent() {
    for bucket in ["1", "-1"] {
        let table = full_table(&[
            ("bucket", bucket),
            ("num-levels", "3"),
            ("num-sorted-run.compaction-trigger", "2"),
            ("compaction.max-size-amplification-percent", "0"),
            ("target-file-row-num", "1"),
            ("read.batch-size", "1"),
        ])
        .await;
        write_batch(&table, &make_batch(vec![1, 2, 5], vec![10, 20, 50])).await;
        assert!(events(&table, 0, 1).await.is_empty());
        let (first, results) = compact(&table).await;
        assert!(results
            .iter()
            .flat_map(|result| result.files())
            .all(|file| file.level == 2));
        assert_eq!(
            events(&table, 0, first).await,
            expected(&[("+I", 1, 10), ("+I", 2, 20), ("+I", 5, 50)])
        );
        write_batch(
            &table,
            &make_batch_with_kinds(vec![1, 2, 3, 4], vec![11, 999, 30, 40], vec![0, 3, 0, 0]),
        )
        .await;
        write_batch(&table, &make_batch_with_kinds(vec![4], vec![999], vec![3])).await;
        let (second, _) = compact(&table).await;
        assert_eq!(
            events(&table, first, second).await,
            expected(&[("-U", 1, 10), ("+U", 1, 11), ("-D", 2, 20), ("+I", 3, 30)])
        );
        assert_eq!(rows(&table).await, vec![(1, 11), (3, 30), (5, 50)]);
        let snapshot = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.commit_kind(), &CommitKind::COMPACT);
        assert_eq!(snapshot.changelog_record_count(), Some(4));
        assert_eq!(compact(&table).await.0, second);
    }
}

#[tokio::test]
async fn intermediate_compaction_defers_changes_until_positive_runs_reach_the_last_level() {
    let table = full_table(&[
        ("num-levels", "3"),
        ("num-sorted-run.compaction-trigger", "2"),
    ])
    .await;
    write_batch(
        &table,
        &make_batch((0..1000).collect(), (0..1000).collect()),
    )
    .await;
    let (first, initial_results) = compact(&table).await;
    write_batch(
        &table,
        &make_batch_with_kinds(vec![42, 43], vec![4200, 0], vec![0, 3]),
    )
    .await;
    let (middle, results) = compact(&table).await;
    assert!(results
        .iter()
        .flat_map(|result| result.files())
        .all(|file| file.level == 1));
    assert!(results
        .iter()
        .all(|result| result.changelog_files().is_empty()));
    assert!(events(&table, first, middle).await.is_empty());
    let mut invalid = serde_json::to_value(&results).unwrap();
    invalid[0]["changelog_files"] =
        serde_json::to_value(initial_results[0].changelog_files()).unwrap();
    let error = table
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .commit_compact(serde_json::from_value(invalid).unwrap())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Only lookup or full compaction"));
    let mut stream = table.new_stream_scan().unwrap();
    let initial = stream.plan().await.unwrap().unwrap();
    assert!(!initial.splits().is_empty());
    assert!(initial
        .splits()
        .iter()
        .flat_map(|split| split.data_files())
        .all(|file| file.level == 2));
    let force_full = table.copy_with_options(std::collections::HashMap::from([(
        "compaction.max-size-amplification-percent".into(),
        "0".into(),
    )]));
    let plan = force_full
        .new_compact_builder()
        .new_planner()
        .unwrap()
        .plan()
        .await
        .unwrap();
    assert!(!plan.tasks().is_empty());
    assert!(plan
        .tasks()
        .iter()
        .flat_map(|task| task.files())
        .all(|file| file.level > 0));
    let (last, _) = compact(&force_full).await;
    assert_eq!(
        events(&table, first, last).await,
        expected(&[("-U", 42, 42), ("+U", 42, 4200), ("-D", 43, 43)])
    );
    assert_eq!(rows(&table).await.len(), 999);
}

#[tokio::test]
async fn full_compaction_can_publish_only_deletes_and_replay_the_result() {
    let table = full_table(&[
        ("num-sorted-run.compaction-trigger", "2"),
        ("compaction.max-size-amplification-percent", "0"),
    ])
    .await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    let (first, _) = compact(&table).await;
    write_batch(&table, &make_batch_with_kinds(vec![1], vec![999], vec![3])).await;
    let (last, results) = compact(&table).await;
    assert!(results.iter().all(|result| result.files().is_empty()));
    assert_eq!(
        events(&table, first, last).await,
        expected(&[("-D", 1, 10)])
    );
    assert!(rows(&table).await.is_empty());
    table
        .new_compact_builder()
        .with_commit_user("full-changelog-test")
        .unwrap()
        .new_commit()
        .unwrap()
        .commit_compact(serde_json::from_value(serde_json::to_value(results).unwrap()).unwrap())
        .await
        .unwrap();
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(last)
    );
}

#[tokio::test]
async fn full_compaction_respects_generated_changelog_filters() {
    for (options, changes) in [
        (
            vec![],
            expected(&[("-U", 1, 10), ("+U", 1, 10), ("-D", 2, 20)]),
        ),
        (
            vec![("changelog-producer.row-deduplicate", "true")],
            expected(&[("-D", 2, 20)]),
        ),
        (
            vec![("changelog-producer.ignore-update-before", "true")],
            expected(&[("+U", 1, 10), ("-D", 2, 20)]),
        ),
        (
            vec![("changelog-producer.ignore-delete", "true")],
            expected(&[("-U", 1, 10), ("+U", 1, 10)]),
        ),
        (
            vec![
                ("changelog-producer.row-deduplicate", "true"),
                ("changelog-producer.row-deduplicate-ignore-fields", "value"),
            ],
            expected(&[("-D", 2, 20)]),
        ),
    ] {
        let mut configured = vec![
            ("num-sorted-run.compaction-trigger", "2"),
            ("compaction.max-size-amplification-percent", "0"),
        ];
        configured.extend_from_slice(&options);
        let table = full_table(&configured).await;
        write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
        let (first, _) = compact(&table).await;
        let value = if options
            .iter()
            .any(|(key, _)| *key == "changelog-producer.row-deduplicate-ignore-fields")
        {
            11
        } else {
            10
        };
        write_batch(
            &table,
            &make_batch_with_kinds(vec![1, 2], vec![value, 999], vec![0, 3]),
        )
        .await;
        let (last, _) = compact(&table).await;
        assert_eq!(events(&table, first, last).await, changes, "{options:?}");
        assert_eq!(rows(&table).await, vec![(1, value)]);
    }
}

#[tokio::test]
async fn full_compaction_reduces_stateful_engines_before_generating_changes() {
    for (engine, options, value) in [
        (
            "partial-update",
            vec![
                ("ignore-delete", "false"),
                ("partial-update.remove-record-on-delete", "true"),
            ],
            11,
        ),
        (
            "aggregation",
            vec![("fields.value.aggregate-function", "sum")],
            21,
        ),
    ] {
        let mut configured = vec![
            ("merge-engine", engine),
            ("num-levels", "3"),
            ("num-sorted-run.compaction-trigger", "2"),
            ("compaction.max-size-amplification-percent", "0"),
        ];
        configured.extend_from_slice(&options);
        let table = full_table(&configured).await;
        write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
        let (first, results) = compact(&table).await;
        assert!(results
            .iter()
            .flat_map(|result| result.files())
            .all(|file| file.level == 2));
        write_batch(&table, &make_batch(vec![1], vec![11])).await;
        let (last, _) = compact(&table).await;
        assert_eq!(
            events(&table, first, last).await,
            expected(&[("-U", 1, 10), ("+U", 1, value)])
        );
        assert_eq!(rows(&table).await, vec![(1, value), (2, 20)]);
        write_batch(&table, &make_batch_with_kinds(vec![1], vec![5], vec![3])).await;
        let (deleted, _) = compact(&table).await;
        let changes = if engine == "aggregation" {
            expected(&[("-U", 1, value), ("+U", 1, value - 5)])
        } else {
            expected(&[("-D", 1, value)])
        };
        assert_eq!(events(&table, last, deleted).await, changes);
        assert_eq!(
            rows(&table).await,
            if engine == "aggregation" {
                vec![(1, value - 5), (2, 20)]
            } else {
                vec![(2, 20)]
            }
        );
    }
}

#[tokio::test]
async fn full_compaction_rejects_live_aggregate_state_that_cannot_be_replayed() {
    let table = full_table(&[
        ("merge-engine", "aggregation"),
        ("fields.value.aggregate-function", "first_non_null_value"),
        ("aggregation.remove-record-on-delete", "true"),
    ])
    .await;
    write_batch(&table, &make_batch_with_kinds(vec![1], vec![20], vec![3])).await;
    let null_insert = RecordBatch::try_from_iter([
        (
            "id",
            std::sync::Arc::new(Int32Array::from(vec![1])) as arrow_array::ArrayRef,
        ),
        (
            "value",
            std::sync::Arc::new(Int32Array::from(vec![None::<i32>])) as arrow_array::ArrayRef,
        ),
    ])
    .unwrap();
    write_batch(&table, &null_insert).await;
    assert_eq!(rows(&table).await, vec![(1, 20)]);
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let before = table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .len();
    let error = builder
        .new_executor()
        .unwrap()
        .execute(&plan.tasks()[0])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Compaction cannot preserve"));
    assert_eq!(
        table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .len(),
        before
    );
    write_batch(&table, &make_batch(vec![1], vec![30])).await;
    assert_eq!(rows(&table).await, vec![(1, 30)]);
    write_batch(&table, &make_batch_with_kinds(vec![1], vec![40], vec![3])).await;
    let (_, results) = compact(&table).await;
    assert!(results
        .iter()
        .all(|result| result.files().is_empty() && result.changelog_files().is_empty()));
    assert!(rows(&table).await.is_empty());
}

#[tokio::test]
async fn full_compaction_validates_levels_and_changelog_options() {
    for (key, value) in [
        ("num-levels", "1"),
        ("changelog-producer.row-deduplicate", "invalid"),
        ("changelog-producer.ignore-update-before", "invalid"),
        ("changelog-producer.ignore-delete", "invalid"),
    ] {
        let table = full_table(&[(key, value)]).await;
        let error = table.new_compact_builder().new_planner().err().unwrap();
        assert!(error.to_string().contains(key), "{error}");
    }
    let table = full_table(&[("num-levels", "3")]).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    compact(&table).await;
    let lower = table.copy_with_options(std::collections::HashMap::from([(
        "num-levels".into(),
        "2".into(),
    )]));
    let error = lower
        .new_compact_builder()
        .new_planner()
        .unwrap()
        .plan()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("configured num-levels"));
}
