"""Tests for the split / replication planner.

All broker states in these tests are simulated fixtures.
"""

import unittest

from split_planner import (
    KeyMapping,
    OldPartitionState,
    ReplicaState,
    ReplicaStatus,
    SplitPlanner,
)


def make_old_partitions(leos, committed=None):
    committed = committed or {}
    return [
        OldPartitionState(
            partition=p,
            log_end_offset=leo,
            committed_offset=committed.get(p, 0),
        )
        for p, leo in enumerate(leos)
    ]


def make_split_key_mapping(old_count=4, keys_per_old=2, finalized=True):
    """Simulate a 4 -> 8 split: old p feeds new p and new p + old_count."""
    assignments = {}
    for old_p in range(old_count):
        for i in range(keys_per_old):
            assignments["key-%d-%d" % (old_p, i)] = (old_p, old_p + i * old_count)
    return KeyMapping(assignments=assignments, finalized=finalized)


def make_replicas(new_count, old_count=4, overrides=None):
    overrides = overrides or {}
    replicas = []
    for new_p in range(new_count):
        kwargs = dict(
            new_partition=new_p,
            source_old_partition=new_p % old_count,
            planned_end_offset=100,
            copied_offset=100,
            status=ReplicaStatus.COMPLETED,
            mapping_finalized_at_start=True,
        )
        kwargs.update(overrides.get(new_p, {}))
        replicas.append(ReplicaState(**kwargs))
    return replicas


class KeyMigrationTest(unittest.TestCase):
    def test_sources_derived_from_key_mapping(self):
        planner = SplitPlanner(
            old_partitions=make_old_partitions([100, 100, 100, 100]),
            new_partition_count=8,
            key_mapping=make_split_key_mapping(),
            replicas=make_replicas(8),
        )
        plan = planner.build_plan()
        for p in plan.partitions:
            self.assertEqual(p.new_partition % 4, p.source_old_partition)
        self.assertEqual(plan.warnings, [])

    def test_unmigrated_key_warns(self):
        mapping = make_split_key_mapping()
        mapping.assignments["stale-key"] = (2, 6)
        mapping.unmigrated_keys = frozenset({"stale-key"})
        planner = SplitPlanner(
            old_partitions=make_old_partitions([100, 100, 100, 100]),
            new_partition_count=8,
            key_mapping=mapping,
            replicas=make_replicas(8),
        )
        plan = planner.build_plan()
        self.assertTrue(any("not yet migrated" in w for w in plan.warnings))
        self.assertFalse(plan.partitions[6].ready_to_switch)

    def test_conflicting_sources_warn(self):
        mapping = make_split_key_mapping()
        mapping.assignments["rogue-key"] = (3, 0)  # new 0 already fed by old 0
        planner = SplitPlanner(
            old_partitions=make_old_partitions([100, 100, 100, 100]),
            new_partition_count=8,
            key_mapping=mapping,
            replicas=make_replicas(8),
        )
        plan = planner.build_plan()
        self.assertTrue(any("multiple old partitions" in w for w in plan.warnings))

    def test_replica_source_mismatch_warns(self):
        replicas = make_replicas(8)
        replicas[5].source_old_partition = 2  # mapping says old 1
        planner = SplitPlanner(
            old_partitions=make_old_partitions([100, 100, 100, 100]),
            new_partition_count=8,
            key_mapping=make_split_key_mapping(),
            replicas=replicas,
        )
        plan = planner.build_plan()
        self.assertTrue(any("replica copies from old 2" in w for w in plan.warnings))
        self.assertEqual(plan.partitions[5].source_old_partition, 1)


class OffsetMappingTest(unittest.TestCase):
    def test_copy_range_from_planned_end(self):
        planner = SplitPlanner(
            old_partitions=make_old_partitions([100, 100, 100, 100]),
            new_partition_count=8,
            key_mapping=make_split_key_mapping(),
            replicas=make_replicas(8),
        )
        plan = planner.build_plan()
        for p in plan.partitions:
            self.assertEqual(p.copy_start, 0)
            self.assertEqual(p.copy_end, 100)
            self.assertIsNone(p.catchup_range)
            self.assertTrue(p.ready_to_switch)

    def test_late_writes_extend_copy_end(self):
        replicas = make_replicas(8)
        replicas[1].planned_end_offset = 100
        replicas[1].copied_offset = 100
        replicas[1].status = ReplicaStatus.RUNNING
        old = make_old_partitions([100, 130, 100, 100])  # old 1 grew after planning
        planner = SplitPlanner(
            old_partitions=old,
            new_partition_count=8,
            key_mapping=make_split_key_mapping(),
            replicas=replicas,
        )
        plan = planner.build_plan()
        for new_p in (1, 5):  # both fed by old partition 1
            p = plan.partitions[new_p]
            self.assertEqual(p.copy_end, 130)
            self.assertEqual(p.catchup_range, (100, 130))
            self.assertFalse(p.ready_to_switch)
        self.assertTrue(any("late write" in w for w in plan.warnings))

    def test_consumer_reread_duplicates_when_mapping_not_finalized(self):
        old = make_old_partitions([100, 100, 100, 100], committed={0: 40})
        replicas = make_replicas(8, overrides={0: dict(mapping_finalized_at_start=False)})
        planner = SplitPlanner(
            old_partitions=old,
            new_partition_count=8,
            key_mapping=make_split_key_mapping(finalized=False),
            replicas=replicas,
        )
        plan = planner.build_plan()
        self.assertIn((0, 40), plan.partitions[0].duplicate_ranges)
        self.assertFalse(plan.partitions[0].ready_to_switch)

    def test_no_duplicates_when_mapping_finalized(self):
        old = make_old_partitions([100, 100, 100, 100], committed={0: 40})
        planner = SplitPlanner(
            old_partitions=old,
            new_partition_count=8,
            key_mapping=make_split_key_mapping(finalized=True),
            replicas=make_replicas(8),
        )
        plan = planner.build_plan()
        for p in plan.partitions:
            self.assertEqual(p.duplicate_ranges, [])


class ReplicationFailureTest(unittest.TestCase):
    def test_interrupted_replica_has_recovery_point(self):
        replicas = make_replicas(
            8,
            overrides={
                3: dict(
                    status=ReplicaStatus.INTERRUPTED,
                    copied_offset=55,
                )
            }
        )
        planner = SplitPlanner(
            old_partitions=make_old_partitions([100, 100, 100, 100]),
            new_partition_count=8,
            key_mapping=make_split_key_mapping(),
            replicas=replicas,
        )
        plan = planner.build_plan()
        p = plan.partitions[3]
        self.assertEqual(p.recovery_point, 55)
        self.assertFalse(p.ready_to_switch)
        self.assertTrue(any("interrupted" in w for w in plan.warnings))

    def test_resume_behind_copied_offset_marks_duplicates(self):
        replicas = make_replicas(
            8,
            overrides={
                2: dict(
                    status=ReplicaStatus.RUNNING,
                    copied_offset=80,
                    resume_offset=50,
                )
            }
        )
        planner = SplitPlanner(
            old_partitions=make_old_partitions([100, 100, 100, 100]),
            new_partition_count=8,
            key_mapping=make_split_key_mapping(),
            replicas=replicas,
        )
        plan = planner.build_plan()
        p = plan.partitions[2]
        self.assertEqual(p.recovery_point, 50)
        self.assertIn((50, 80), p.duplicate_ranges)
        self.assertTrue(any("duplicates" in w for w in plan.warnings))

    def test_ready_after_resume_completes(self):
        replicas = make_replicas(
            8,
            overrides={
                2: dict(
                    status=ReplicaStatus.COMPLETED,
                    copied_offset=100,
                    resume_offset=None,
                )
            }
        )
        planner = SplitPlanner(
            old_partitions=make_old_partitions([100, 100, 100, 100]),
            new_partition_count=8,
            key_mapping=make_split_key_mapping(),
            replicas=replicas,
        )
        plan = planner.build_plan()
        self.assertTrue(plan.partitions[2].ready_to_switch)

    def test_switch_order_ready_first_least_work_first(self):
        replicas = make_replicas(
            8,
            overrides={
                1: dict(status=ReplicaStatus.RUNNING, copied_offset=90),
                2: dict(status=ReplicaStatus.RUNNING, copied_offset=10),
            }
        )
        planner = SplitPlanner(
            old_partitions=make_old_partitions([100, 100, 100, 100]),
            new_partition_count=8,
            key_mapping=make_split_key_mapping(),
            replicas=replicas,
        )
        plan = planner.build_plan()
        ready = [p.new_partition for p in plan.partitions if p.ready_to_switch]
        self.assertEqual(plan.switch_order[: len(ready)], sorted(ready))
        # among the not-ready: partition 1 (10 remaining) before 2 (90 remaining)
        tail = plan.switch_order[len(ready):]
        self.assertEqual(tail, [1, 2])

    def test_invalid_replica_status_rejected(self):
        with self.assertRaises(ValueError):
            ReplicaState(
                new_partition=0,
                source_old_partition=0,
                planned_end_offset=100,
                status="bogus",
            )


if __name__ == "__main__":
    unittest.main()
