"""Split + replication planner.

Scenario: a topic is split (e.g. 4 -> 8 partitions) while producers still
route with the old key mapping, and replication tasks may have started
before the offset mapping was finalized.  The planner computes, per new
partition:

  * copy start/end offsets (end is extended by late producer writes),
  * the recovery point for interrupted replication,
  * duplicate ranges a consumer may re-read,
  * the conditions that must hold before switching consumers,
and a global switch order.
"""

from typing import Dict, List, Optional, Sequence, Set, Tuple

from .models import (
    KeyMapping,
    NewPartitionPlan,
    OldPartitionState,
    ReplicaState,
    ReplicaStatus,
    SplitPlan,
)


class SplitPlanner:
    def __init__(
        self,
        old_partitions,          # Sequence[OldPartitionState]
        new_partition_count,     # int
        key_mapping,             # KeyMapping
        replicas,                # Sequence[ReplicaState]
    ):
        if new_partition_count <= 0:
            raise ValueError("new_partition_count must be positive")
        self.old_partitions = {p.partition: p for p in old_partitions}
        if not self.old_partitions:
            raise ValueError("at least one old partition is required")
        self.new_partition_count = new_partition_count
        self.key_mapping = key_mapping
        self.replicas = {r.new_partition: r for r in replicas}
        self.warnings = []

    # ------------------------------------------------------------------
    # key routing migration
    # ------------------------------------------------------------------
    def _derive_sources(self):
        """new partition -> set of old partitions its keys come from."""
        sources = {}
        for key, (old_p, new_p) in sorted(self.key_mapping.assignments.items()):
            if old_p not in self.old_partitions:
                self.warnings.append(
                    "key %r routed from unknown old partition %d" % (key, old_p)
                )
                continue
            if not 0 <= new_p < self.new_partition_count:
                self.warnings.append(
                    "key %r routed to out-of-range new partition %d" % (key, new_p)
                )
                continue
            sources.setdefault(new_p, set()).add(old_p)
            if key in self.key_mapping.unmigrated_keys:
                self.warnings.append(
                    "key %r not yet migrated to post-split routing" % key
                )
        for new_p in sorted(sources):
            olds = sources[new_p]
            if len(olds) > 1:
                self.warnings.append(
                    "new partition %d draws keys from multiple old partitions %s"
                    % (new_p, sorted(olds))
                )
        return sources

    def _source_for(self, new_p, sources):
        replica = self.replicas.get(new_p)
        mapped = sources.get(new_p)
        if mapped and len(mapped) == 1:
            mapped_src = next(iter(mapped))
            if replica is not None and replica.source_old_partition != mapped_src:
                self.warnings.append(
                    "new partition %d: replica copies from old %d but key mapping "
                    "says old %d" % (new_p, replica.source_old_partition, mapped_src)
                )
            return mapped_src
        if replica is not None:
            return replica.source_old_partition
        return None

    # ------------------------------------------------------------------
    # per-partition planning
    # ------------------------------------------------------------------
    def _plan_partition(self, new_p, sources):
        replica = self.replicas.get(new_p)
        source = self._source_for(new_p, sources)
        if source is None or source not in self.old_partitions:
            self.warnings.append(
                "new partition %d has no determinable source old partition" % new_p
            )
            source = -1
            old = OldPartitionState(partition=-1, log_end_offset=0)
        else:
            old = self.old_partitions[source]

        planned_end = replica.planned_end_offset if replica else 0
        copied = replica.copied_offset if replica else 0
        status = replica.status if replica else ReplicaStatus.PENDING

        # --- late writes from old producers extend the copy end ---
        copy_end = max(planned_end, old.log_end_offset)
        catchup_range = None
        if old.log_end_offset > planned_end:
            catchup_range = (planned_end, old.log_end_offset)
            self.warnings.append(
                "new partition %d: %d late write(s) on old partition %d after "
                "replication was planned"
                % (new_p, old.log_end_offset - planned_end, source)
            )

        # --- recovery point / restart duplicates ---
        recovery_point = copied
        duplicate_ranges = []
        if replica and replica.resume_offset is not None:
            recovery_point = replica.resume_offset
            if replica.resume_offset < copied:
                duplicate_ranges.append((replica.resume_offset, copied))
                self.warnings.append(
                    "new partition %d: resume offset %d is behind copied offset %d; "
                    "re-copy creates duplicates"
                    % (new_p, replica.resume_offset, copied)
                )
        if status == ReplicaStatus.INTERRUPTED:
            self.warnings.append(
                "new partition %d: replication interrupted at offset %d, resume "
                "from recovery point %d" % (new_p, copied, recovery_point)
            )

        # --- consumer re-read duplicates: replication started before the
        #     offset mapping was finalized, so a switched consumer cannot
        #     translate its committed offset and restarts from 0 ---
        mapping_ready = self.key_mapping.finalized and (
            replica.mapping_finalized_at_start if replica else False
        )
        if not mapping_ready and old.committed_offset > 0:
            duplicate_ranges.append((0, old.committed_offset))

        # --- switch conditions ---
        conditions = []
        pending = []

        caught_up = status == ReplicaStatus.COMPLETED and copied >= copy_end
        cond = "replication caught up to old LEO %d (copied %d)" % (copy_end, copied)
        conditions.append(cond)
        if not caught_up:
            pending.append(cond)

        cond = "offset mapping finalized before consumer switch"
        conditions.append(cond)
        if not mapping_ready:
            pending.append(cond)

        cond = "old producers fenced on partition %d (no further late writes)" % source
        conditions.append(cond)
        if catchup_range is not None:
            pending.append(
                cond + "; catch-up range %s still open" % (catchup_range,)
            )

        cond = "consumer start offset resolved via finalized mapping"
        conditions.append(cond)
        if not mapping_ready:
            pending.append(cond)

        cond = "all producers migrated to post-split key routing"
        conditions.append(cond)
        stale = [
            key
            for key in sorted(self.key_mapping.unmigrated_keys)
            if self.key_mapping.old_partition_of(key) == source
        ]
        if stale:
            pending.append(cond + "; stragglers on old partition %d: %s"
                         % (source, stale))

        return NewPartitionPlan(
            new_partition=new_p,
            source_old_partition=source,
            copy_start=0,
            copy_end=copy_end,
            catchup_range=catchup_range,
            recovery_point=recovery_point,
            duplicate_ranges=duplicate_ranges,
            switch_conditions=conditions,
            pending_conditions=pending,
            ready_to_switch=not pending,
        )

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------
    def build_plan(self):
        sources = self._derive_sources()
        plans = [
            self._plan_partition(new_p, sources)
            for new_p in range(self.new_partition_count)
        ]

        # Switch ready partitions first, least remaining work first, so
        # consumers migrate off the old partitions as early as possible.
        def sort_key(plan):
            remaining = max(0, plan.copy_end - plan.recovery_point)
            return (0 if plan.ready_to_switch else 1, remaining, plan.new_partition)

        switch_order = [p.new_partition for p in sorted(plans, key=sort_key)]

        return SplitPlan(
            old_partition_count=len(self.old_partitions),
            new_partition_count=self.new_partition_count,
            partitions=plans,
            switch_order=switch_order,
            warnings=list(self.warnings),
        )
