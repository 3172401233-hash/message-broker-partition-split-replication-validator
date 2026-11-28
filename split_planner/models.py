"""Data model for the partition split / replication planner.

All broker states (log end offsets, replica progress, committed
watermarks) are *simulated* inputs; nothing here talks to a real broker.
"""

from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Tuple


class ReplicaStatus:
    PENDING = "pending"
    RUNNING = "running"
    INTERRUPTED = "interrupted"
    COMPLETED = "completed"

    ALL = (PENDING, RUNNING, INTERRUPTED, COMPLETED)


@dataclass
class OldPartitionState:
    """Simulated state of one pre-split partition."""

    partition: int
    log_end_offset: int          # current LEO on the old partition
    committed_offset: int = 0    # consumer group committed watermark


@dataclass
class KeyMapping:
    """Key routing: key -> (old_partition, new_partition).

    `finalized` is False while producers/consumers may still resolve
    routes with the old (pre-split) mapping.
    `unmigrated_keys` are keys whose producers still send with the
    pre-split routing (late key-migration stragglers).
    """

    assignments: Dict[str, Tuple[int, int]] = field(default_factory=dict)
    finalized: bool = False
    unmigrated_keys: FrozenSet[str] = frozenset()

    def old_partition_of(self, key):
        entry = self.assignments.get(key)
        return entry[0] if entry else None

    def new_partition_of(self, key):
        entry = self.assignments.get(key)
        return entry[1] if entry else None


@dataclass
class ReplicaState:
    """Simulated replication progress for one new partition."""

    new_partition: int
    source_old_partition: int
    planned_end_offset: int          # old-partition LEO snapshot at task creation
    copied_offset: int = 0           # contiguous copied prefix (exclusive)
    status: str = ReplicaStatus.PENDING
    resume_offset: Optional[int] = None  # restart point after an interruption
    mapping_finalized_at_start: bool = False  # was offset mapping ready at copy start

    def __post_init__(self):
        if self.status not in ReplicaStatus.ALL:
            raise ValueError("unknown replica status: %r" % self.status)


@dataclass
class NewPartitionPlan:
    new_partition: int
    source_old_partition: int
    copy_start: int
    copy_end: int
    catchup_range: Optional[Tuple[int, int]]   # late-write tail, if any
    recovery_point: int
    duplicate_ranges: List[Tuple[int, int]]
    switch_conditions: List[str]
    pending_conditions: List[str]
    ready_to_switch: bool


@dataclass
class SplitPlan:
    old_partition_count: int
    new_partition_count: int
    partitions: List[NewPartitionPlan]
    switch_order: List[int]
    warnings: List[str]
