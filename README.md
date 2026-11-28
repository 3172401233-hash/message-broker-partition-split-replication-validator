# message-broker-partition-split-replication-validator

验证分区拆分、副本复制和 offset 映射。

> **注意：本项目中所有 broker 状态（LEO、副本进度、提交水位等）均为模拟数据。**
> 规划器不连接任何真实 broker，仅根据输入的模拟状态计算切换计划。

## 场景

Topic 从 4 个分区拆成 8 个分区，但：

- 生产者仍按旧 key 路由（`hash(key) % 4`）写入，产生**晚写**（late writes）；
- 复制任务在 offset 映射完成前就已启动，消费者切换到新分区后可能**重复读到旧数据**；
- 复制可能**中断**，需要从恢复点续传，且回退续传会产生重复段。

## 功能

`split_planner.SplitPlanner` 输入旧分区状态、新分区数量、key 映射、
副本状态与提交水位，输出 `SplitPlan`：

- 每个新分区的复制起止点 `copy_start` / `copy_end`（晚写会延伸 `copy_end` 并生成 `catchup_range`）；
- 切换条件 `switch_conditions` 与未满足项 `pending_conditions`；
- 切换顺序 `switch_order`（可切换者优先、剩余工作量小者优先）；
- 重复范围 `duplicate_ranges`（offset 映射未就绪导致的消费者重读 + 续传回退段）；
- 恢复点 `recovery_point`（复制中断后的续传起点）。

## 运行测试

```bash
python -m unittest
```

兼容 Python 3.8.2。测试覆盖 key 迁移、offset 映射和复制失败三类场景。

## 目录结构

- `split_planner/models.py` — 输入/输出数据模型（全部为模拟状态）
- `split_planner/planner.py` — 拆分与复制规划逻辑
- `tests/test_planner.py` — 单元测试
