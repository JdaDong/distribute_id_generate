"""
数据库自增 ID 生成器

原理：
  利用数据库（如 MySQL）的 AUTO_INCREMENT 特性生成全局唯一 ID。
  本实现使用 SQLite 模拟，原理相同。

策略：
1. 单实例自增 - 简单直接
2. 多实例步长模式 - 多个数据库实例分别负责不同步长的 ID
   例如: 实例1 → 1,3,5,7...  实例2 → 2,4,6,8...
3. 批量获取 - 一次获取多个 ID，减少数据库访问

特点：
- 严格递增
- 实现简单
- 单点瓶颈，扩展性有限
"""

import sqlite3
import threading
import time
import os
from typing import List, Optional


class DatabaseIDGenerator:
    """数据库自增 ID 生成器（使用 SQLite 模拟）"""

    def __init__(self, db_path: str = ":memory:"):
        """
        初始化数据库 ID 生成器

        Args:
            db_path: 数据库路径，默认使用内存数据库
        """
        self.db_path = db_path
        self._lock = threading.Lock()
        # 对内存数据库使用持久连接，避免多次 connect 导致表丢失
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._init_db()

    def _init_db(self):
        """初始化数据库表"""
        cursor = self._conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS id_generator (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                biz_tag VARCHAR(128) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_biz_tag ON id_generator(biz_tag)
        """)
        self._conn.commit()

    def _get_conn(self) -> sqlite3.Connection:
        """获取数据库连接"""
        return self._conn

    def generate(self, biz_tag: str = "default") -> int:
        """
        生成一个自增 ID

        Args:
            biz_tag: 业务标识

        Returns:
            int: 自增 ID
        """
        with self._lock:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO id_generator (biz_tag) VALUES (?)",
                (biz_tag,)
            )
            uid = cursor.lastrowid
            conn.commit()
            return uid

    def batch_generate(self, biz_tag: str = "default", count: int = 10) -> List[int]:
        """
        批量生成自增 ID

        Args:
            biz_tag: 业务标识
            count: 批量数量

        Returns:
            list: ID 列表
        """
        with self._lock:
            conn = self._get_conn()
            cursor = conn.cursor()
            ids = []
            for _ in range(count):
                cursor.execute(
                    "INSERT INTO id_generator (biz_tag) VALUES (?)",
                    (biz_tag,)
                )
                ids.append(cursor.lastrowid)
            conn.commit()
            return ids

    def get_current_max(self, biz_tag: str = "default") -> int:
        """获取当前最大 ID"""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT MAX(id) FROM id_generator WHERE biz_tag = ?",
            (biz_tag,)
        )
        result = cursor.fetchone()[0]
        return result or 0


class MultiInstanceIDGenerator:
    """
    多实例步长模式 ID 生成器

    通过设置不同的起始值和步长，让多个实例生成不重叠的 ID 序列。
    例如：
      实例0 (start=0, step=3): 0, 3, 6, 9, 12, ...
      实例1 (start=1, step=3): 1, 4, 7, 10, 13, ...
      实例2 (start=2, step=3): 2, 5, 8, 11, 14, ...
    """

    def __init__(self, instance_id: int, total_instances: int):
        """
        初始化多实例 ID 生成器

        Args:
            instance_id: 当前实例 ID（从 0 开始）
            total_instances: 实例总数
        """
        if instance_id < 0 or instance_id >= total_instances:
            raise ValueError(f"instance_id 必须在 0-{total_instances - 1} 之间")

        self.instance_id = instance_id
        self.total_instances = total_instances
        self.current = instance_id  # 起始值 = 实例ID
        self._lock = threading.Lock()

    def generate(self) -> int:
        """生成下一个 ID"""
        with self._lock:
            uid = self.current
            self.current += self.total_instances
            return uid


def demo():
    print("=" * 60)
    print("数据库自增 ID 生成器")
    print("=" * 60)

    # ---- 单实例模式 ----
    print("\n[单实例自增]")
    gen = DatabaseIDGenerator()

    print("生成 10 个 ID：")
    for i in range(10):
        uid = gen.generate("order")
        print(f"  [{i+1:2d}] {uid}")

    # 批量生成
    print("\n[批量生成 5 个]")
    ids = gen.batch_generate("batch", count=5)
    for i, uid in enumerate(ids):
        print(f"  [{i+1}] {uid}")

    # 多业务
    print("\n[多业务]")
    print(f"  订单: {gen.generate('order')}")
    print(f"  用户: {gen.generate('user')}")
    print(f"  当前订单最大ID: {gen.get_current_max('order')}")

    # ---- 多实例步长模式 ----
    print("\n" + "=" * 60)
    print("多实例步长模式")
    print("=" * 60)

    instances = [MultiInstanceIDGenerator(i, 3) for i in range(3)]

    print("\n3 个实例各生成 5 个 ID：")
    for inst_idx, inst in enumerate(instances):
        ids = [inst.generate() for _ in range(5)]
        print(f"  实例{inst_idx}: {ids}")

    # 验证不重叠
    print("\n验证唯一性：")
    all_ids = []
    instances2 = [MultiInstanceIDGenerator(i, 3) for i in range(3)]
    for inst in instances2:
        for _ in range(1000):
            all_ids.append(inst.generate())
    unique_count = len(set(all_ids))
    print(f"  总数: {len(all_ids)}, 去重后: {unique_count}")
    print(f"  唯一性: {'✅ 通过' if unique_count == len(all_ids) else '❌ 有重复'}")

    # 性能测试
    print("\n性能测试：单实例生成 1 万个 ID...")
    gen2 = DatabaseIDGenerator()
    start = time.time()
    for _ in range(10_000):
        gen2.generate("perf")
    elapsed = time.time() - start
    print(f"  耗时: {elapsed:.3f}s")
    print(f"  QPS:  {10_000 / elapsed:,.0f} ID/s")

    print("\n性能测试：多实例步长模式生成 10 万个 ID...")
    inst = MultiInstanceIDGenerator(0, 1)
    start = time.time()
    for _ in range(100_000):
        inst.generate()
    elapsed = time.time() - start
    print(f"  耗时: {elapsed:.3f}s")
    print(f"  QPS:  {100_000 / elapsed:,.0f} ID/s")


if __name__ == "__main__":
    demo()
