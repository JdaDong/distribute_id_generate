"""
号段模式（Segment）分布式 ID 生成器

原理：
  每次从存储层（数据库/文件）批量获取一段连续 ID，在内存中分配，用完再取下一段。
  类似于美团 Leaf-Segment 的设计。

特点：
- 减少数据库访问频率，吞吐量高
- ID 趋势递增
- 支持双 Buffer 预加载，避免取号段时阻塞
- 服务重启可能浪费部分号段
"""

import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class Segment:
    """号段"""
    current: int = 0      # 当前值
    max_id: int = 0       # 号段最大值
    step: int = 0         # 步长
    min_id: int = 0       # 号段最小值

    @property
    def remaining(self) -> int:
        return self.max_id - self.current

    @property
    def idle_percent(self) -> float:
        if self.step == 0:
            return 0
        return self.remaining / self.step


@dataclass
class SegmentBuffer:
    """双 Buffer 号段容器"""
    key: str
    segments: list = field(default_factory=lambda: [Segment(), Segment()])
    current_index: int = 0         # 当前使用的 segment 索引
    is_next_ready: bool = False    # 下一个 segment 是否准备好
    is_loading: bool = False       # 是否正在加载下一个 segment
    lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def current(self) -> Segment:
        return self.segments[self.current_index]

    @property
    def next(self) -> Segment:
        return self.segments[(self.current_index + 1) % 2]

    def switch(self):
        """切换到下一个 segment"""
        self.current_index = (self.current_index + 1) % 2
        self.is_next_ready = False


class SegmentIDAllocator:
    """
    号段模式 ID 分配器

    模拟数据库存储，使用内存字典代替。
    生产环境应替换为真实数据库（如 MySQL）。

    数据库表结构示例:
    CREATE TABLE id_alloc (
        biz_tag VARCHAR(128) PRIMARY KEY COMMENT '业务标识',
        max_id  BIGINT NOT NULL COMMENT '当前最大ID',
        step    INT NOT NULL COMMENT '号段步长',
        description VARCHAR(256) COMMENT '描述',
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
    """

    # 预加载阈值：当前号段剩余不足该比例时，预加载下一号段
    LOAD_THRESHOLD = 0.3

    def __init__(self, step: int = 1000):
        """
        初始化号段分配器

        Args:
            step: 每次获取的号段大小
        """
        self.step = step
        self.buffers: Dict[str, SegmentBuffer] = {}
        self._lock = threading.Lock()

        # 模拟数据库存储 {biz_tag: max_id}
        self._db: Dict[str, int] = {}
        self._db_lock = threading.Lock()

    def _load_segment_from_db(self, biz_tag: str) -> Segment:
        """
        从数据库加载一个号段（模拟）

        相当于执行:
        UPDATE id_alloc SET max_id = max_id + step WHERE biz_tag = ?
        SELECT max_id, step FROM id_alloc WHERE biz_tag = ?
        """
        with self._db_lock:
            if biz_tag not in self._db:
                self._db[biz_tag] = 0

            old_max = self._db[biz_tag]
            new_max = old_max + self.step
            self._db[biz_tag] = new_max

            return Segment(
                current=old_max,
                max_id=new_max,
                step=self.step,
                min_id=old_max + 1,
            )

    def _ensure_buffer(self, biz_tag: str):
        """确保业务标识对应的 Buffer 存在并初始化"""
        if biz_tag not in self.buffers:
            with self._lock:
                if biz_tag not in self.buffers:
                    buf = SegmentBuffer(key=biz_tag)
                    # 加载第一个号段
                    seg = self._load_segment_from_db(biz_tag)
                    buf.segments[0] = seg
                    self.buffers[biz_tag] = buf

    def _async_load_next(self, buf: SegmentBuffer):
        """异步加载下一个号段"""
        def _load():
            try:
                seg = self._load_segment_from_db(buf.key)
                buf.next.current = seg.current
                buf.next.max_id = seg.max_id
                buf.next.step = seg.step
                buf.next.min_id = seg.min_id
                buf.is_next_ready = True
            finally:
                buf.is_loading = False

        buf.is_loading = True
        t = threading.Thread(target=_load, daemon=True)
        t.start()

    def generate(self, biz_tag: str = "default") -> int:
        """
        生成一个全局唯一的自增 ID

        Args:
            biz_tag: 业务标识，不同业务使用不同的号段

        Returns:
            int: 全局唯一 ID
        """
        self._ensure_buffer(biz_tag)
        buf = self.buffers[biz_tag]

        with buf.lock:
            seg = buf.current

            # 触发预加载
            if seg.idle_percent < self.LOAD_THRESHOLD and not buf.is_next_ready and not buf.is_loading:
                self._async_load_next(buf)

            # 当前号段还有余量
            if seg.current < seg.max_id:
                seg.current += 1
                return seg.current

            # 当前号段用完，切换到下一个
            if buf.is_next_ready:
                buf.switch()
                buf.current.current += 1
                return buf.current.current

            # 下一个号段还没准备好，同步等待
            if buf.is_loading:
                while not buf.is_next_ready:
                    time.sleep(0.001)
                buf.switch()
                buf.current.current += 1
                return buf.current.current

            # 都没有，直接同步加载
            seg_new = self._load_segment_from_db(biz_tag)
            buf.next.current = seg_new.current
            buf.next.max_id = seg_new.max_id
            buf.next.step = seg_new.step
            buf.next.min_id = seg_new.min_id
            buf.is_next_ready = True
            buf.switch()
            buf.current.current += 1
            return buf.current.current

    def get_info(self, biz_tag: str = "default") -> dict:
        """获取号段信息"""
        if biz_tag not in self.buffers:
            return {"error": "业务标识不存在"}
        buf = self.buffers[biz_tag]
        seg = buf.current
        return {
            "biz_tag": biz_tag,
            "current": seg.current,
            "max_id": seg.max_id,
            "remaining": seg.remaining,
            "step": seg.step,
            "db_max_id": self._db.get(biz_tag, 0),
        }


def demo():
    print("=" * 60)
    print("号段模式（Segment）ID 生成器")
    print("=" * 60)

    allocator = SegmentIDAllocator(step=1000)

    # 单业务生成
    print("\n[订单业务] 生成 10 个 ID：")
    for i in range(10):
        uid = allocator.generate("order")
        print(f"  [{i+1:2d}] {uid}")

    # 多业务隔离
    print("\n[用户业务] 生成 5 个 ID：")
    for i in range(5):
        uid = allocator.generate("user")
        print(f"  [{i+1}] {uid}")

    print("\n号段信息：")
    for tag in ["order", "user"]:
        info = allocator.get_info(tag)
        print(f"  {tag}: {info}")

    # 性能测试
    print("\n性能测试：生成 10 万个 ID...")
    start = time.time()
    for _ in range(100_000):
        allocator.generate("perf_test")
    elapsed = time.time() - start
    print(f"  耗时: {elapsed:.3f}s")
    print(f"  QPS:  {100_000 / elapsed:,.0f} ID/s")

    # 多线程测试
    print("\n多线程测试（4 线程各生成 1 万个 ID）：")
    results = []
    lock = threading.Lock()

    def worker():
        local_ids = []
        for _ in range(10_000):
            local_ids.append(allocator.generate("concurrent"))
        with lock:
            results.extend(local_ids)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - start

    unique_count = len(set(results))
    print(f"  总数: {len(results)}, 去重后: {unique_count}")
    print(f"  唯一性: {'✅ 通过' if unique_count == len(results) else '❌ 有重复'}")
    print(f"  耗时: {elapsed:.3f}s, QPS: {len(results) / elapsed:,.0f} ID/s")


if __name__ == "__main__":
    demo()
