"""
Redis 自增 ID 生成器

原理：
  利用 Redis 的 INCR/INCRBY 命令的原子性实现全局递增 ID。
  支持添加日期前缀，便于按时间维度管理。

特点：
- 严格递增
- 高性能（Redis 单线程保证原子性）
- 需依赖 Redis 服务
- 可按业务/日期分 key

本实现提供两种模式：
1. 连接真实 Redis（需安装 redis-py）
2. 内存模拟模式（无需 Redis，用于演示和测试）
"""

import time
import threading
from datetime import datetime
from typing import Optional


class MockRedis:
    """模拟 Redis 客户端（用于无 Redis 环境的演示）"""

    def __init__(self):
        self._data = {}
        self._lock = threading.Lock()

    def incr(self, key: str) -> int:
        with self._lock:
            self._data[key] = self._data.get(key, 0) + 1
            return self._data[key]

    def incrby(self, key: str, amount: int) -> int:
        with self._lock:
            self._data[key] = self._data.get(key, 0) + amount
            return self._data[key]

    def get(self, key: str) -> Optional[str]:
        return str(self._data.get(key, 0))

    def set(self, key: str, value) -> bool:
        with self._lock:
            self._data[key] = int(value)
        return True

    def expire(self, key: str, seconds: int) -> bool:
        return True

    def ping(self) -> bool:
        return True


class RedisIDGenerator:
    """
    Redis 自增 ID 生成器

    支持多种 ID 格式：
    - 纯数字递增: 1, 2, 3, ...
    - 日期前缀:   20260413:000001
    - 业务前缀:   ORDER:20260413:000001
    """

    def __init__(self, redis_client=None, host: str = "localhost", port: int = 6379, db: int = 0):
        """
        初始化 Redis ID 生成器

        Args:
            redis_client: 已有的 Redis 客户端实例（传入则直接使用）
            host: Redis 主机地址
            port: Redis 端口
            db: Redis 数据库编号
        """
        if redis_client:
            self.redis = redis_client
        else:
            try:
                import redis
                self.redis = redis.Redis(host=host, port=port, db=db, decode_responses=True)
                self.redis.ping()
                print(f"✅ 已连接 Redis: {host}:{port}/{db}")
            except Exception:
                print("⚠️  Redis 连接失败，使用内存模拟模式")
                self.redis = MockRedis()

    def generate(self, biz_tag: str = "global") -> int:
        """
        生成简单递增 ID

        Args:
            biz_tag: 业务标识

        Returns:
            int: 递增 ID
        """
        key = f"id:gen:{biz_tag}"
        return self.redis.incr(key)

    def generate_with_date(self, biz_tag: str = "global", date_format: str = "%Y%m%d") -> str:
        """
        生成带日期前缀的 ID

        格式: {日期}:{序列号(补零6位)}
        例如: 20260413:000001

        Args:
            biz_tag: 业务标识
            date_format: 日期格式

        Returns:
            str: 带日期前缀的 ID
        """
        date_str = datetime.now().strftime(date_format)
        key = f"id:gen:{biz_tag}:{date_str}"
        seq = self.redis.incr(key)
        # 设置过期时间（2天），避免 key 无限增长
        self.redis.expire(key, 86400 * 2)
        return f"{date_str}:{seq:06d}"

    def generate_with_prefix(self, biz_tag: str, prefix: str = "") -> str:
        """
        生成带业务前缀的 ID

        格式: {前缀}{日期}{序列号}
        例如: ORD20260413000001

        Args:
            biz_tag: 业务标识
            prefix: 业务前缀

        Returns:
            str: 带前缀的 ID
        """
        date_str = datetime.now().strftime("%Y%m%d")
        key = f"id:gen:{biz_tag}:{date_str}"
        seq = self.redis.incr(key)
        self.redis.expire(key, 86400 * 2)
        return f"{prefix}{date_str}{seq:06d}"

    def batch_generate(self, biz_tag: str = "global", count: int = 10) -> list:
        """
        批量生成 ID（利用 INCRBY 减少网络往返）

        Args:
            biz_tag: 业务标识
            count: 批量数量

        Returns:
            list: ID 列表
        """
        key = f"id:gen:{biz_tag}"
        max_id = self.redis.incrby(key, count)
        return list(range(max_id - count + 1, max_id + 1))

    def get_current(self, biz_tag: str = "global") -> int:
        """获取当前 ID 值"""
        key = f"id:gen:{biz_tag}"
        val = self.redis.get(key)
        return int(val) if val else 0


def demo():
    print("=" * 60)
    print("Redis 自增 ID 生成器")
    print("=" * 60)

    gen = RedisIDGenerator()

    # 简单递增
    print("\n[简单递增]")
    for i in range(5):
        uid = gen.generate("order")
        print(f"  [{i+1}] {uid}")

    # 带日期前缀
    print("\n[日期前缀]")
    for i in range(5):
        uid = gen.generate_with_date("order")
        print(f"  [{i+1}] {uid}")

    # 带业务前缀
    print("\n[业务前缀]")
    for i in range(5):
        uid = gen.generate_with_prefix("order", prefix="ORD")
        print(f"  [{i+1}] {uid}")

    # 批量生成
    print("\n[批量生成 10 个]")
    ids = gen.batch_generate("batch", count=10)
    for i, uid in enumerate(ids):
        print(f"  [{i+1:2d}] {uid}")

    # 多业务隔离
    print("\n[多业务隔离]")
    print(f"  订单: {gen.generate('order')}")
    print(f"  用户: {gen.generate('user')}")
    print(f"  支付: {gen.generate('payment')}")

    # 性能测试
    print("\n性能测试：生成 10 万个 ID...")
    start = time.time()
    for _ in range(100_000):
        gen.generate("perf")
    elapsed = time.time() - start
    print(f"  耗时: {elapsed:.3f}s")
    print(f"  QPS:  {100_000 / elapsed:,.0f} ID/s")


if __name__ == "__main__":
    demo()
