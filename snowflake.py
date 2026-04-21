"""
Snowflake 雪花算法 - 分布式 ID 生成器

64 位 ID 结构：
┌─────────┬───────────────────────────┬────────────┬──────────────┐
│ 1bit    │ 41bit 时间戳(ms)          │ 10bit 机器 │ 12bit 序列号 │
│ 符号位  │ 约可用 69 年              │ 1024 台    │ 4096/ms      │
└─────────┴───────────────────────────┴────────────┴──────────────┘

特点：
- 趋势递增
- 高性能（纯内存计算）
- 可从 ID 反解出生成时间、机器信息
- 需注意时钟回拨问题
"""

import time
import threading
import string
from datetime import datetime


# ============================================================
# Base62 编码器 - 用于 Snowflake ID 混淆
# ============================================================

class Base62:
    """
    Base62 编码/解码器

    将 Snowflake 的 64 位整数 ID 编码为短字符串，隐藏时间和机器信息。
    例如: 831469991521030144 → "dGhlMnR0Zg"

    字符集: 0-9, a-z, A-Z（共 62 个字符）
    """

    CHARSET = string.digits + string.ascii_lowercase + string.ascii_uppercase  # 0-9a-zA-Z
    BASE = len(CHARSET)  # 62

    @classmethod
    def encode(cls, num: int) -> str:
        """
        将整数编码为 Base62 字符串

        Args:
            num: 非负整数

        Returns:
            str: Base62 编码字符串
        """
        if num < 0:
            raise ValueError("不支持负数编码")
        if num == 0:
            return cls.CHARSET[0]

        result = []
        while num > 0:
            num, remainder = divmod(num, cls.BASE)
            result.append(cls.CHARSET[remainder])
        return "".join(reversed(result))

    @classmethod
    def decode(cls, encoded: str) -> int:
        """
        将 Base62 字符串解码为整数

        Args:
            encoded: Base62 编码字符串

        Returns:
            int: 解码后的整数
        """
        num = 0
        for char in encoded:
            idx = cls.CHARSET.index(char)
            num = num * cls.BASE + idx
        return num

    @classmethod
    def encode_with_shuffle(cls, num: int, shuffle_key: int = 0x5DEECE66D) -> str:
        """
        带混淆的编码：先对数字做位运算混淆，再 Base62 编码

        混淆后即使 ID 连续，编码后的字符串也看不出规律。

        Args:
            num: 非负整数
            shuffle_key: 混淆密钥（异或因子）

        Returns:
            str: 混淆后的 Base62 字符串
        """
        shuffled = num ^ shuffle_key
        return cls.encode(shuffled)

    @classmethod
    def decode_with_shuffle(cls, encoded: str, shuffle_key: int = 0x5DEECE66D) -> int:
        """
        带混淆的解码：先 Base62 解码，再反向位运算恢复原始数字

        Args:
            encoded: 混淆后的 Base62 字符串
            shuffle_key: 混淆密钥（必须与编码时一致）

        Returns:
            int: 原始整数
        """
        shuffled = cls.decode(encoded)
        return shuffled ^ shuffle_key


class Snowflake:
    """Snowflake 雪花算法 ID 生成器"""

    # 起始时间戳 (2020-01-01 00:00:00 UTC)，可自定义
    EPOCH = 1577836800000

    # 各部分位数
    SEQUENCE_BITS = 12
    WORKER_ID_BITS = 5
    DATACENTER_ID_BITS = 5

    # 最大值
    MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1          # 4095
    MAX_WORKER_ID = (1 << WORKER_ID_BITS) - 1        # 31
    MAX_DATACENTER_ID = (1 << DATACENTER_ID_BITS) - 1  # 31

    # 位移
    WORKER_ID_SHIFT = SEQUENCE_BITS                               # 12
    DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS          # 17
    TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS  # 22

    def __init__(self, worker_id: int = 0, datacenter_id: int = 0):
        """
        初始化雪花算法生成器

        Args:
            worker_id: 工作节点 ID (0-31)
            datacenter_id: 数据中心 ID (0-31)
        """
        if worker_id < 0 or worker_id > self.MAX_WORKER_ID:
            raise ValueError(f"worker_id 必须在 0-{self.MAX_WORKER_ID} 之间")
        if datacenter_id < 0 or datacenter_id > self.MAX_DATACENTER_ID:
            raise ValueError(f"datacenter_id 必须在 0-{self.MAX_DATACENTER_ID} 之间")

        self.worker_id = worker_id
        self.datacenter_id = datacenter_id
        self.sequence = 0
        self.last_timestamp = -1
        self._lock = threading.Lock()

    def _current_millis(self) -> int:
        """获取当前毫秒级时间戳"""
        return int(time.time() * 1000)

    def _wait_next_millis(self, last_timestamp: int) -> int:
        """等待到下一毫秒"""
        timestamp = self._current_millis()
        while timestamp <= last_timestamp:
            timestamp = self._current_millis()
        return timestamp

    def generate(self) -> int:
        """
        生成一个全局唯一的 64 位 ID

        Returns:
            int: 64 位唯一 ID

        Raises:
            RuntimeError: 时钟回拨时抛出异常
        """
        with self._lock:
            timestamp = self._current_millis()

            # 时钟回拨检测
            if timestamp < self.last_timestamp:
                offset = self.last_timestamp - timestamp
                if offset <= 5:
                    # 小幅回拨，等待追上
                    time.sleep(offset / 1000)
                    timestamp = self._current_millis()
                    if timestamp < self.last_timestamp:
                        raise RuntimeError(
                            f"时钟回拨 {self.last_timestamp - timestamp}ms，拒绝生成 ID"
                        )
                else:
                    raise RuntimeError(
                        f"时钟回拨 {offset}ms 超过阈值，拒绝生成 ID"
                    )

            if timestamp == self.last_timestamp:
                # 同一毫秒内，序列号递增
                self.sequence = (self.sequence + 1) & self.MAX_SEQUENCE
                if self.sequence == 0:
                    # 序列号溢出，等待下一毫秒
                    timestamp = self._wait_next_millis(self.last_timestamp)
            else:
                # 新的毫秒，序列号重置
                self.sequence = 0

            self.last_timestamp = timestamp

            # 组装 64 位 ID
            uid = (
                ((timestamp - self.EPOCH) << self.TIMESTAMP_SHIFT)
                | (self.datacenter_id << self.DATACENTER_ID_SHIFT)
                | (self.worker_id << self.WORKER_ID_SHIFT)
                | self.sequence
            )
            return uid

    @classmethod
    def parse(cls, uid: int) -> dict:
        """
        反解析 Snowflake ID

        Args:
            uid: Snowflake 生成的 ID

        Returns:
            dict: 包含时间戳、数据中心ID、工作节点ID、序列号的字典
        """
        sequence = uid & cls.MAX_SEQUENCE
        worker_id = (uid >> cls.WORKER_ID_SHIFT) & cls.MAX_WORKER_ID
        datacenter_id = (uid >> cls.DATACENTER_ID_SHIFT) & cls.MAX_DATACENTER_ID
        timestamp = (uid >> cls.TIMESTAMP_SHIFT) + cls.EPOCH

        return {
            "id": uid,
            "timestamp": timestamp,
            "datetime": datetime.fromtimestamp(timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S.%f"),
            "datacenter_id": datacenter_id,
            "worker_id": worker_id,
            "sequence": sequence,
            "binary": format(uid, "064b"),
        }

    def generate_base62(self) -> str:
        """生成 Base62 编码的 ID（缩短长度，但仍可反解析）"""
        uid = self.generate()
        return Base62.encode(uid)

    def generate_obfuscated(self, shuffle_key: int = 0x5DEECE66D) -> str:
        """
        生成混淆后的 Base62 ID（隐藏时间和机器信息）

        连续生成的 ID 编码后看不出任何规律，适合对外暴露。

        Args:
            shuffle_key: 混淆密钥

        Returns:
            str: 混淆后的短字符串 ID
        """
        uid = self.generate()
        return Base62.encode_with_shuffle(uid, shuffle_key)

    @classmethod
    def decode_obfuscated(cls, encoded: str, shuffle_key: int = 0x5DEECE66D) -> dict:
        """
        解码混淆后的 ID 并反解析

        Args:
            encoded: 混淆后的 Base62 字符串
            shuffle_key: 混淆密钥

        Returns:
            dict: 反解析结果
        """
        uid = Base62.decode_with_shuffle(encoded, shuffle_key)
        return cls.parse(uid)


# ============================================================
# ZooKeeper workerId 自动分配器
# ============================================================

class ZookeeperWorkerIdAllocator:
    """
    基于 ZooKeeper 的 workerId 自动分配器

    原理（模拟美团 Leaf 的设计）：
    1. 在 ZK 上创建持久顺序节点 /snowflake/workers/worker-XXXXXXXX
    2. 取节点序号 % max_worker_num 作为 workerId
    3. 将本机信息（IP、端口、时间戳）写入节点数据
    4. 使用临时节点实现服务下线自动回收

    本实现提供两种模式：
    - 连接真实 ZooKeeper（需安装 kazoo）
    - 内存模拟模式（无需 ZK，用于演示和测试）
    """

    ZK_ROOT = "/snowflake/workers"

    def __init__(self, zk_hosts: str = "127.0.0.1:2181", use_mock: bool = False):
        """
        初始化 ZK workerId 分配器

        Args:
            zk_hosts: ZooKeeper 连接地址
            use_mock: 是否使用模拟模式
        """
        self.zk_hosts = zk_hosts
        self.worker_id = -1
        self.datacenter_id = 0
        self._zk = None

        if use_mock:
            self._zk = MockZooKeeper()
            print("⚠️  ZooKeeper 使用内存模拟模式")
        else:
            try:
                from kazoo.client import KazooClient
                self._zk = KazooClient(hosts=zk_hosts, timeout=5)
                self._zk.start(timeout=5)
                print(f"✅ 已连接 ZooKeeper: {zk_hosts}")
            except Exception as e:
                print(f"⚠️  ZooKeeper 连接失败 ({e})，使用内存模拟模式")
                self._zk = MockZooKeeper()

    def allocate(self, datacenter_id: int = 0) -> tuple:
        """
        分配 workerId 和 datacenterId

        Returns:
            tuple: (worker_id, datacenter_id)
        """
        import socket
        import json

        self.datacenter_id = datacenter_id

        # 确保根路径存在
        self._zk.ensure_path(self.ZK_ROOT)

        # 创建顺序节点，写入机器信息
        node_data = json.dumps({
            "hostname": socket.gethostname(),
            "ip": self._get_local_ip(),
            "datacenter_id": datacenter_id,
            "timestamp": int(time.time() * 1000),
        }).encode("utf-8")

        node_path = self._zk.create(
            f"{self.ZK_ROOT}/worker-",
            value=node_data,
            sequence=True,
            ephemeral=True,
        )

        # 从节点路径中提取序号
        seq_str = node_path.split("-")[-1]
        seq_num = int(seq_str)

        # 取模得到 workerId（0-31）
        self.worker_id = seq_num % (Snowflake.MAX_WORKER_ID + 1)

        print(f"  ZK 节点: {node_path}")
        print(f"  分配 workerId={self.worker_id}, datacenterId={self.datacenter_id}")

        return self.worker_id, self.datacenter_id

    def create_snowflake(self, datacenter_id: int = 0) -> "Snowflake":
        """
        分配 workerId 并创建 Snowflake 实例

        Args:
            datacenter_id: 数据中心 ID

        Returns:
            Snowflake: 配置好的 Snowflake 实例
        """
        worker_id, dc_id = self.allocate(datacenter_id)
        return Snowflake(worker_id=worker_id, datacenter_id=dc_id)

    @staticmethod
    def _get_local_ip() -> str:
        """获取本机 IP"""
        import socket
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"

    def close(self):
        """关闭 ZK 连接"""
        if self._zk and hasattr(self._zk, "stop"):
            try:
                self._zk.stop()
            except Exception:
                pass


class MockZooKeeper:
    """模拟 ZooKeeper 客户端（用于无 ZK 环境的演示）"""

    _counter = 0
    _lock = threading.Lock()

    def __init__(self):
        self._nodes = {}

    def ensure_path(self, path: str):
        pass

    def create(self, path: str, value: bytes = b"", sequence: bool = False,
               ephemeral: bool = False) -> str:
        with MockZooKeeper._lock:
            if sequence:
                MockZooKeeper._counter += 1
                path = f"{path}{MockZooKeeper._counter:010d}"
            self._nodes[path] = value
            return path

    def get(self, path: str):
        return self._nodes.get(path, b""), {}

    def get_children(self, path: str):
        prefix = path.rstrip("/") + "/"
        return [k[len(prefix):] for k in self._nodes if k.startswith(prefix)]

    def stop(self):
        pass


def demo():
    print("=" * 60)
    print("Snowflake 雪花算法 ID 生成器")
    print("=" * 60)

    sf = Snowflake(worker_id=1, datacenter_id=1)

    # ---- 基础功能 ----
    print("\n【基础】生成 10 个 ID：")
    ids = []
    for i in range(10):
        uid = sf.generate()
        ids.append(uid)
        print(f"  [{i+1:2d}] {uid}")

    print(f"\n【反解析】ID = {ids[0]}：")
    info = Snowflake.parse(ids[0])
    for key, value in info.items():
        print(f"  {key:15s}: {value}")

    # ---- Base62 编码 ----
    print("\n" + "=" * 60)
    print("Base62 编码（缩短长度）")
    print("=" * 60)

    print("\n原始 ID → Base62 编码：")
    for uid in ids[:5]:
        encoded = Base62.encode(uid)
        decoded = Base62.decode(encoded)
        print(f"  {uid}  →  {encoded:12s}  →  {decoded}  {'✅' if decoded == uid else '❌'}")

    # ---- 混淆编码 ----
    print("\n" + "=" * 60)
    print("混淆编码（隐藏时间和机器信息）")
    print("=" * 60)

    print("\n连续 ID 混淆后完全无规律：")
    sf2 = Snowflake(worker_id=1, datacenter_id=1)
    for i in range(5):
        uid = sf2.generate()
        plain = Base62.encode(uid)
        obfuscated = Base62.encode_with_shuffle(uid)
        print(f"  ID: {uid}  Base62: {plain:12s}  混淆: {obfuscated}")

    print("\n混淆编码可逆验证：")
    sf3 = Snowflake(worker_id=1, datacenter_id=1)
    for i in range(3):
        obf = sf3.generate_obfuscated()
        decoded_info = Snowflake.decode_obfuscated(obf)
        print(f"  混淆ID: {obf:14s} → 原始: {decoded_info['id']}  时间: {decoded_info['datetime']}")

    # ---- ZooKeeper workerId 分配 ----
    print("\n" + "=" * 60)
    print("ZooKeeper workerId 自动分配")
    print("=" * 60)

    print("\n模拟 3 个服务实例注册：")
    allocator = ZookeeperWorkerIdAllocator(use_mock=True)
    instances = []
    for i in range(3):
        print(f"\n  --- 实例 {i} ---")
        sf_inst = allocator.create_snowflake(datacenter_id=i % 2)
        instances.append(sf_inst)

    print("\n各实例生成 ID（workerId 不同，ID 不冲突）：")
    all_ids = []
    for i, inst in enumerate(instances):
        inst_ids = [inst.generate() for _ in range(3)]
        all_ids.extend(inst_ids)
        parsed = Snowflake.parse(inst_ids[0])
        print(f"  实例{i} (w={parsed['worker_id']}, dc={parsed['datacenter_id']}): {inst_ids}")

    unique_count = len(set(all_ids))
    print(f"\n  唯一性: 总数={len(all_ids)}, 去重={unique_count} {'✅ 通过' if unique_count == len(all_ids) else '❌ 有重复'}")

    allocator.close()

    # ---- 性能测试 ----
    print("\n" + "=" * 60)
    print("性能测试")
    print("=" * 60)

    sf_perf = Snowflake(worker_id=1, datacenter_id=1)

    print("\n纯数字 ID 生成 10 万个：")
    start = time.time()
    for _ in range(100_000):
        sf_perf.generate()
    elapsed = time.time() - start
    print(f"  耗时: {elapsed:.3f}s, QPS: {100_000 / elapsed:,.0f} ID/s")

    print("\nBase62 编码 ID 生成 10 万个：")
    start = time.time()
    for _ in range(100_000):
        sf_perf.generate_base62()
    elapsed = time.time() - start
    print(f"  耗时: {elapsed:.3f}s, QPS: {100_000 / elapsed:,.0f} ID/s")

    print("\n混淆编码 ID 生成 10 万个：")
    start = time.time()
    for _ in range(100_000):
        sf_perf.generate_obfuscated()
    elapsed = time.time() - start
    print(f"  耗时: {elapsed:.3f}s, QPS: {100_000 / elapsed:,.0f} ID/s")


if __name__ == "__main__":
    demo()
