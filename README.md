# 分布式全局 ID 生成策略

五种主流分布式 ID 生成方案的 Python 实现，每种均包含完整代码、性能测试和使用示例。

## 项目结构

```
distribute_id_generate/
├── main.py              # 统一入口（交互式菜单）
├── uuid_generator.py    # UUID 生成器
├── snowflake.py         # Snowflake 雪花算法
├── segment.py           # 号段模式（Segment）
├── redis_id.py          # Redis 自增 ID
├── database_id.py       # 数据库自增 ID
├── requirements.txt     # 依赖（可选）
└── README.md
```

## 快速开始

```bash
# 运行交互式菜单
python main.py

# 直接运行某个策略
python main.py 1    # UUID
python main.py 2    # Snowflake
python main.py 3    # Segment
python main.py 4    # Redis
python main.py 5    # Database
python main.py 0    # 全部运行

# 单独运行
python uuid_generator.py
python snowflake.py
python segment.py
python redis_id.py
python database_id.py
```

## 策略说明

### 1. UUID (`uuid_generator.py`)
- **原理**: 基于时间戳、MAC 地址、随机数生成 128 位唯一标识
- **优点**: 本地生成、无依赖、性能极高
- **缺点**: 无序、字符串长、不适合做数据库索引
- **适用**: traceId、请求标识、临时文件名

### 2. Snowflake 雪花算法 (`snowflake.py`) ⭐推荐
- **原理**: 64 位 = 1bit 符号 + 41bit 时间戳 + 10bit 机器 + 12bit 序列号
- **优点**: 趋势递增、高性能、可反解析
- **缺点**: 依赖时钟（含回拨处理）
- **适用**: 大多数分布式场景的首选方案
- **增强功能**:
  - **Base62 编码**: 将长数字 ID 编码为短字符串（如 `831784799021699072` → `ZrA9PS0NoI`）
  - **混淆编码**: XOR + Base62，连续 ID 编码后完全无规律，适合对外暴露（如订单号）
  - **ZooKeeper workerId 分配**: 模拟美团 Leaf 方案，通过 ZK 顺序节点自动分配 workerId，无需人工配置

### 3. 号段模式 (`segment.py`)
- **原理**: 批量从存储层获取一段 ID，内存中分配
- **优点**: 减少 DB 访问、高吞吐、支持双 Buffer
- **缺点**: 重启浪费号段
- **适用**: 对数据库友好、需严格递增的场景

### 4. Redis 自增 (`redis_id.py`)
- **原理**: 利用 Redis INCR 命令原子递增
- **优点**: 严格递增、高性能、支持日期/业务前缀
- **缺点**: 依赖 Redis
- **适用**: 已有 Redis 基础设施的系统
- **注意**: 无 Redis 时自动降级为内存模拟模式

### 5. 数据库自增 (`database_id.py`)
- **原理**: AUTO_INCREMENT + 多实例步长模式
- **优点**: 实现简单、严格递增
- **缺点**: 单点瓶颈
- **适用**: 小规模系统、对 QPS 要求不高的场景

## 策略对比

| 策略 | 有序性 | 性能 | 依赖 | 复杂度 | 推荐场景 |
|------|--------|------|------|--------|----------|
| UUID | 无序 | 极高 | 无 | 低 | 临时标识 |
| Snowflake | 趋势递增 | 极高 | 无 | 中 | **通用首选** |
| 号段模式 | 趋势递增 | 高 | DB | 中 | 高吞吐业务 |
| Redis | 严格递增 | 高 | Redis | 中 | 有Redis基建 |
| 数据库 | 严格递增 | 低 | DB | 低 | 小规模系统 |

## 依赖

核心功能无外部依赖。可选依赖：

```bash
pip install -r requirements.txt
```

| 依赖 | 用途 | 是否必须 |
|------|------|----------|
| redis | Redis ID 生成连接真实 Redis | 可选，无则自动降级模拟模式 |
| kazoo | ZooKeeper workerId 自动分配 | 可选，无则自动降级模拟模式 |
# distribute_id_generate
