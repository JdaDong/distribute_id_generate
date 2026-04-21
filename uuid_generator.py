"""
UUID 全局唯一 ID 生成器

基于 Python 标准库 uuid 模块，支持多种 UUID 版本：
- UUID1: 基于时间戳和 MAC 地址
- UUID3: 基于命名空间和名称的 MD5 哈希
- UUID4: 基于随机数（最常用）
- UUID5: 基于命名空间和名称的 SHA-1 哈希
"""

import uuid
from typing import Optional


class UUIDGenerator:
    """UUID 生成器"""

    @staticmethod
    def uuid1() -> str:
        """基于时间戳和 MAC 地址生成 UUID（有序，但暴露 MAC 地址）"""
        return str(uuid.uuid1())

    @staticmethod
    def uuid3(namespace: uuid.UUID, name: str) -> str:
        """基于命名空间和名称的 MD5 哈希生成 UUID（确定性）"""
        return str(uuid.uuid3(namespace, name))

    @staticmethod
    def uuid4() -> str:
        """基于随机数生成 UUID（最常用，完全随机）"""
        return str(uuid.uuid4())

    @staticmethod
    def uuid5(namespace: uuid.UUID, name: str) -> str:
        """基于命名空间和名称的 SHA-1 哈希生成 UUID（确定性）"""
        return str(uuid.uuid5(namespace, name))

    @staticmethod
    def uuid4_hex() -> str:
        """生成不带连字符的 UUID4（32 字符）"""
        return uuid.uuid4().hex

    @staticmethod
    def uuid4_int() -> int:
        """生成 UUID4 对应的整数值（128 位整数）"""
        return uuid.uuid4().int

    @staticmethod
    def short_uuid(length: int = 8) -> str:
        """生成短 UUID（截取前 N 位，注意唯一性降低）"""
        return uuid.uuid4().hex[:length]


def demo():
    gen = UUIDGenerator()

    print("=" * 60)
    print("UUID 全局唯一 ID 生成器")
    print("=" * 60)

    print(f"\nUUID1 (时间+MAC):  {gen.uuid1()}")
    print(f"UUID3 (MD5哈希):   {gen.uuid3(uuid.NAMESPACE_DNS, 'example.com')}")
    print(f"UUID4 (随机):      {gen.uuid4()}")
    print(f"UUID5 (SHA1哈希):  {gen.uuid5(uuid.NAMESPACE_DNS, 'example.com')}")
    print(f"UUID4 Hex:         {gen.uuid4_hex()}")
    print(f"UUID4 Int:         {gen.uuid4_int()}")
    print(f"Short UUID (8位):  {gen.short_uuid(8)}")

    print("\n批量生成 5 个 UUID4:")
    for i in range(5):
        print(f"  [{i+1}] {gen.uuid4()}")


if __name__ == "__main__":
    demo()
