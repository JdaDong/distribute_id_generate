"""
分布式全局 ID 生成策略 - 统一入口

集成五种主流 ID 生成策略，可分别运行 demo 查看效果。
"""

import sys


def print_menu():
    print()
    print("=" * 60)
    print("  分布式全局 ID 生成策略 演示程序")
    print("=" * 60)
    print()
    print("  [1] UUID 生成器          - 基于随机数/时间戳/哈希")
    print("  [2] Snowflake 雪花算法   - 64位趋势递增（推荐）")
    print("  [3] 号段模式 Segment     - 批量获取，高吞吐")
    print("  [4] Redis 自增 ID        - 原子递增，严格有序")
    print("  [5] 数据库自增 ID        - AUTO_INCREMENT + 步长模式")
    print("  [0] 全部运行")
    print("  [q] 退出")
    print()


def run_uuid():
    from uuid_generator import demo
    demo()


def run_snowflake():
    from snowflake import demo
    demo()


def run_segment():
    from segment import demo
    demo()


def run_redis():
    from redis_id import demo
    demo()


def run_database():
    from database_id import demo
    demo()


DEMOS = {
    "1": ("UUID", run_uuid),
    "2": ("Snowflake", run_snowflake),
    "3": ("Segment", run_segment),
    "4": ("Redis", run_redis),
    "5": ("Database", run_database),
}


def run_all():
    for key in sorted(DEMOS.keys()):
        name, func = DEMOS[key]
        print(f"\n{'#' * 60}")
        print(f"# {name}")
        print(f"{'#' * 60}")
        func()
        print()


def main():
    if len(sys.argv) > 1:
        choice = sys.argv[1]
        if choice == "0":
            run_all()
        elif choice in DEMOS:
            DEMOS[choice][1]()
        else:
            print(f"未知选项: {choice}")
            print_menu()
        return

    while True:
        print_menu()
        choice = input("请选择 [0-5/q]: ").strip().lower()

        if choice == "q":
            print("再见！")
            break
        elif choice == "0":
            run_all()
        elif choice in DEMOS:
            DEMOS[choice][1]()
        else:
            print("无效选项，请重新选择")


if __name__ == "__main__":
    main()
