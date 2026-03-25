import sqlite3
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
DB_FILE = Path(os.environ["DB_FILE"])
if not DB_FILE.is_absolute():
    DB_FILE = BASE_DIR / DB_FILE

def clear_database(db_file: str | None = None):
    """
    清空数据库中的所有表数据，但保留表结构。
    """
    target_db_file = db_file or DB_FILE

    if not os.path.exists(target_db_file):
        print(f"数据库文件 {target_db_file} 不存在，无需清空。")
        return

    print(f"正在连接到数据库 {target_db_file}...")
    conn = sqlite3.connect(target_db_file)
    cursor = conn.cursor()

    try:
        # 获取所有表名
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        if not tables:
            print("数据库中没有任何表。")
            return

        print(f"找到 {len(tables)} 张表，准备清空数据...")
        
        # 遍历所有表并清空数据
        for table_name in tables:
            table_name = table_name[0]
            # 跳过 sqlite 内部表
            if table_name.startswith("sqlite_"):
                continue
                
            print(f"正在清空表: {table_name} ...", end=" ")
            cursor.execute(f"DELETE FROM {table_name};")
            print("完成")

        # 提交更改
        conn.commit()
        print("\n✅ 所有表数据已成功清空！(表结构已保留)")
        
        # 可选：释放数据库空间 (VACUUM)
        print("正在整理数据库文件并释放空间...")
        cursor.execute("VACUUM;")
        print("✅ 数据库空间整理完成！")

    except Exception as e:
        print(f"\n❌ 清空数据库时发生错误: {e}")
        conn.rollback()
    finally:
        conn.close()
        print("数据库连接已关闭。")

if __name__ == "__main__":
    confirm = input("⚠️ 警告：此操作将清空数据库中的所有数据且无法恢复！\n确定要继续吗？(输入 'yes' 确认): ")
    if confirm.strip().lower() == 'yes':
        clear_database()
    else:
        print("操作已取消。")
