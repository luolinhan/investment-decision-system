# 测试新的慧博客户端
import sys
sys.path.insert(0, 'C:/Projects/research_report_system')

from app.services.hibor_client import HuiborClient

print("=== 测试慧博投研客户端 ===\n")

# 创建客户端
client = HuiborClient(username="luolinhan", password="LUOLINHAN666")

# 登录
client.login()

print("\n" + "="*50)
print("获取研报列表...")

# 获取研报列表
reports = client.get_report_list(page=1)
print(f"获取到 {len(reports)} 条研报")

for r in reports[:5]:
    print(f"  - {r['title'][:40]}")

# 关闭
client.close()