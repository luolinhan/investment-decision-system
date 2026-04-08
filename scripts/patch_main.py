"""
安全更新main.py添加V2路由
"""
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
MAIN_PY = BASE_DIR / "app" / "main.py"

def patch_main():
    content = MAIN_PY.read_text(encoding="utf-8")

    # 检查是否已经patched
    if "investment_v2" in content:
        print("main.py已包含V2路由，跳过")
        return

    # 添加import
    import_pattern = r"from app\.routers import ([^\n]+)"
    match = re.search(import_pattern, content)
    if match:
        old_import = match.group(0)
        new_import = old_import.replace(
            match.group(1),
            match.group(1) + ", investment_v2"
        )
        content = content.replace(old_import, new_import)
        print("已添加investment_v2导入")

    # 添加include_router
    router_pattern = r"(app\.include_router\(investment\.router\))"
    match = re.search(router_pattern, content)
    if match:
        old_router = match.group(0)
        new_router = old_router + "\napp.include_router(investment_v2.router_v2)"
        content = content.replace(old_router, new_router)
        print("已添加V2路由注册")

    # 保存
    MAIN_PY.write_text(content, encoding="utf-8")
    print("main.py更新完成")

if __name__ == "__main__":
    patch_main()