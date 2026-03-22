# 使用Selenium访问慧博投研
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

print("=== 慧博投研 Selenium 测试 ===\n")

# 配置Chrome选项
options = Options()
options.add_argument("--headless")  # 无头模式
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1920,1080")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

try:
    print("启动Chrome...")
    driver = webdriver.Chrome(options=options)

    # 访问慧博首页
    print("访问慧博投研...")
    driver.get("https://www.hibor.com.cn/")
    time.sleep(3)

    print(f"页面标题: {driver.title}")
    print(f"页面长度: {len(driver.page_source)}")

    # 尝试登录
    print("\n尝试登录...")
    driver.get("https://www.hibor.com.cn/login.html")
    time.sleep(2)

    # 输入用户名密码
    try:
        username_input = driver.find_element(By.NAME, "username")
        password_input = driver.find_element(By.NAME, "password")

        username_input.send_keys("luolinhan")
        password_input.send_keys("LUOLINHAN666")

        # 点击登录
        login_btn = driver.find_element(By.CSS_SELECTOR, "input[type='submit'], button[type='submit'], .login-btn")
        login_btn.click()

        time.sleep(3)
        print(f"登录后页面: {driver.current_url}")

    except Exception as e:
        print(f"登录元素查找失败: {e}")

    # 保存页面截图
    driver.save_screenshot("hibor_screenshot.png")
    print("截图已保存到 hibor_screenshot.png")

    # 保存页面源码
    with open("hibor_selenium.html", "w", encoding="utf-8") as f:
        f.write(driver.page_source)
    print("页面已保存到 hibor_selenium.html")

    driver.quit()
    print("\n测试完成!")

except Exception as e:
    print(f"错误: {e}")
    import traceback
    traceback.print_exc()