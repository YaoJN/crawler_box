import requests
from bs4 import BeautifulSoup
import sqlite3
import time

# Rapidgator 文件夹的前缀 URL
BASE_URL = "https://rapidgator.net/folder/7380162/4K.html?ajax=grid&page="

# 设置请求头，模拟浏览器访问
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# 数据库路径，注意Windows路径反斜杠转义或用原始字符串
DB_PATH = r"C:\DB\disk_info.db"


# 创建数据库和表，新增file_name、file_size、file_link字段
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rg_4k_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT NOT NULL,
            file_size TEXT,
            file_link TEXT NOT NULL UNIQUE,
            created_at TEXT DEFAULT (datetime('now', 'localtime'))
        )
    ''')
    conn.commit()
    conn.close()


# 保存数据到数据库，跳过重复的 file_link
def save_to_db(file_name, file_size, file_link):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO rg_4k_files (file_name, file_size, file_link) VALUES (?, ?, ?)",
            (file_name, file_size, file_link)
        )
        conn.commit()
    except sqlite3.IntegrityError:
        # 如果链接已存在则忽略
        pass
    conn.close()


# 爬取单页数据
def scrape_page(page_num):
    url = BASE_URL + str(page_num)
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        print(f"第 {page_num} 页加载失败")
        return False

    soup = BeautifulSoup(response.text, "html.parser")

    # 找当前选中页码，避免无限翻页（根据你的之前逻辑）
    selected_li = soup.find("li", class_="page selected")
    if not selected_li:
        print("未找到当前选中页码标签，结束爬取")
        return False
    try:
        current_page_on_site = int(selected_li.get_text(strip=True))
    except ValueError:
        print("页码转换失败，结束爬取")
        return False
    if current_page_on_site != page_num:
        print(f"页面实际页码 {current_page_on_site} 与请求页码 {page_num} 不符，结束爬取")
        return False

    # 选择table下所有tr，不包含thead的行
    rows = soup.select("table.items tbody tr")
    if not rows:
        print("本页无资源，结束爬取")
        return False

    for row in rows:
        # 获取文件名和链接，文件名在<td><a>标签中，去掉前面img部分
        a_tag = row.find("td").find("a")
        if not a_tag:
            continue
        file_link_suffix = a_tag['href']
        # 拼接完整链接
        file_link = "https://rapidgator.net" + file_link_suffix
        # 文件名是a标签的文本，去掉前后空白
        file_name = a_tag.get_text(strip=True)

        # 获取文件大小，是同一行的第二个<td>的文本
        size_td = row.find_all("td")
        file_size = size_td[1].get_text(strip=True) if len(size_td) > 1 else ""

        save_to_db(file_name, file_size, file_link)

    return True


# 执行爬取任务
def run_scraper():
    print("开始爬取 Rapidgator 页面内容...")
    init_db()
    page = 1
    while True:
        print(f"正在处理第 {page} 页...")
        has_data = scrape_page(page)
        if not has_data:
            print("没有更多页面，爬取结束。")
            break
        page += 1
        time.sleep(2)  # 等待2秒，防止访问过快被封


if __name__ == "__main__":
    run_scraper()
