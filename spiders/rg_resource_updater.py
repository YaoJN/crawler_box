import requests
from bs4 import BeautifulSoup
import sqlite3
import time

# URL 与 file_type 映射
URLS = {
    "https://rapidgator.net/folder/5536086/IDOLS.html?ajax=grid&page=": "IDOLS",
    "https://rapidgator.net/folder/5535536/VIRTUAL_REALITY.html?ajax=grid&page=": "VR",
    "https://rapidgator.net/folder/7380162/4K.html?ajax=grid&page=": "4K",
    "https://rapidgator.net/folder/5535978/UNCENSORED.html?ajax=grid&page=": "MOSACO",

}

# 请求头
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# 数据库路径
DB_PATH = r"C:\DB\disk_info.db"


# 创建数据库
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rg_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_type TEXT NOT NULL,
            file_name TEXT NOT NULL,
            file_size TEXT,
            file_link TEXT NOT NULL UNIQUE,
            created_at TEXT DEFAULT (datetime('now', 'localtime'))
        )
    ''')
    conn.commit()
    conn.close()


# 保存记录到数据库
def save_to_db(file_type, file_name, file_size, file_link):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO rg_files (file_type, file_name, file_size, file_link) VALUES (?, ?, ?, ?)",
            (file_type, file_name, file_size, file_link)
        )
        conn.commit()
    except sqlite3.IntegrityError:
        # 忽略重复
        pass
    conn.close()


# 爬取单页
def scrape_page(base_url, file_type, page_num):
    url = base_url + str(page_num)
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        print(f"[{file_type}] 第 {page_num} 页加载失败")
        return False

    soup = BeautifulSoup(response.text, "html.parser")

    selected_li = soup.find("li", class_="page selected")
    if not selected_li:
        print(f"[{file_type}] 未找到当前选中页码标签，结束爬取")
        return False
    try:
        current_page_on_site = int(selected_li.get_text(strip=True))
    except ValueError:
        print(f"[{file_type}] 页码解析失败")
        return False
    if current_page_on_site != page_num:
        print(f"[{file_type}] 页码不一致，跳出循环")
        return False

    rows = soup.select("table.items tbody tr")
    if not rows:
        print(f"[{file_type}] 本页无资源")
        return False

    for row in rows:
        a_tag = row.find("td").find("a")
        if not a_tag:
            continue
        file_link = "https://rapidgator.net" + a_tag['href']
        file_name = a_tag.get_text(strip=True)
        size_td = row.find_all("td")
        file_size = size_td[1].get_text(strip=True) if len(size_td) > 1 else ""
        save_to_db(file_type, file_name, file_size, file_link)

    return True


# 执行主程序
def run_scraper():
    print("初始化数据库...")
    init_db()

    for base_url, file_type in URLS.items():
        print(f"\n开始处理类型：{file_type}")
        page = 1
        while True:
            print(f"正在处理第 {page} 页（{file_type}）...")
            has_data = scrape_page(base_url, file_type, page)
            if not has_data:
                print(f"[{file_type}] 爬取结束")
                break
            page += 1
            time.sleep(2)


if __name__ == "__main__":
    run_scraper()
