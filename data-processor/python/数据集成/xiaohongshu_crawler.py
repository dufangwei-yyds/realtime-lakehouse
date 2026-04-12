import random
import time
from DataRecorder import Recorder
from numpy.core.records import record
from selenium import webdriver
from selenium.common import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from confluent_kafka import Producer
import json
import logging
import os
import math
from DrissionPage import ChromiumPage
from numpy.core.records import record

"""
数据集成模块: 爬虫数据-小红书案例
整合目标：
    1. 使用Selenium搜索关键词获取列表页
    2. 提取列表页中的用户主页链接
    3. 使用DrissionPage访问每个用户主页并爬取笔记数据
    4. 支持从 keyword.txt 中读取多个关键词依次执行
    5. 数据实时发送到 Kafka (topic: social_hot_styles)
    kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic social_hot_styles --from-beginning
"""

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# URL设置、页面加载超时时间(秒)、登录等待时间(秒)
XHS_HOME_URL = "https://www.xiaohongshu.com"
XHS_SEARCH_URL = "https://www.xiaohongshu.com/search_result?keyword="
WAIT_TIMEOUT = 20
LOGIN_WAIT_TIME = 60

# Kafka 配置
KAFKA_BROKER = "192.168.63.128:9092"
KAFKA_TOPIC = "social_hot_styles"

# 初始化 Kafka 生产者
producer = Producer({"bootstrap.servers": KAFKA_BROKER})

# 初始化浏览器 (Selenium)
def init_driver():
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")

    driver = webdriver.Chrome(options=chrome_options)
    return driver

# 登录前等待
def pre_login_wait(driver):
    logger.info(f"正在访问首页: {XHS_HOME_URL}")
    driver.get(XHS_HOME_URL)
    logger.info(f"请在{LOGIN_WAIT_TIME}秒内完成登录（扫码或输入账号密码）...")

    for i in range(LOGIN_WAIT_TIME, 0, -1):
        logger.info(f"等待登录倒计时: {i}秒")
        time.sleep(1)

    logger.info("登录等待时间结束，开始执行搜索...")

# 构建搜索URL
def build_search_url(keyword):
    from urllib.parse import quote
    encoded_keyword = quote(keyword)
    return f"{XHS_SEARCH_URL}{encoded_keyword}{'&source=unknown'}"

# 打开搜索页面并验证是否加载成功
def test_search_page(driver, url):
    logger.info(f"正在访问测试页面: {url}")
    driver.get(url)

    try:
        WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='note-item']"))
        )
        logger.info("✅ 页面加载成功，检测到笔记列表项！")
        return True
    except TimeoutException:
        logger.error("❌ 页面加载超时，未找到笔记列表项。")
        screenshot_path = os.path.join(os.getcwd(), "test_search_timeout.png")
        driver.save_screenshot(screenshot_path)
        logger.info(f"已保存当前页面截图至: {screenshot_path}")
        return False

# 提取用户主页链接
def extract_user_profile_links_from_page(driver):
    logger.info("🔍 开始提取用户主页链接...")

    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    profile_links = set()
    base_url = "https://www.xiaohongshu.com"

    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        if href.startswith("/user/profile/"):
            full_url = urljoin(base_url, href)
            profile_links.add(full_url)

    logger.info(f"✅ 共提取到 {len(profile_links)} 个用户主页链接")
    for link in profile_links:
        print(link)

    return profile_links

# 读取关键词文件
def read_keywords_from_file(filename="keyword.txt"):
    if not os.path.exists(filename):
        logger.error(f"关键词文件 {filename} 不存在！")
        return []

    with open(filename, "r", encoding="utf-8") as f:
        keywords = [line.strip() for line in f if line.strip()]
    logger.info(f"✅ 从文件读取到 {len(keywords)} 个关键词")
    return keywords

# 抓取指定用户主页的笔记信息
def get_info(page, keyword):
    data_list = []
    try:
        container = page.ele('.feeds-container')
        sections = container.eles('.note-item')

        for section in sections:
            try:
                note_type = "视频" if section.ele('.play-icon', timeout=0) else "图文"
                footer = section.ele(".footer")
                title = footer.ele('.title', timeout=0).text
                author = footer.ele('css: .author-wrapper a span').text
                like = footer.ele('.count').text

                cover_link = section.ele('css:a.cover.mask.ld')
                image_element = cover_link.ele('tag:img')
                image_src = image_element.attr('src') if image_element else ""

                data = {
                    "keyword": keyword,
                    "note_type": note_type,
                    "author": author,
                    "title": title,
                    "likes": like,
                    "link": image_src,
                    "crawl_time": time.strftime("%Y-%m-%d %H:%M:%S")
                }

                data_list.append(data)

            except Exception as e:
                print(f"提取信息时出错: {e}")
                continue
    except Exception as e:
        print(f"页面元素定位失败: {e}")

    return data_list

# 发送数据到 Kafka
def send_to_kafka(data):
    try:
        note_json = json.dumps(data, ensure_ascii=False)
        producer.produce(
            KAFKA_TOPIC,
            key=data["title"][:50],  # Kafka key限制长度
            value=note_json.encode("utf-8"),
            on_delivery=lambda err, msg: logger.info(f"发送结果: {err or '成功'}")
        )
        producer.poll(0)
    except Exception as e:
        logger.error(f"发送到Kafka失败: {e}")

# 执行滚动爬取
def crawler(page, scroll_times, keyword):
    global total_num
    total_num = 0
    for _ in range(scroll_times):
        data_list = get_info(page, keyword)
        if data_list:
            total_num += len(data_list)
            for data in data_list:
                send_to_kafka(data)
            print(f'已发送 {len(data_list)} 条数据到 Kafka')

        page.scroll.to_bottom()
        time.sleep(random.uniform(5, 10))

if __name__ == "__main__":
    keywords = read_keywords_from_file("D:/workspace/data-processor/python/数据集成/resource/keyword.txt")
    

    if not keywords:
        logger.error("没有可处理的关键词，请检查 keyword.txt 文件")
        exit()

    for keyword in keywords:
        logger.info(f"🔍 开始处理关键词：{keyword}")

        # 初始化数据记录器(每个关键词一个文件)
        current_time = time.localtime()
        formatted_time = time.strftime("%Y-%m-%d %H%M%S", current_time)
        init_file_path = f'爬取数据-{keyword}-{formatted_time}.xlsx'
        recorder = Recorder(path=init_file_path, cache_size=1)

        # 初始化 Selenium 浏览器
        driver = init_driver()
        try:
            pre_login_wait(driver)
            search_url = build_search_url(keyword)
            if test_search_page(driver, search_url):
                user_profile_links = extract_user_profile_links_from_page(driver)
            else:
                user_profile_links = []
        finally:
            driver.quit()

        # 初始化 DrissionPage
        if user_profile_links:
            page = ChromiumPage()
            for user_url in user_profile_links:
                logger.info(f"正在爬取用户主页: {user_url}")
                page.get(user_url)
                try:
                    page.wait.ele_displayed('.feeds-container', timeout=10)
                    note_num = 50
                    times = math.ceil(note_num / 20 * 1.1)
                    crawler(page, times, keyword)
                    logger.info(f"已完成该用户数据爬取：{user_url}")
                except Exception as e:
                    logger.error(f"爬取用户 {user_url} 出错: {e}")
            page.quit()
        else:
            logger.warning(f"未提取到该关键词 '{keyword}' 的用户主页链接")

        # 保存数据到 Excel(可选)
        recorder.record()
        logger.info(f"✅ 关键词 '{keyword}' 数据抓取完成，已保存至文件")
