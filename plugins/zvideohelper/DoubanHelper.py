import re
from typing import Tuple, Optional
from urllib.parse import unquote, quote
import requests
from bs4 import BeautifulSoup
from http.cookies import SimpleCookie
from app.helper.cookiecloud import CookieCloudHelper
from app.log import logger


class DoubanHelper:

    def __init__(self, user_cookie: str = None):
        # 初始化Cookie（修复NoneType错误）
        self.cookies: dict = {}
        if not user_cookie:
            self.cookiecloud = CookieCloudHelper()
            cookie_dict, msg = self.cookiecloud.download()
            if cookie_dict is None:
                logger.error(f"获取CookieCloud数据失败：{msg}")
            else:
                self.cookies = cookie_dict.get("douban.com", {})
        else:
            # 解析用户传入的Cookie字符串
            try:
                self.cookies = {k: v.value for k, v in SimpleCookie(user_cookie).items()}
            except Exception as e:
                logger.error(f"解析用户传入Cookie失败：{e}")
                self.cookies = {}

        # 移除无用Cookie字段
        self.cookies.pop("__utmz", None)  # 用pop+默认值，避免KeyError
        self.cookies.pop("ck", None)

        # 初始化请求头（统一基础配置）
        self.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.57'
        self.headers = {
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, sdch',
            'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4,en-GB;q=0.2,zh-TW;q=0.2',
            'Connection': 'keep-alive',
            'DNT': '1'
        }

        # 获取最新CK（关键：添加异常捕获）
        self.ck: Optional[str] = None
        try:
            self.set_ck()
            self.ck = self.cookies.get('ck')
        except Exception as e:
            logger.error(f"获取豆瓣CK失败：{e}")

        # 日志输出（优化格式，方便排查）
        logger.debug(f"豆瓣CK：{self.ck}，Cookie：{self.cookies}")
        if not self.cookies:
            logger.error("豆瓣Cookie为空，请检查插件配置或CookieCloud")
        if not self.ck:
            logger.error("豆瓣CK获取失败，无法设置观影状态，请检查Cookie登录状态")

    def _build_cookie_str(self) -> str:
        """辅助方法：构建Cookie字符串（减少重复代码）"""
        return ";".join([f"{key}={value}" for key, value in self.cookies.items()])

    def set_ck(self):
        """获取最新CK（优化容错和异常处理）"""
        self.headers["Cookie"] = self._build_cookie_str()
        try:
            # 添加超时（避免卡住），设置30秒
            response = requests.get(
                "https://www.douban.com/",
                headers=self.headers,
                timeout=30,
                allow_redirects=True
            )
            response.raise_for_status()  # 抛出HTTP错误（如403、401）
        except requests.exceptions.RequestException as e:
            logger.error(f"请求豆瓣首页获取CK失败：{e}")
            self.cookies['ck'] = ''
            return

        ck_str = response.headers.get('Set-Cookie', '')
        logger.debug(f"豆瓣Set-Cookie响应：{ck_str}")
        if not ck_str:
            logger.error("豆瓣未返回CK，可能Cookie已失效或登录状态异常")
            self.cookies['ck'] = ''
            return

        # 容错解析CK（避免格式变化导致索引错误）
        cookie_parts = ck_str.split(";")[0].strip()
        if "=" not in cookie_parts:
            logger.error(f"CK格式异常：{ck_str}")
            self.cookies['ck'] = ''
            return

        ck = cookie_parts.split("=")[1].strip()
        logger.debug(f"解析到CK：{ck}")
        self.cookies['ck'] = ck

    def get_subject_id(self, title: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        搜索豆瓣获取影片信息（优化：编码、超时、异常捕获、日志、匹配准确性）
        :return: (影片标题, subject_id, 评分)
        """
        # 1. 显式编码标题（处理特殊字符、中文）
        encoded_title = quote(title, safe='')
        url = f"https://www.douban.com/search?cat=1002&q={encoded_title}"
        logger.debug(f"豆瓣搜索URL：{url}（原始标题：{title}）")

        try:
            # 2. 添加超时（15秒），避免批量处理卡住
            response = requests.get(
                url,
                headers=self.headers,
                cookies=self.cookies,
                timeout=15,
                allow_redirects=True
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"搜索豆瓣影片失败（标题：{title}）：{e}")
            return None, None, None

        # 3. 状态码判断（补充非200日志）
        if response.status_code != 200:
            logger.error(f"搜索豆瓣影片失败（标题：{title}），状态码：{response.status_code}，响应：{response.text[:500]}")
            return None, None, None

        # 4. 解析搜索结果
        soup = BeautifulSoup(response.text, 'html.parser')
        title_divs = soup.find_all("div", class_="title")
        subject_items = []

        for div in title_divs:
            a_tag = div.find("a")
            if not a_tag:
                continue

            item = {}
            # 标题
            item["title"] = a_tag.get_text(strip=True)
            # 年份（从subject-cast提取）
            cast_span = div.find(class_="subject-cast")
            if cast_span:
                cast_text = cast_span.get_text(strip=True)
                year_match = re.search(r'(\d{4})$', cast_text)
                if year_match:
                    item["year"] = year_match.group(1)
                else:
                    item["year"] = ""
            else:
                item["year"] = ""
            # 评分
            rating_span = div.find(class_="rating_nums")
            item["rating_nums"] = rating_span.get_text(strip=True) if rating_span else "0"
            # Subject ID
            link = unquote(a_tag.get("href", ""))
            subject_match = re.search(r"subject/(\d+)/", link)
            if subject_match:
                item["subject_id"] = subject_match.group(1)
            else:
                continue  # 无subject_id的跳过

            subject_items.append(item)

        if not subject_items:
            logger.warning(f"豆瓣未找到匹配影片（标题：{title}）")
            return None, None, None

        # 5. 优化匹配逻辑：优先匹配包含年份的标题（如果原始标题有年份）
        target_year = re.search(r'(\d{4})', title)
        if target_year:
            target_year_str = target_year.group(1)
            for item in subject_items:
                if item["year"] == target_year_str:
                    logger.debug(f"优先匹配年份：{title} → {item['title']}（{item['year']}），subject_id：{item['subject_id']}")
                    return item["title"], item["subject_id"], item["rating_nums"]

        # 6. 无年份匹配时，返回第一个结果（保留原有逻辑）
        first_item = subject_items[0]
        logger.debug(f"匹配豆瓣影片（取第一个结果）：{title} → {first_item['title']}，subject_id：{first_item['subject_id']}，评分：{first_item['rating_nums']}")
        return first_item["title"], first_item["subject_id"], first_item["rating_nums"]

    def set_watching_status(self, subject_id: str, status: str = "do", private: bool = True) -> bool:
        """
        设置豆瓣观影状态（优化：超时、异常捕获、JSON解析容错）
        :param subject_id: 豆瓣影片ID
        :param status: do=在看，collect=已看
        :param private: 是否私有
        :return: 是否成功
        """
        if not self.ck:
            logger.error(f"设置观影状态失败（subject_id：{subject_id}）：CK为空")
            return False

        # 构建请求头（动态设置Referer和Host）
        headers = self.headers.copy()
        headers.update({
            "Referer": f"https://movie.douban.com/subject/{subject_id}/",
            "Origin": "https://movie.douban.com",
            "Host": "movie.douban.com",
            "Cookie": self._build_cookie_str(),
            "Content-Type": "application/x-www-form-urlencoded"  # 补充必要的Content-Type
        })

        # 构建请求数据
        data = {
            "ck": self.ck,
            "interest": status,
            "rating": "",
            "foldcollect": "U",
            "tags": "",
            "comment": ""
        }
        if private:
            data["private"] = "on"

        logger.debug(f"设置豆瓣观影状态：subject_id={subject_id}，status={status}，data={data}")

        try:
            # 添加超时（15秒）
            response = requests.post(
                url=f"https://movie.douban.com/j/subject/{subject_id}/interest",
                headers=headers,
                data=data,
                timeout=15,
                allow_redirects=True
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"设置观影状态失败（subject_id：{subject_id}）：{e}，响应：{getattr(response, 'text', '')[:500]}")
            return False

        # 解析响应（容错JSON解析失败）
        try:
            resp_json = response.json()
        except json.JSONDecodeError as e:
            logger.error(f"解析豆瓣响应失败（subject_id：{subject_id}）：{e}，响应：{response.text[:500]}")
            return False

        # 处理响应结果（严格匹配豆瓣返回格式）
        ret = resp_json.get("r")
        if ret == 0:
            logger.info(f"设置豆瓣观影状态成功：subject_id={subject_id}，status={status}")
            return True
        elif ret is False:
            logger.error(f"设置观影状态失败（subject_id：{subject_id}）：影片未开播或状态异常，响应：{resp_json}")
            return False
        else:
            logger.error(f"设置观影状态失败（subject_id：{subject_id}）：未知响应，{resp_json}")
            return False


if __name__ == "__main__":
    # 测试代码（保持原有功能，替换print为logger）
    doubanHelper = DoubanHelper()
    subject_title, subject_id, score = doubanHelper.get_subject_id("火线 第 3 季")
    logger.info(f"测试结果：subject_title={subject_title}，subject_id={subject_id}，score={score}")
    if subject_id:
        doubanHelper.set_watching_status(subject_id=subject_id, status="do", private=True)
