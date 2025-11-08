from datetime import datetime, timedelta
import sqlite3
import json
from enum import Enum
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from app.schemas.types import EventType, NotificationType
from app.core.event import eventmanager, Event
from pathlib import Path
from app.core.config import settings
from app.plugins import _PluginBase
from typing import Any, List, Dict, Tuple, Optional
from app.log import logger
import time
from urllib.parse import unquote, quote
import requests
from bs4 import BeautifulSoup
from http.cookies import SimpleCookie
from app.helper.cookiecloud import CookieCloudHelper


class DoubanHelper:
    """豆瓣工具类：仅保留评分查询核心功能"""
    def __init__(self, user_cookie: str = None):
        self.cookies: dict = {}
        # 初始化Cookie
        if not user_cookie:
            self.cookiecloud = CookieCloudHelper()
            cookie_dict, msg = self.cookiecloud.download()
            if cookie_dict is None:
                logger.error(f"获取CookieCloud数据失败：{msg}")
            else:
                self.cookies = cookie_dict.get("douban.com", {})
        else:
            try:
                self.cookies = {k: v.value for k, v in SimpleCookie(user_cookie).items()}
            except Exception as e:
                logger.error(f"解析用户传入Cookie失败：{e}")
                self.cookies = {}

        # 初始化请求头
        self.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.57'
        self.headers = {
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, sdch',
            'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4,en-GB;q=0.2,zh-TW;q=0.2',
            'Connection': 'keep-alive',
            'DNT': '1'
        }

        # 日志输出Cookie状态
        if not self.cookies:
            logger.error("豆瓣Cookie为空，请检查插件配置或CookieCloud")

    def get_subject_id(self, title: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """搜索豆瓣获取影片评分（优化编码、超时、匹配逻辑）"""
        # 标题编码处理
        encoded_title = quote(title, safe='')
        url = f"https://www.douban.com/search?cat=1002&q={encoded_title}"
        logger.debug(f"豆瓣搜索URL：{url}（原始标题：{title}）")

        try:
            # 15秒超时防止卡住
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

        if response.status_code != 200:
            logger.error(f"豆瓣搜索状态码异常（标题：{title}）：{response.status_code}")
            return None, None, None

        # 解析搜索结果
        soup = BeautifulSoup(response.text, 'html.parser')
        title_divs = soup.find_all("div", class_="title")
        subject_items = []

        for div in title_divs:
            a_tag = div.find("a")
            if not a_tag:
                continue

            item = {}
            item["title"] = a_tag.get_text(strip=True)
            # 提取年份
            cast_span = div.find(class_="subject-cast")
            item["year"] = re.search(r'(\d{4})$', cast_span.get_text(strip=True)).group(1) if cast_span else ""
            # 提取评分
            rating_span = div.find(class_="rating_nums")
            item["rating_nums"] = rating_span.get_text(strip=True) if rating_span else "0"
            # 提取豆瓣ID
            link = unquote(a_tag.get("href", ""))
            subject_match = re.search(r"subject/(\d+)/", link)
            if subject_match:
                item["subject_id"] = subject_match.group(1)
                subject_items.append(item)

        if not subject_items:
            logger.warning(f"豆瓣未找到匹配影片（标题：{title}）")
            return None, None, None

        # 优先匹配年份一致的结果
        target_year = re.search(r'(\d{4})', title)
        if target_year:
            target_year_str = target_year.group(1)
            for item in subject_items:
                if item["year"] == target_year_str:
                    return item["title"], item["subject_id"], item["rating_nums"]

        # 无年份匹配则返回第一个结果
        first_item = subject_items[0]
        return first_item["title"], first_item["subject_id"], first_item["rating_nums"]


class DoubanRatingSync(_PluginBase):
    # 插件基础信息（唯一标识，避免冲突）
    plugin_name = "豆瓣评分修正"
    plugin_desc = "同步豆瓣评分至极影视，无豆瓣评分则保留原有评分"
    plugin_icon = "https://img9.doubanio.com/favicon.ico"  # 豆瓣图标URL
    plugin_version = "1.0"
    plugin_author = "funcygo"
    author_url = "https://github.com/funcygo"
    plugin_config_prefix = "doubanratingsync"  # 配置前缀唯一
    plugin_order = 10
    auth_level = 1

    # 私有属性（仅保留核心配置）
    _enabled = False
    _cron = "0 1 * * *"  # 默认每天凌晨1点执行
    _notify = False
    _onlyonce = False
    _db_path = ""
    _cookie = ""
    _douban_score_update_days = 7  # 默认7天更新一次评分
    _cached_data: dict = {}  # 缓存已处理影片（标题→评分→更新时间）
    _scheduler: Optional[BackgroundScheduler] = None
    _should_stop = False
    _douban_helper: Optional[DoubanHelper] = None

    def init_plugin(self, config: dict = None):
        self._should_stop = False
        self.stop_service()  # 停止现有任务，避免重复

        # 加载配置
        if config:
            self._enabled = config.get("enabled", False)
            self._cron = config.get("cron", self._cron)
            self._notify = config.get("notify", False)
            self._onlyonce = config.get("onlyonce", False)
            self._db_path = config.get("db_path", "")
            self._cookie = config.get("cookie", "")
            self._douban_score_update_days = int(config.get("douban_score_update_days", 7))
            # 初始化豆瓣工具类
            self._douban_helper = DoubanHelper(user_cookie=self._cookie)

        # 加载缓存（已处理影片，避免重复请求）
        self._cached_data = self.get_data("doubanratingsync") or {}

        # 校验数据库路径
        if self._onlyonce or (self._enabled and self._cron):
            path = Path(self._db_path)
            if not path.exists():
                logger.error(f"极影视数据库路径不存在：{self._db_path}")
                if self._notify:
                    self.post_message(
                        mtype=NotificationType.SiteMessage,
                        title="【豆瓣评分修正】初始化失败",
                        text=f"极影视数据库路径不存在：{self._db_path}\n请检查路径配置并映射数据库文件"
                    )
                return

        # 立即执行一次
        if self._onlyonce:
            logger.info("豆瓣评分修正：立即执行一次任务")
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.sync_douban_rating,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                name="豆瓣评分修正-立即执行"
            )
            # 关闭立即执行开关
            self._onlyonce = False
            self._update_config()
            # 启动任务
            if self._scheduler.get_jobs():
                self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    def _update_config(self):
        """更新配置到数据库"""
        self.update_config({
            "enabled": self._enabled,
            "cron": self._cron,
            "notify": self._notify,
            "onlyonce": self._onlyonce,
            "db_path": self._db_path,
            "cookie": self._cookie,
            "douban_score_update_days": self._douban_score_update_days
        })

    def get_command(self) -> List[Dict[str, Any]]:
        """注册手动命令（支持微信/其他渠道触发）"""
        return [
            {
                "cmd": "/sync_douban_rating",
                "event": EventType.PluginAction,
                "desc": "手动同步豆瓣评分至极影视",
                "category": "工具",
                "data": {"action": "sync_douban_rating"}
            }
        ]

    @eventmanager.register(EventType.PluginAction)
    def handle_command(self, event: Event):
        """处理手动命令"""
        event_data = event.event_data or {}
        if event_data.get("action") == "sync_douban_rating":
            logger.info("收到手动命令：同步豆瓣评分")
            self.post_message(
                channel=event.event_data.get("channel"),
                title="【豆瓣评分修正】",
                text="开始同步豆瓣评分至极影视...",
                userid=event.event_data.get("user")
            )
            # 执行同步任务
            self.sync_douban_rating()
            self.post_message(
                channel=event.event_data.get("channel"),
                title="【豆瓣评分修正】",
                text="豆瓣评分同步完成！",
                userid=event.event_data.get("user")
            )

    def get_service(self) -> List[Dict[str, Any]]:
        """注册公共定时服务（在MoviePilot服务列表可见）"""
        if self._enabled and self._cron:
            return [
                {
                    "id": "DoubanRatingSync",
                    "name": "豆瓣评分修正",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.sync_douban_rating,
                    "kwargs": {}
                }
            ]
        return []

    def sync_douban_rating(self):
        """核心任务：批量同步豆瓣评分"""
        self._should_stop = False
        logger.info("开始同步豆瓣评分至极影视...")
        conn = None
        cursor = None
        message = ""

        try:
            # 数据库连接（自动释放）
            conn = sqlite3.connect(self._db_path)
            conn.text_factory = str
            cursor = conn.cursor()

            # 1. SQL去重查询：过滤特定路径+排除合集+仅含douban_score字段
            cursor.execute("""
            SELECT DISTINCT zc.rowid, zc.meta_info, zc.updated_at
            FROM zvideo_collection zc 
            LEFT JOIN zvideo_list zl ON zc.collection_id = zl.collection_id 
            WHERE zc.extend_type != 7  # 排除合集
              AND JSON_EXTRACT(zc.meta_info, '$.douban_score') IS NOT NULL
              AND (zl.path NOT LIKE '/tmp/zfsv3/sata11/13107640652/data/RR%' OR zl.path IS NULL)
            """)
            rows = cursor.fetchall()
            logger.info(f"查询到待处理影片：{len(rows)} 部（已自动去重）")
            if not rows:
                logger.info("无需要更新评分的影片")
                return

            # 2. 筛选需要更新的影片（缓存未命中/评分过期）
            current_time = datetime.now()
            batch_update = []
            for row in rows:
                if self._should_stop:
                    logger.info("任务被中断，停止同步")
                    return

                rowid, meta_info_json, updated_at = row
                try:
                    meta_info = json.loads(meta_info_json)
                    title = meta_info["title"]
                    old_score = float(meta_info.get("douban_score", 0))

                    # 检查缓存：是否已处理且未过期
                    cache_info = self._cached_data.get(title, {})
                    if cache_info:
                        cache_score = cache_info.get("score")
                        cache_time = datetime.strptime(cache_info.get("time"), "%Y-%m-%d %H:%M:%S")
                        # 缓存未过期且评分一致，跳过
                        if (current_time - cache_time).days < self._douban_score_update_days and cache_score == old_score:
                            continue

                    # 需要更新的影片加入批次
                    batch_update.append((rowid, title, old_score, meta_info))
                except Exception as e:
                    logger.error(f"解析影片信息失败（rowid：{rowid}）：{e}")
                    continue

            logger.info(f"需要更新评分的影片：{len(batch_update)} 部")
            if not batch_update:
                return

            # 3. 批量获取豆瓣评分（限流：10部/批，间隔10秒）
            batch_size = 10
            title_score_map = {}
            current_time_str = datetime.now(tz=pytz.timezone(settings.TZ)).strftime("%Y-%m-%d %H:%M:%S.%f") + \
                               datetime.now(tz=pytz.timezone(settings.TZ)).strftime("%z")[:3] + ":" + \
                               datetime.now(tz=pytz.timezone(settings.TZ)).strftime("%z")[3:]

            for i in range(0, len(batch_update), batch_size):
                if self._should_stop:
                    return

                batch = batch_update[i:i+batch_size]
                logger.info(f"处理第 {i//batch_size + 1} 批：{len(batch)} 部影片")

                # 单批内获取评分
                for rowid, title, old_score, meta_info in batch:
                    if title in title_score_map:
                        score = title_score_map[title]
                    else:
                        # 豆瓣接口请求（带重试）
                        retry_count = 3
                        score = None
                        while retry_count > 0:
                            try:
                                _, _, score = self._douban_helper.get_subject_id(title)
                                if score and score != "0":
                                    break
                            except Exception as e:
                                logger.error(f"获取 {title} 评分失败（剩余重试：{retry_count-1}）：{e}")
                                time.sleep(2)
                            retry_count -= 1
                        title_score_map[title] = score or old_score  # 无评分则保留原分数

                # 批间延迟，避免限流
                if i + batch_size < len(batch_update):
                    logger.info("批处理完成，延迟10秒避免豆瓣限流...")
                    time.sleep(10)

            # 4. 批量更新数据库
            update_sql = """
            UPDATE zvideo_collection 
            SET meta_info = ?, updated_at = ?, score = CAST(?) AS DECIMAL(3,1)
            WHERE rowid = ?
            """
            update_params = []
            for rowid, title, old_score, meta_info in batch_update:
                score = title_score_map.get(title, old_score)
                try:
                    score = float(score) if score else old_score
                except:
                    score = old_score

                # 更新meta_info和缓存
                meta_info["douban_score"] = score
                updated_meta = json.dumps(meta_info, ensure_ascii=False)
                update_params.append((updated_meta, current_time_str, score, rowid))

                # 构建通知消息
                if old_score == 0 and score > 0:
                    logger.info(f"首次获取评分：{title} → {score}")
                    message += f"✅ {title}：新增豆瓣评分 {score}\n"
                elif old_score != score and score > 0:
                    logger.info(f"评分更新：{title} {old_score} → {score}")
                    message += f"🔄 {title}：评分更新 {old_score} → {score}\n"

            # 执行批量更新
            if update_params:
                cursor.executemany(update_sql, update_params)
                conn.commit()
                logger.info(f"成功更新 {len(update_params)} 部影片评分")

                # 更新缓存
                for rowid, title, old_score, meta_info in batch_update:
                    self._cached_data[title] = {
                        "score": title_score_map.get(title, old_score),
                        "time": current_time.strftime("%Y-%m-%d %H:%M:%S")
                    }
                self.save_data("doubanratingsync", self._cached_data)

            # 发送通知
            if self._notify and message:
                self.post_message(
                    mtype=NotificationType.SiteMessage,
                    title="【豆瓣评分修正】同步结果",
                    text=message[:1000]  # 限制消息长度
                )

        except sqlite3.Error as e:
            if conn:
                conn.rollback()
            logger.error(f"数据库操作失败：{e}")
            if self._notify:
                self.post_message(
                    mtype=NotificationType.SiteMessage,
                    title="【豆瓣评分修正】同步失败",
                    text=f"数据库操作异常：{str(e)}"
                )
        finally:
            # 释放资源
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            logger.info("豆瓣评分同步任务结束")

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """配置表单（Vuetify组件，适配MoviePilot界面）"""
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用插件", "color": "primary"}
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "notify", "label": "同步结果通知"}
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "onlyonce", "label": "立即同步一次"}
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cron",
                                            "label": "定时同步周期（Cron表达式）",
                                            "placeholder": "默认：0 1 * * *（每天凌晨1点）",
                                            "hint": "Cron格式：分 时 日 月 周，例如 0 3 * * * 每天3点"
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "douban_score_update_days",
                                            "label": "评分更新周期（天）",
                                            "type": "number",
                                            "min": 0,
                                            "placeholder": "默认：7天",
                                            "hint": "0表示仅首次获取，不更新"
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "cookie",
                                            "label": "豆瓣Cookie（可选）",
                                            "rows": 2,
                                            "placeholder": "留空则从CookieCloud获取，格式：name1=value1; name2=value2"
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "db_path",
                                            "label": "极影视数据库路径（必填）",
                                            "rows": 1,
                                            "placeholder": "示例：/zspace/zsrp/sqlite/zvideo/zvideo.db",
                                            "hint": "需通过Portainer/1Panel映射极影视数据库文件到容器可访问路径"
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "error",
                                            "variant": "tonal",
                                            "text": "⚠️ 重要提示：使用前请备份极影视数据库，避免数据异常！"
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "notify": True,
            "onlyonce": False,
            "cron": "0 1 * * *",
            "douban_score_update_days": 7,
            "cookie": "",
            "db_path": ""
        }

    def get_page(self) -> List[dict]:
        """无需详情页面，返回空"""
        return []

    def stop_service(self):
        """停止定时任务"""
        self._should_stop = True
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error(f"停止服务失败：{e}")


if __name__ == "__main__":
    # 测试代码（本地运行时使用）
    plugin = DoubanRatingSync()
    plugin.init_plugin({
        "enabled": True,
        "onlyonce": True,
        "db_path": "/path/to/zvideo.db",
        "cookie": "your_douban_cookie",
        "notify": True
    })