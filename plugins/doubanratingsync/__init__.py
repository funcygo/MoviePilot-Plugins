from datetime import datetime, timedelta
import sqlite3
import json
import re
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from app.schemas.types import EventType, NotificationType
from app.core.event import eventmanager, Event
from pathlib import Path
from app.core.config import settings
from app.plugins import _PluginBase
from typing import Any, List, Dict, Tuple, Optional, Set
from app.log import logger
import time
from urllib.parse import unquote, quote
import requests
from bs4 import BeautifulSoup
from http.cookies import SimpleCookie
from app.helper.cookiecloud import CookieCloudHelper


class DoubanHelper:
    """豆瓣工具类（优化：支持ID查询+智能重试）"""
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
        self.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
        self.headers = {
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, sdch',
            'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4',
            'Connection': 'keep-alive',
            'DNT': '1'
        }

        # 日志输出Cookie状态
        if not self.cookies:
            logger.warning("豆瓣Cookie为空，可能导致查询失败或频率限制，建议配置Cookie")

    def get_rating_by_id(self, imdb_id: str = None, tmdb_id: str = None) -> Optional[str]:
        """通过IMDb/TMDB ID查询豆瓣评分（优先ID匹配，更精准）"""
        if not imdb_id and not tmdb_id:
            return None

        # 构造搜索URL（优先IMDb ID）
        if imdb_id:
            url = f"https://www.douban.com/search?cat=1002&q={quote(imdb_id)}"
            logger.debug(f"通过IMDb ID查询豆瓣：{imdb_id} → URL：{url}")
        else:
            url = f"https://www.douban.com/search?cat=1002&q=tmdb{tmdb_id}"
            logger.debug(f"通过TMDB ID查询豆瓣：{tmdb_id} → URL：{url}")

        try:
            response = requests.get(
                url,
                headers=self.headers,
                cookies=self.cookies,
                timeout=15,
                allow_redirects=True
            )
            response.raise_for_status()

            # 状态码处理
            if response.status_code == 429:
                logger.warning(f"豆瓣接口限流（ID查询）：{imdb_id or tmdb_id}")
                return None
            elif response.status_code != 200:
                logger.error(f"豆瓣ID查询状态码异常：{response.status_code}（ID：{imdb_id or tmdb_id}）")
                return None

            # 解析评分
            soup = BeautifulSoup(response.text, 'html.parser')
            rating_span = soup.find("span", class_="rating_nums")
            if rating_span:
                score = rating_span.get_text(strip=True)
                return score if score != "0" else None
            else:
                logger.debug(f"ID查询未找到评分：{imdb_id or tmdb_id}")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"ID查询豆瓣失败（{imdb_id or tmdb_id}）：{str(e)[:50]}")
            return None

    def get_rating_by_title(self, title: str, year: str = None) -> Optional[str]:
        """通过标题+年份查询豆瓣评分（兼容无ID场景）"""
        # 标题+年份组合搜索，提升准确率
        search_keyword = f"{title} {year}" if year else title
        encoded_title = quote(search_keyword, safe='')
        url = f"https://www.douban.com/search?cat=1002&q={encoded_title}"
        logger.debug(f"通过标题查询豆瓣：{search_keyword} → URL：{url}")

        try:
            response = requests.get(
                url,
                headers=self.headers,
                cookies=self.cookies,
                timeout=15,
                allow_redirects=True
            )
            response.raise_for_status()

            if response.status_code == 429:
                logger.warning(f"豆瓣接口限流（标题查询）：{search_keyword}")
                return None
            elif response.status_code != 200:
                logger.error(f"豆瓣标题查询状态码异常：{response.status_code}（关键词：{search_keyword}）")
                return None

            # 解析搜索结果（优先匹配年份）
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
                item["score"] = rating_span.get_text(strip=True) if rating_span else None
                subject_items.append(item)

            # 优先匹配年份
            if year:
                for item in subject_items:
                    if item["year"] == year and item["score"] and item["score"] != "0":
                        return item["score"]
            # 无年份匹配则返回第一个有效评分
            for item in subject_items:
                if item["score"] and item["score"] != "0":
                    return item["score"]

            logger.debug(f"标题查询未找到有效评分：{search_keyword}")
            return None

        except requests.exceptions.RequestException as e:
            logger.error(f"标题查询豆瓣失败（{search_keyword}）：{str(e)[:50]}")
            return None

    def get_rating(self, meta_info: dict) -> Optional[str]:
        """统一评分查询入口：先ID后标题，智能匹配"""
        # 从meta_info提取IMDb/TMDB ID和年份
        imdb_id = meta_info.get("imdb_id")
        tmdb_id = meta_info.get("tmdb_id")
        year = str(meta_info.get("year", "")) if meta_info.get("year") else None
        title = meta_info.get("title", "")

        # 1. 优先通过IMDb ID查询
        if imdb_id and imdb_id.startswith("tt"):
            score = self.get_rating_by_id(imdb_id=imdb_id)
            if score:
                return score
        # 2. 其次通过TMDB ID查询
        if tmdb_id:
            score = self.get_rating_by_id(tmdb_id=str(tmdb_id))
            if score:
                return score
        # 3. 最后通过标题+年份查询
        score = self.get_rating_by_title(title=title, year=year)
        return score


class DoubanRatingSync(_PluginBase):
    # 插件基础信息（唯一标识，避免冲突）
    plugin_name = "豆瓣评分修正"
    plugin_desc = "同步豆瓣评分至极影视（优先ID匹配，无评分保留原数据，支持进度追踪）"
    plugin_icon = "https://img9.doubanio.com/favicon.ico"
    plugin_version = "1.2"  # 优化版版本号
    plugin_author = "funcygo"
    author_url = "https://github.com/funcygo"
    plugin_config_prefix = "doubanratingsync"
    plugin_order = 10
    auth_level = 1

    # 私有属性（新增配置项）
    _enabled = False
    _cron = "0 1 * * *"  # 每天凌晨1点执行
    _notify = True
    _onlyonce = False
    _db_path = ""
    _cookie = ""
    _douban_score_update_days = 30  # 默认30天更新
    _max_sync_count = 100  # 单次同步最大影片数（新增）
    _sync_types: Set[str] = {"movie", "tv"}  # 同步类型：电影/电视剧（新增）
    _cached_data: dict = {}  # 缓存：标题→{score, time}
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
            self._notify = config.get("notify", True)
            self._onlyonce = config.get("onlyonce", False)
            self._db_path = config.get("db_path", "")
            self._cookie = config.get("cookie", "")
            self._douban_score_update_days = int(config.get("douban_score_update_days", 30))
            self._max_sync_count = int(config.get("max_sync_count", 100))
            self._sync_types = set(config.get("sync_types", ["movie", "tv"]))  # 转为集合
            # 初始化豆瓣工具类
            self._douban_helper = DoubanHelper(user_cookie=self._cookie)

        # 加载并清理缓存（清理超过365天的记录）
        self._cached_data = self.get_data("doubanratingsync") or {}
        self._clean_expired_cache()
        logger.info(f"缓存初始化完成，有效缓存数：{len(self._cached_data)}")

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
            logger.info("豆瓣评分修正：立即执行一次同步任务")
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

    def _clean_expired_cache(self):
        """清理过期缓存（超过365天）"""
        current_time = datetime.now()
        expired_keys = []
        for title, cache_info in self._cached_data.items():
            try:
                cache_time = datetime.strptime(cache_info.get("time", ""), "%Y-%m-%d %H:%M:%S")
                if (current_time - cache_time).days > 365:
                    expired_keys.append(title)
            except:
                expired_keys.append(title)  # 格式异常的缓存也清理

        if expired_keys:
            for key in expired_keys:
                del self._cached_data[key]
            self.save_data("doubanratingsync", self._cached_data)
            logger.info(f"清理过期缓存：{len(expired_keys)} 条")

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
            "douban_score_update_days": self._douban_score_update_days,
            "max_sync_count": self._max_sync_count,
            "sync_types": list(self._sync_types)  # 集合转列表存储
        })

    def get_command(self) -> List[Dict[str, Any]]:
        """注册手动命令"""
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
                text="豆瓣评分同步任务已完成！可查看日志了解详情",
                userid=event.event_data.get("user")
            )

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_service(self) -> List[Dict[str, Any]]:
        """注册公共定时服务"""
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

    def _get_media_type(self, meta_info: dict) -> Optional[str]:
        """识别影片类型（movie/tv/other）"""
        media_type = meta_info.get("type")
        if media_type == "电影":
            return "movie"
        elif media_type in ["电视剧", "综艺", "动漫"]:  # 综艺/动漫归为tv类
            return "tv"
        else:
            return "other"

    def sync_douban_rating(self):
        """核心同步逻辑（全优化版）"""
        self._should_stop = False
        logger.info("="*60 + " 豆瓣评分修正同步开始 " + "="*60)
        message = ""

        # 进度统计变量
        total_query = 0  # 查询到的有效影片总数
        total_filtered = 0  # 筛选后符合条件（类型+需更新）的总数
        updated_count = 0  # 成功更新数量
        skipped_count = 0  # 跳过数量
        skipped_logs = []  # 跳过明细
        failed_count = 0  # 失败数量
        failed_logs = []  # 失败明细

        try:
            # 1. 数据库连接（上下文管理器，自动关闭）
            with sqlite3.connect(self._db_path) as conn:
                conn.text_factory = str
                cursor = conn.cursor()

                # 2. 查询待处理影片（SQL优化：仅查询必要字段，减少数据传输）
                cursor.execute("""
                SELECT DISTINCT zc.rowid, zc.meta_info, zc.updated_at
                FROM zvideo_collection zc 
                LEFT JOIN zvideo_list zl ON zc.collection_id = zl.collection_id 
                WHERE zc.extend_type != 7  -- 排除合集
                  AND JSON_EXTRACT(zc.meta_info, '$.douban_score') IS NOT NULL  -- 含douban_score字段
                  AND (zl.path NOT LIKE '/tmp/zfsv3/sata11/13107640652/data/RR%' OR zl.path IS NULL)  -- 过滤无效路径
                LIMIT ?  -- 限制单次查询数量
                """, (self._max_sync_count,))
                rows = cursor.fetchall()
                total_query = len(rows)
                logger.info(f"【查询结果】共查询到 {total_query} 部有效影片（已去重，单次上限：{self._max_sync_count}）")

                if not rows:
                    logger.info("【无任务】无需要处理的影片，任务结束")
                    return

                # 3. 筛选符合条件的影片（类型+需更新）
                current_time = datetime.now()
                batch_update = []
                for row in rows:
                    if self._should_stop:
                        logger.info("【任务中断】检测到中断请求，停止同步")
                        return

                    rowid, meta_info_json, updated_at = row
                    try:
                        # 解析meta_info（容错：字段缺失处理）
                        meta_info = json.loads(meta_info_json)
                        title = meta_info.get("title", f"未知影片（rowid：{rowid}）")
                        old_score = float(meta_info.get("douban_score", 0))
                        media_type = self._get_media_type(meta_info)

                        # 类型筛选：跳过未勾选的类型
                        if media_type not in self._sync_types:
                            skipped_count += 1
                            skip_reason = f"【跳过-类型不匹配】{title}：类型{media_type}未勾选同步"
                            logger.info(skip_reason)
                            skipped_logs.append(skip_reason)
                            continue

                        # 缓存检查：是否需要更新
                        cache_info = self._cached_data.get(title, {})
                        need_update = False
                        if not cache_info:
                            need_update = True
                            logger.debug(f"【缓存未命中】{title}：首次处理，需更新")
                        else:
                            cache_score = cache_info.get("score")
                            try:
                                cache_time = datetime.strptime(cache_info.get("time", ""), "%Y-%m-%d %H:%M:%S")
                                # 缓存过期或评分变化需更新
                                if (current_time - cache_time).days >= self._douban_score_update_days or str(cache_score) != str(old_score):
                                    need_update = True
                                    logger.debug(f"【缓存过期/评分变化】{title}：缓存时间{cache_time.strftime('%Y-%m-%d')}，需更新")
                                else:
                                    skipped_count += 1
                                    skip_reason = f"【跳过-缓存未过期】{title}：缓存更新于{cache_time.strftime('%Y-%m-%d')}，评分{old_score}未变化（有效期至{cache_time.strftime('%Y-%m-%d')}）"
                                    logger.info(skip_reason)
                                    skipped_logs.append(skip_reason)
                            except:
                                # 缓存格式异常，视为需要更新
                                need_update = True
                                logger.debug(f"【缓存格式异常】{title}：重新获取评分")

                        if need_update:
                            batch_update.append((rowid, title, old_score, meta_info))
                    except json.JSONDecodeError as e:
                        # JSON解析失败：跳过并记录
                        skipped_count += 1
                        skip_reason = f"【跳过-解析失败】rowid：{rowid}，JSON解析错误：{str(e)[:30]}"
                        logger.error(skip_reason)
                        skipped_logs.append(skip_reason)
                    except Exception as e:
                        # 其他异常：跳过并记录
                        skipped_count += 1
                        skip_reason = f"【跳过-未知错误】{title}（rowid：{rowid}）：{str(e)[:30]}"
                        logger.error(skip_reason)
                        skipped_logs.append(skip_reason)

                total_filtered = len(batch_update)
                logger.info(f"\n【筛选结果】符合条件需更新影片：{total_filtered} 部，已跳过：{skipped_count} 部")
                if not batch_update:
                    logger.info("【无更新任务】所有影片均无需更新，任务结束")
                    return

                # 4. 批量获取豆瓣评分（智能重试+限流）
                batch_size = 10  # 每批10部
                title_score_map = {}
                tz = pytz.timezone(settings.TZ)
                current_time_str = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S.%f") + \
                                   datetime.now(tz).strftime("%z")[:3] + ":" + \
                                   datetime.now(tz).strftime("%z")[3:]

                for i in range(0, len(batch_update), batch_size):
                    if self._should_stop:
                        return

                    batch = batch_update[i:i+batch_size]
                    batch_idx = i//batch_size + 1
                    processed_in_batch = 0  # 本批更新数
                    logger.info(f"\n【批处理】开始第 {batch_idx} 批，本批 {len(batch)} 部影片（总进度：{updated_count}/{total_filtered}）")

                    for rowid, title, old_score, meta_info in batch:
                        remaining = total_filtered - updated_count
                        logger.info(f"【获取评分】[{updated_count+1}/{total_filtered}] 正在查询：{title}（原评分：{old_score}，类型：{self._get_media_type(meta_info)}）")

                        # 优先从缓存获取（批内去重）
                        if title in title_score_map:
                            score = title_score_map[title]
                        else:
                            # 智能重试：根据响应调整策略
                            retry_count = 3
                            score = None
                            retry_delay = 2  # 初始重试间隔2秒
                            while retry_count > 0:
                                try:
                                    score = self._douban_helper.get_rating(meta_info)
                                    if score:
                                        break
                                    # 未获取到评分，直接退出重试
                                    if not score:
                                        break
                                except Exception as e:
                                    logger.error(f"【获取失败】{title}（剩余重试：{retry_count-1}）：{str(e)[:50]}")
                                    time.sleep(retry_delay)
                                    # 每次重试间隔翻倍（2→4→8秒）
                                    retry_delay *= 2
                                retry_count -= 1
                            title_score_map[title] = score  # 即使为None也存储，避免重复请求

                        # 计算有效期
                        expire_time = current_time + timedelta(days=self._douban_score_update_days)
                        expire_str = expire_time.strftime("%Y-%m-%d")

                        # 处理结果
                        if score:
                            new_score = float(score)
                            if new_score != old_score:
                                # 评分更新
                                logger.info(f"【评分更新】[{updated_count+1}/{total_filtered}] {title}：原评分 {old_score} → 豆瓣评分 {new_score}（有效期至 {expire_str}）")
                                # 记录到更新列表
                                title_score_map[title] = new_score
                                processed_in_batch += 1
                            else:
                                # 评分一致，跳过
                                skipped_count += 1
                                skip_reason = f"【跳过-评分一致】{title}：豆瓣评分{new_score}与原评分一致（有效期至 {expire_str}）"
                                logger.info(skip_reason)
                                skipped_logs.append(skip_reason)
                                title_score_map[title] = old_score
                        else:
                            # 未获取到有效评分，跳过
                            skipped_count += 1
                            skip_reason = f"【跳过-无有效评分】{title}：未查询到有效豆瓣评分，保留原评分{old_score}（有效期至 {expire_str}）"
                            logger.info(skip_reason)
                            skipped_logs.append(skip_reason)
                            title_score_map[title] = old_score

                    # 批间限流：避免触发豆瓣频率限制
                    if i + batch_size < len(batch_update):
                        logger.info(f"【批处理完成】第 {batch_idx} 批结束，本批更新 {processed_in_batch} 部，累计更新 {updated_count} 部，剩余 {total_filtered - updated_count} 部")
                        logger.info("【限流延迟】等待10秒后继续下一批...")
                        time.sleep(10)

                # 5. 批量更新数据库（事务安全）
                update_sql = """
                UPDATE zvideo_collection 
                SET meta_info = ?,  -- 更新含豆瓣评分的meta_info
                    updated_at = ?,  -- 更新时间
                    score = CAST(?) AS DECIMAL(3,1)  -- 同步到score列（前端显示）
                WHERE rowid = ?
                """
                update_params = []

                for rowid, title, old_score, meta_info in batch_update:
                    new_score = title_score_map.get(title, old_score)
                    try:
                        new_score = float(new_score) if new_score else old_score
                    except:
                        new_score = old_score
                        failed_count += 1
                        failed_logs.append(f"【更新失败】{title}：评分格式异常，无法更新")
                        continue

                    # 仅更新评分有变化且有效（>0）的记录
                    if new_score != old_score and new_score > 0:
                        meta_info["douban_score"] = new_score
                        # 补充更新时间到meta_info（可选）
                        meta_info["douban_score_updated_at"] = current_time.strftime("%Y-%m-%d %H:%M:%S")
                        updated_meta = json.dumps(meta_info, ensure_ascii=False)
                        update_params.append((updated_meta, current_time_str, new_score, rowid))
                        updated_count += 1

                        # 构建通知消息
                        expire_time = current_time + timedelta(days=self._douban_score_update_days)
                        expire_str = expire_time.strftime("%Y-%m-%d")
                        message += f"🔄 {title}：原评分 {old_score} → 豆瓣评分 {new_score}（有效期至 {expire_str}）\n"

                # 执行批量更新
                if update_params:
                    cursor.executemany(update_sql, update_params)
                    conn.commit()
                    logger.info(f"\n【数据库更新完成】共更新 {len(update_params)} 部影片评分（累计更新：{updated_count} 部）")

                    # 更新缓存（包含本次所有处理的影片，无论是否更新）
                    for rowid, title, old_score, meta_info in batch_update:
                        final_score = title_score_map.get(title, old_score)
                        self._cached_data[title] = {
                            "score": final_score,
                            "time": current_time.strftime("%Y-%m-%d %H:%M:%S")
                        }
                    self.save_data("doubanratingsync", self._cached_data)
                else:
                    logger.info("【无更新】无需要写入数据库的评分变化")

        except sqlite3.Error as e:
            logger.error(f"\n【数据库错误】{str(e)}")
            failed_count += 1
            failed_logs.append(f"【数据库错误】{str(e)}")
            if self._notify:
                self.post_message(
                    mtype=NotificationType.SiteMessage,
                    title="【豆瓣评分修正】同步失败",
                    text=f"数据库操作异常：{str(e)}"
                )
        except Exception as e:
            logger.error(f"\n【全局错误】同步任务异常终止：{str(e)}")
            failed_count += 1
            failed_logs.append(f"【全局错误】{str(e)}")
            if self._notify:
                self.post_message(
                    mtype=NotificationType.SiteMessage,
                    title="【豆瓣评分修正】同步失败",
                    text=f"任务异常终止：{str(e)}"
                )

        # 6. 任务汇总日志（完整明细）
        logger.info("\n" + "="*60 + " 同步任务汇总 " + "="*60)
        logger.info(f"查询到有效影片：{total_query} 部")
        logger.info(f"符合条件需更新影片：{total_filtered} 部")
        logger.info(f"成功更新影片：{updated_count} 部")
        logger.info(f"跳过影片：{skipped_count} 部")
        logger.info(f"失败影片：{failed_count} 部")
        logger.info(f"更新率：{updated_count/total_filtered*100:.1f}%" if total_filtered > 0 else "0.0%")

        # 跳过明细
        if skipped_logs:
            logger.info(f"\n【跳过影片明细】（共 {len(skipped_logs)} 部）")
            for idx, log in enumerate(skipped_logs[:50], 1):  # 最多显示50条，避免日志过长
                logger.info(f"{idx}. {log}")
            if len(skipped_logs) > 50:
                logger.info(f"... 还有 {len(skipped_logs)-50} 条跳过记录，可查看debug日志")

        # 失败明细
        if failed_logs:
            logger.info(f"\n【失败影片明细】（共 {len(failed_logs)} 部）")
            for idx, log in enumerate(failed_logs, 1):
                logger.info(f"{idx}. {log}")

        logger.info("="*60 + " 同步任务结束 " + "="*60)

        # 发送通知（限制长度）
        if self._notify and message:
            max_msg_len = 2000
            if len(message) > max_msg_len:
                message = message[:max_msg_len] + f"\n... 还有 {len(message)-max_msg_len} 字符未显示"
            self.post_message(
                mtype=NotificationType.SiteMessage,
                title=f"【豆瓣评分修正】同步完成（更新 {updated_count} 部）",
                text=message
            )

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """配置表单（新增配置项）"""
        return [
            {
                "component": "VForm",
                "content": [
                    # 基础配置行
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用插件", "color": "primary"}
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "notify", "label": "同步结果通知"}
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "onlyonce", "label": "立即同步一次"}
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "max_sync_count",
                                            "label": "单次同步最大数量",
                                            "type": "number",
                                            "min": 10,
                                            "max": 500,
                                            "placeholder": "默认：100"
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    # 同步类型筛选行
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "sync_types",
                                            "label": "同步类型筛选",
                                            "multiple": True,
                                            "items": [
                                                {"label": "电影", "value": "movie"},
                                                {"label": "电视剧/综艺/动漫", "value": "tv"}
                                            ],
                                            "placeholder": "默认：电影+电视剧/综艺/动漫"
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
                                            "placeholder": "默认：30天",
                                            "hint": "0表示仅首次获取，不自动更新"
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    # 定时配置行
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
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
                            }
                        ]
                    },
                    # Cookie配置行
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
                                            "placeholder": "留空则从CookieCloud获取，格式：name1=value1; name2=value2",
                                            "hint": "建议配置，避免豆瓣限流"
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    # 数据库路径行
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
                    # 提示行
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
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "⚠️ 重要提示：1. 使用前请备份极影视数据库；2. 配置Cookie可降低限流风险；3. 同步类型建议按需勾选，提升效率"
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
            "douban_score_update_days": 30,
            "max_sync_count": 100,
            "sync_types": ["movie", "tv"],
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
    # 本地测试代码
    plugin = DoubanRatingSync()
    plugin.init_plugin({
        "enabled": True,
        "onlyonce": True,
        "db_path": "/path/to/zvideo.db",
        "cookie": "your_douban_cookie",
        "notify": True,
        "max_sync_count": 50,
        "sync_types": ["movie", "tv"],
        "douban_score_update_days": 30
    })