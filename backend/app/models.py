"""领域数据模型：跨模块传递的真实业务对象，不只dict传递。

每个数据类描述在一个数据流中由一个模块传给下一个模块的数据结构，
包含校验、序列化和失败处理能力。
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# 枚举
# ---------------------------------------------------------------------------

class SiteType(str, Enum):
    NEWS = "新闻门户"
    CONTENT_SITE = "内容平台"
    BBS = "社区论坛"
    SEARCH = "搜索引擎"
    SOCIAL = "社交媒体"
    GOVERNMENT = "政府公示"
    FINANCE = "金融资讯"


class ProxyType(str, Enum):
    HTTP = "HTTP"
    HTTPS = "HTTPS"
    SOCKS5 = "SOCKS5"


class AntiType(str, Enum):
    IP_LIMIT = "IP限频"
    UA_CHECK = "UA检测"
    COOKIE_CHECK = "Cookie校验"
    CAPTCHA = "验证码"
    JS_CHALLENGE = "JS挑战"
    FINGERPRINT = "浏览器指纹"


class CleanType(str, Enum):
    TRIM = "去空格"
    NORMALIZE = "标准化"
    VALIDATE = "校验"
    DEFAULT_FILL = "默认填充"
    REGEX_EXTRACT = "正则提取"
    TYPE_CAST = "类型转换"


class DeliveryFormat(str, Enum):
    CSV = "CSV"
    JSON = "JSON"
    PARQUET = "Parquet"
    AVRO = "Avro"


class SampleMethod(str, Enum):
    RANDOM = "随机抽样"
    STRATIFIED = "分层抽样"
    SYSTEMATIC = "等距抽样"
    TOP_N = "头N条"


class ArchiveFormat(str, Enum):
    GZIP = "gzip"
    LZ4 = "lz4"
    SNAPPY = "snappy"
    ZSTD = "zstd"


# ---------------------------------------------------------------------------
# 跨模块数据类
# ---------------------------------------------------------------------------

@dataclass
class SiteConfig:
    """采集站点配置：由 crawl_site 生成，crawl_job 消费。"""
    site_id: str
    domain: str
    site_type: SiteType
    robots_policy: str
    language: str
    region: str
    access_method: str
    registered_at: str
    verified: bool = False
    last_validated: str | None = None

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> SiteConfig:
        site_type_str = str(row.get("站点类型", SiteType.NEWS.value))
        try:
            st = SiteType(site_type_str)
        except ValueError:
            st = SiteType.NEWS
        return cls(
            site_id=str(row.get("站点编号", "")),
            domain=str(row.get("站点域名", "")),
            site_type=st,
            robots_policy=str(row.get("Robots协议", "allow")),
            language=str(row.get("站点语言", "zh-CN")),
            region=str(row.get("地域归属", "CN")),
            access_method=str(row.get("接入方式", "direct")),
            registered_at=str(row.get("注册时间", "")),
            verified=row.get("status") == "已验证",
            last_validated=str(row.get("注册时间", "")),
        )

    def is_crawlable(self) -> bool:
        return self.verified and self.robots_policy.lower() != "disallow"


@dataclass
class CrawlJobSpec:
    """采集作业规格：由 crawl_job 生成，scheduler/interactor 消费。"""
    job_id: str
    site: SiteConfig
    entry_urls: list[str] = field(default_factory=list)
    max_depth: int = 3
    concurrency: int = 4
    schedule_policy: str = "interval"
    completed_pages: int = 0
    failed_pages: int = 0
    status: str = "待执行"
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @classmethod
    def from_seed_row(cls, row: dict[str, Any], site: SiteConfig | None = None) -> CrawlJobSpec:
        raw_url = str(row.get("入口URL", ""))
        # 支持逗号分隔的多 URL
        urls = [u.strip() for u in raw_url.split(",") if u.strip()] if raw_url else []
        try:
            md = int(row.get("最大深度", 3))
        except (ValueError, TypeError):
            md = 3
        try:
            cc = int(row.get("并发线程", 4))
        except (ValueError, TypeError):
            cc = 4
        try:
            cp = int(row.get("完成页数", 0))
        except (ValueError, TypeError):
            cp = 0
        try:
            fp = int(row.get("失败页数", 0))
        except (ValueError, TypeError):
            fp = 0
        return cls(
            job_id=str(row.get("作业编号", "")),
            site=site or SiteConfig(site_id="", domain="", site_type=SiteType.NEWS,
                                     robots_policy="allow", language="zh-CN",
                                     region="CN", access_method="direct", registered_at=""),
            entry_urls=urls,
            max_depth=md,
            concurrency=cc,
            schedule_policy=str(row.get("调度策略", "interval")),
            completed_pages=cp,
            failed_pages=fp,
            status=str(row.get("status", "待执行")),
        )


@dataclass
class UrlFingerprint:
    """URL 去重指纹：由 url_dedup 生成，crawl_job 在调度前检查。"""
    fingerprint_id: str
    source_url: str
    algorithm: str
    hash_value: str
    discovery_channel: str
    first_seen: str
    duplicate_count: int = 0
    status: str = "已记录"

    @classmethod
    def compute_v1_hash(cls, url: str, algorithm: str = "sha256") -> str:
        """v1 指纹：通用参数排序，不区分参数语义（采集任务入口现状）。"""
        parsed = urlparse(url)
        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        params = "&".join(sorted(parsed.query.split("&"))) if parsed.query else ""
        if params:
            normalized += f"?{params}"
        return hashlib.new(algorithm, normalized.encode()).hexdigest()

    @classmethod
    def compute_v2_hash(cls, url: str, policy: SiteParamPolicy | None = None,
                        algorithm: str = "sha256") -> str:
        """v2 指纹：按站点参数语义剔除跟踪参数，功能参数保留并排序。

        未登记语义的参数一律按功能参数保留（保守策略），保证新规则
        不误合并功能参数值不同的地址；无站点策略时退化为 v1 通用排序。
        """
        parsed = urlparse(url)
        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if parsed.query:
            kept: list[str] = []
            for pair in parsed.query.split("&"):
                if not pair:
                    continue
                name = pair.split("=", 1)[0]
                if policy is not None and policy.is_tracking(name):
                    continue  # 跟踪参数不参与业务等价
                kept.append(pair)
            if kept:
                normalized += f"?{'&'.join(sorted(kept))}"
        return hashlib.new(algorithm, normalized.encode()).hexdigest()

    @classmethod
    def compute(cls, url: str, algorithm: str = "sha256") -> UrlFingerprint:
        h = cls.compute_v1_hash(url, algorithm)
        return cls(
            fingerprint_id=f"FP-{h[:12]}",
            source_url=url,
            algorithm=algorithm,
            hash_value=h,
            discovery_channel="crawl_job",
            first_seen=datetime.now(timezone.utc).isoformat(),
        )

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> UrlFingerprint:
        try:
            dup = int(row.get("重复次数", 0))
        except (ValueError, TypeError):
            dup = 0
        return cls(
            fingerprint_id=str(row.get("指纹编号", "")),
            source_url=str(row.get("源URL", "")),
            algorithm=str(row.get("指纹算法", "sha256")),
            hash_value=str(row.get("指纹哈希", "")),
            discovery_channel=str(row.get("发现渠道", "")),
            first_seen=str(row.get("首次发现", "")),
            duplicate_count=dup,
            status=str(row.get("status", "已记录")),
        )


# ---------------------------------------------------------------------------
# 指纹 v1→v2 迁移：站点参数语义、兼容回执、执行确认凭证
# ---------------------------------------------------------------------------

class ParamRole(str, Enum):
    FUNCTIONAL = "功能参数"
    TRACKING = "跟踪参数"


@dataclass(frozen=True)
class SiteParamPolicy:
    """站点参数语义策略：v2 指纹按站点区分功能参数与跟踪参数。

    未登记的参数一律按功能参数保留（保守策略），保证新规则不误合并
    功能参数值不同的地址。
    """
    site_domain: str
    functional_params: tuple[str, ...] = ()
    tracking_params: tuple[str, ...] = ()

    def role_of(self, param_name: str) -> ParamRole:
        if param_name in self.tracking_params:
            return ParamRole.TRACKING
        return ParamRole.FUNCTIONAL

    def is_tracking(self, param_name: str) -> bool:
        return self.role_of(param_name) == ParamRole.TRACKING


@dataclass
class FingerprintCompatReceipt:
    """指纹兼容回执：同次上线期间新旧消费者据此互相承认对方登记的指纹。

    存量字段（旧 URL、旧摘要、重复次数）一律保留；无法核实来源或缺原始
    网址的记录只做恒等转换（v2_hash == legacy_hash），转换原因随回执保留。
    """
    receipt_id: str
    source_channel: str
    site_domain: str
    legacy_url: str
    legacy_hash: str
    v2_hash: str
    duplicate_count: int = 0
    rule_version: str = "v1"        # 转换时实际使用的规则版本
    conversion_reason: str = ""     # 恒等转换的原因；正常转换为已按站点参数语义转换
    status: str = "已确认"           # 已确认 / 已回滚

    @property
    def is_identity(self) -> bool:
        """恒等转换的回执只认旧键，不参与 v2 等价合并。"""
        return self.v2_hash == self.legacy_hash

    def canonical_key(self, rule_version: str) -> str:
        """按消费者规则版本给出业务等价键。"""
        return self.v2_hash if rule_version == "v2" else self.legacy_hash


@dataclass
class ExecutionConfirmation:
    """执行确认凭证：同一来源同一旧键只确认一个执行机会。

    重复导入与回滚重放都沿用该凭证，不新增执行；replay_count 只记录重放次数。
    凭证上保留实际使用的规则版本和对应旧键，回滚后可凭旧键命中。
    """
    confirmation_id: str
    receipt_id: str
    source_channel: str
    legacy_hash: str
    canonical_key: str
    rule_version: str
    executable_address: str
    confirmed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    replay_count: int = 0


@dataclass
class ResolvedEntry:
    """调度解析结果：一个入口地址在某规则版本下的归一化结论。"""
    input_url: str
    source_channel: str
    legacy_hash: str
    canonical_key: str        # 当前规则版本下的业务等价键
    executable_address: str   # 调度实际选的地址
    rule_version: str         # 实际使用的规则版本
    receipt_id: str = ""
    conversion_reason: str = ""


@dataclass
class FingerprintRegistry:
    """指纹迁移注册表：兼容回执与执行确认凭证的登记、查询与回滚。

    回执与确认都以 (来源, 旧键) 去重：重复导入不新增回执，
    回滚后重放不新增执行机会。
    """
    active_rule_version: str = "v2"   # 本次上线生效的规则版本；回滚后为 v1
    rolled_back: bool = False
    receipts: dict[tuple[str, str], FingerprintCompatReceipt] = field(default_factory=dict)
    confirmations: dict[tuple[str, str], ExecutionConfirmation] = field(default_factory=dict)

    def receipt_for(self, source_channel: str, legacy_hash: str) -> FingerprintCompatReceipt | None:
        return self.receipts.get((source_channel, legacy_hash))

    def confirmation_for(self, source_channel: str, legacy_hash: str) -> ExecutionConfirmation | None:
        return self.confirmations.get((source_channel, legacy_hash))

    def execution_count(self) -> int:
        """已确认的执行机会总数（重放不计入）。"""
        return len(self.confirmations)


@dataclass
class ExtractRule:
    """内容抽取规则：由 content_extract 管理，crawl_job 在下载后调用。"""
    rule_id: str
    target_site: str
    xpath_selector: str
    field_mapping: dict[str, str] = field(default_factory=dict)
    data_type: str = "string"
    clean_regex: str = ""
    extract_order: int = 0
    status: str = "草稿"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> ExtractRule:
        return cls(
            rule_id=str(row.get("规则编号", "")),
            target_site=str(row.get("目标站点", "")),
            xpath_selector=str(row.get("XPath选择器", "")),
            field_mapping={"raw": str(row.get("字段映射", ""))},
            data_type=str(row.get("数据类型", "string")),
            clean_regex=str(row.get("清洗正则", "")),
            extract_order=int(row.get("抽取顺序", 0)),
            status=str(row.get("status", "草稿")),
        )


@dataclass
class RawExtractedContent:
    """原始抽取内容：content_extract 输出，field_clean 输入。"""
    source_url: str
    extract_rule_id: str
    raw_fields: dict[str, str] = field(default_factory=dict)
    extracted_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    errors: list[str] = field(default_factory=list)

    def has_errors(self) -> bool:
        return len(self.errors) > 0


@dataclass
class CleanRule:
    """字段清洗规则：field_clean 管理，对 RawExtractedContent 逐字段清洗。"""
    rule_id: str
    target_field: str
    clean_type: CleanType
    expression: str = ""
    null_strategy: str = "skip"
    validation_range: str = ""
    clean_order: int = 0
    status: str = "草稿"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> CleanRule:
        return cls(
            rule_id=str(row.get("规则编号", "")),
            target_field=str(row.get("目标字段", "")),
            clean_type=CleanType(str(row.get("清洗类型", CleanType.TRIM.value))),
            expression=str(row.get("处理表达式", "")),
            null_strategy=str(row.get("空值策略", "skip")),
            validation_range=str(row.get("校验范围", "")),
            clean_order=int(row.get("清洗顺序", 0)),
            status=str(row.get("status", "草稿")),
        )


@dataclass
class CleanedContent:
    """清洗后内容：field_clean 输出，batch_store 输入。"""
    source_url: str
    extract_rule_id: str
    fields: dict[str, Any] = field(default_factory=dict)
    clean_errors: list[str] = field(default_factory=list)
    cleaned_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def is_valid(self) -> bool:
        if len(self.clean_errors) > 0:
            return False
        if len(self.fields) == 0:
            return False
        return all(v for v in self.fields.values())


@dataclass
class StorageBatch:
    """存储批次：batch_store 管理，data_delivery/data_archive 消费。"""
    batch_id: str
    source_job_id: str
    row_count: int = 0
    storage_path: str = ""
    compression: str = "gzip"
    schema_version: str = "v1"
    checksum: str = ""
    stored_at: str = ""
    status: str = "采集中"
    records: list[CleanedContent] = field(default_factory=list)

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> StorageBatch:
        return cls(
            batch_id=str(row.get("批次编号", "")),
            source_job_id=str(row.get("来源作业", "")),
            row_count=int(row.get("数据行数", 0)),
            storage_path=str(row.get("存储路径", "")),
            compression=str(row.get("压缩格式", "gzip")),
            schema_version=str(row.get("Schema版本", "v1")),
            checksum=str(row.get("校验摘要", "")),
            stored_at=str(row.get("入库时间", "")),
            status=str(row.get("status", "采集中")),
        )

    def compute_checksum(self) -> str:
        payload = json.dumps([r.fields for r in self.records], sort_keys=True, default=str)
        return hashlib.sha256(payload.encode()).hexdigest()


@dataclass
class DeliveryTask:
    """数据投递任务：data_delivery 管理，从 batch_store 读取批次投递。"""
    task_id: str
    data_source: str
    target_system: str
    delivery_format: DeliveryFormat
    frequency: str
    partition_strategy: str
    last_delivery: str = ""
    status: str = "待投递"
    batch_ref_id: str = ""
    delivered_row_count: int = 0

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> DeliveryTask:
        df_str = str(row.get("投递格式", DeliveryFormat.CSV.value))
        try:
            df = DeliveryFormat(df_str)
        except ValueError:
            df = DeliveryFormat.CSV
        return cls(
            task_id=str(row.get("任务编号", "")),
            data_source=str(row.get("数据来源", "")),
            target_system=str(row.get("目标系统", "")),
            delivery_format=df,
            frequency=str(row.get("投递频率", "daily")),
            partition_strategy=str(row.get("分区策略", "")),
            last_delivery=str(row.get("最近投递", "")),
            status=str(row.get("status", "待投递")),
        )


@dataclass
class QualitySample:
    """质量抽样计划：quality_sample 管理，从 batch_store 提取样本检测。"""
    plan_id: str
    target_batch: str
    sample_ratio: float
    sample_method: SampleMethod
    sample_size: int = 0
    check_result: str = ""
    deviation_threshold: float = 0.05
    last_sampled: str = ""
    status: str = "待采样"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> QualitySample:
        ratio_str = str(row.get("抽样比例", "0.1"))
        try:
            ratio = float(ratio_str.rstrip("%")) / 100 if "%" in ratio_str else float(ratio_str)
        except (ValueError, TypeError):
            ratio = 0.1
        sm_str = str(row.get("抽样方式", SampleMethod.RANDOM.value))
        try:
            sm = SampleMethod(sm_str)
        except ValueError:
            sm = SampleMethod.RANDOM
        try:
            ss = int(row.get("样本数量", 0))
        except (ValueError, TypeError):
            ss = 0
        try:
            dt = float(row.get("偏差阈值", 0.05))
        except (ValueError, TypeError):
            dt = 0.05
        return cls(
            plan_id=str(row.get("计划编号", "")),
            target_batch=str(row.get("目标批次", "")),
            sample_ratio=ratio,
            sample_method=sm,
            sample_size=ss,
            check_result=str(row.get("检验结果", "")),
            deviation_threshold=dt,
            last_sampled=str(row.get("最近采样", "")),
            status=str(row.get("status", "待采样")),
        )


@dataclass
class ProxyNode:
    """代理节点：proxy_pool 管理，anti_block/crawl_cost 消费。"""
    node_id: str
    address: str
    proxy_type: ProxyType
    region_tag: str
    operator: str
    latency_ms: float = 0.0
    success_rate: float = 1.0
    last_checked: str = ""
    status: str = "待校验"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> ProxyNode:
        pt_str = str(row.get("代理类型", ProxyType.HTTP.value))
        try:
            pt = ProxyType(pt_str)
        except ValueError:
            pt = ProxyType.HTTP
        try:
            lat = float(row.get("响应延迟", 0))
        except (ValueError, TypeError):
            lat = 0.0
        try:
            sr = float(row.get("成功率", 1.0))
        except (ValueError, TypeError):
            sr = 1.0
        return cls(
            node_id=str(row.get("节点编号", "")),
            address=str(row.get("代理地址", "")),
            proxy_type=pt,
            region_tag=str(row.get("地域标签", "")),
            operator=str(row.get("运营商", "")),
            latency_ms=lat,
            success_rate=sr,
            last_checked=str(row.get("最后校验", "")),
            status=str(row.get("status", "待校验")),
        )

    def is_available(self) -> bool:
        return self.status == "可用" and self.success_rate >= 0.8 and self.latency_ms < 3000


@dataclass
class AntiBlockStrategy:
    """反爬对抗策略：anti_block 管理，结合 proxy_pool 提供代理轮换。"""
    strategy_id: str
    target_site: str
    anti_type: AntiType
    ua_pool: list[str] = field(default_factory=list)
    cookie_rotation: bool = False
    request_interval_ms: int = 1000
    captcha_engine: str = ""
    status: str = "待配置"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> AntiBlockStrategy:
        at_str = str(row.get("对抗类型", AntiType.IP_LIMIT.value))
        try:
            at = AntiType(at_str)
        except ValueError:
            at = AntiType.IP_LIMIT
        try:
            interval = int(row.get("请求间隔", 1000))
        except (ValueError, TypeError):
            interval = 1000
        return cls(
            strategy_id=str(row.get("策略编号", "")),
            target_site=str(row.get("目标站点", "")),
            anti_type=at,
            ua_pool=[str(row.get("UserAgent池", ""))] if row.get("UserAgent池") else [],
            cookie_rotation=bool(row.get("Cookie轮换", False)),
            request_interval_ms=interval,
            captcha_engine=str(row.get("验证码识别", "")),
            status=str(row.get("status", "待配置")),
        )


@dataclass
class CrawlLimitRule:
    """采集限速规则：crawl_limit 管理，控制 crawl_job 执行节奏。"""
    rule_id: str
    target_site: str
    request_interval_ms: int
    concurrency_max: int = 1
    daily_limit: int = 10000
    time_window: str = ""
    limit_type: str = "fixed"
    status: str = "草稿"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> CrawlLimitRule:
        try: interval = int(row.get("请求间隔", 1000))
        except (ValueError, TypeError): interval = 1000
        try: conc = int(row.get("并发上限", 1))
        except (ValueError, TypeError): conc = 1
        try: daily = int(row.get("日请求量", 10000))
        except (ValueError, TypeError): daily = 10000
        return cls(
            rule_id=str(row.get("策略编号", "")),
            target_site=str(row.get("目标站点", "")),
            request_interval_ms=interval,
            concurrency_max=conc,
            daily_limit=daily,
            time_window=str(row.get("时段窗口", "")),
            limit_type=str(row.get("限速类型", "fixed")),
            status=str(row.get("status", "草稿")),
        )


@dataclass
class ParseLogEntry:
    """解析日志记录：parse_log 管理，记录每次抽取的解析结果。"""
    record_id: str
    source_url: str
    parse_rule_id: str
    parse_result: str = ""
    error_type: str = ""
    retry_count: int = 0
    error_detail: str = ""
    recorded_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> ParseLogEntry:
        return cls(
            record_id=str(row.get("记录编号", "")),
            source_url=str(row.get("来源URL", "")),
            parse_rule_id=str(row.get("解析规则", "")),
            parse_result=str(row.get("解析结果", "")),
            error_type=str(row.get("异常类型", "")),
            retry_count=int(row.get("重试次数", 0)),
            error_detail=str(row.get("错误详情", "")),
            recorded_at=str(row.get("记录时间", "")),
        )


@dataclass
class IncrCursor:
    """增量采集游标：incr_crawl 管理，记录每个站点的增量位置。"""
    cursor_id: str
    target_site: str
    incr_field: str
    last_cursor: str = ""
    current_cursor: str = ""
    new_count: int = 0
    updated_count: int = 0
    last_sync: str = ""
    status: str = "待初始化"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> IncrCursor:
        return cls(
            cursor_id=str(row.get("标记编号", "")),
            target_site=str(row.get("目标站点", "")),
            incr_field=str(row.get("增量字段", "")),
            last_cursor=str(row.get("上次游标", "")),
            current_cursor=str(row.get("当前游标", "")),
            new_count=int(row.get("新增数量", 0)),
            updated_count=int(row.get("更新数量", 0)),
            last_sync=str(row.get("最后同步", "")),
            status=str(row.get("status", "待初始化")),
        )


@dataclass
class DiffRecord:
    """差异检测记录：diff_check 管理，比较增量采集与历史数据差异。"""
    record_id: str
    target_site: str
    page_url: str
    change_type: str
    old_summary: str = ""
    new_summary: str = ""
    affected_fields: list[str] = field(default_factory=list)
    detected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    status: str = "已检测"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> DiffRecord:
        return cls(
            record_id=str(row.get("记录编号", "")),
            target_site=str(row.get("目标站点", "")),
            page_url=str(row.get("检测页面", "")),
            change_type=str(row.get("变更类型", "")),
            old_summary=str(row.get("旧内容摘要", "")),
            new_summary=str(row.get("新内容摘要", "")),
            affected_fields=[str(row.get("影响字段", ""))] if row.get("影响字段") else [],
            detected_at=str(row.get("检测时间", "")),
            status=str(row.get("status", "已检测")),
        )


@dataclass
class CostRecord:
    """采集计费记录：crawl_cost 管理，统计代理和请求的成本。"""
    record_id: str
    site: str
    billing_cycle: str
    request_count: int = 0
    data_volume_mb: float = 0.0
    proxy_usage_hours: float = 0.0
    estimated_cost: float = 0.0
    status: str = "统计中"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> CostRecord:
        return cls(
            record_id=str(row.get("记录编号", "")),
            site=str(row.get("采集站点", "")),
            billing_cycle=str(row.get("计费周期", "")),
            request_count=int(row.get("请求次数", 0)),
            data_volume_mb=float(row.get("数据流量", 0)),
            proxy_usage_hours=float(row.get("代理用量", 0)),
            estimated_cost=float(row.get("费用预估", 0)),
            status=str(row.get("status", "统计中")),
        )


@dataclass
class ArchivePolicy:
    """数据归档策略：data_archive 管理，定期归档批次存储数据。"""
    policy_id: str
    data_scope: str
    archive_cycle: str
    archive_format: ArchiveFormat
    retention_days: int = 90
    storage_location: str = ""
    compression_method: str = "gzip"
    last_archive: str = ""
    status: str = "待触发"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> ArchivePolicy:
        af_str = str(row.get("归档格式", ArchiveFormat.GZIP.value))
        try:
            af = ArchiveFormat(af_str)
        except ValueError:
            af = ArchiveFormat.GZIP
        try:
            rd = int(row.get("保留时长", 90))
        except (ValueError, TypeError):
            rd = 90
        return cls(
            policy_id=str(row.get("策略编号", "")),
            data_scope=str(row.get("数据范围", "")),
            archive_cycle=str(row.get("归档周期", "monthly")),
            archive_format=af,
            retention_days=rd,
            storage_location=str(row.get("存储位置", "")),
            compression_method=str(row.get("压缩方式", "gzip")),
            last_archive=str(row.get("最后归档", "")),
            status=str(row.get("status", "待触发")),
        )


@dataclass
class SchedulePlan:
    """调度计划：scheduler 管理，编排 crawl_job 的定时执行。"""
    plan_id: str
    linked_job_id: str
    cron_expr: str
    timezone_setting: str = "Asia/Shanghai"
    dependency_check: str = ""
    failure_notify: str = ""
    concurrency_control: str = ""
    next_run: str = ""
    status: str = "待激活"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> SchedulePlan:
        return cls(
            plan_id=str(row.get("计划编号", "")),
            linked_job_id=str(row.get("关联作业", "")),
            cron_expr=str(row.get("Cron表达式", "")),
            timezone_setting=str(row.get("时区设置", "Asia/Shanghai")),
            dependency_check=str(row.get("依赖检查", "")),
            failure_notify=str(row.get("失败通知", "")),
            concurrency_control=str(row.get("并发控制", "")),
            next_run=str(row.get("下次执行", "")),
            status=str(row.get("status", "待激活")),
        )