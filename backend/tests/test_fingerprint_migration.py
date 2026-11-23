"""指纹 v1→v2 迁移测试：兼容回执、混合部署选址、幂等确认、回滚重放。

场景：构建一组含功能参数、跟踪参数和缺原始网址的旧记录，分别用准备
就绪和未就绪的来源启动新旧消费者，核对两个版本的入口地址和采集作业
结果；数据环境回滚后重放任一已确认申请，不增加重复执行。
"""
from __future__ import annotations

import pytest

from app.integration import (
    REASON_CONVERTED, REASON_MISSING_URL, REASON_SOURCE_NOT_READY,
    confirm_execution, migrate_fingerprint_registry, resolve_entry_address,
    rollback_fingerprint_registry, select_executable_addresses,
    sync_conversion_to_job_list,
)
from app.models import (
    FingerprintRegistry, SiteParamPolicy, UrlFingerprint,
)
from app.store import store

# ---------------------------------------------------------------------------
# 场景数据：同一站点、两种来源（就绪 / 未就绪）、三类旧记录
# ---------------------------------------------------------------------------

SITE = "shop-mig.example.com"
READY_SRC = "crawl_job"        # 准备就绪：已登记 v2 指纹的来源
NOT_READY_SRC = "manual_import"  # 未就绪：仍只有旧字段的来源

POLICY = SiteParamPolicy(
    site_domain=SITE,
    functional_params=("id", "page"),
    tracking_params=("utm_source", "utm_medium", "gclid"),
)
POLICIES = {SITE: POLICY}
READY_SOURCES = {READY_SRC}

# 功能参数 + 跟踪参数：与 R2 仅跟踪参数不同，与 R3 功能参数值不同
URL_R1 = f"https://{SITE}/item?id=42&page=2&utm_source=email"
URL_R2 = f"https://{SITE}/item?utm_medium=social&id=42&page=2"
URL_R3 = f"https://{SITE}/item?id=43&page=2&utm_source=email"
# 缺原始网址的旧记录：只有存量旧摘要
R5_LEGACY_HASH = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"


def _legacy_row(url: str, source: str, dup: int = 0, hash_value: str = "") -> dict:
    """构造 url_dedup 旧记录（v1 时代字段）。"""
    legacy_hash = hash_value or (UrlFingerprint.compute_v1_hash(url) if url else "")
    return {
        "指纹编号": f"FP-{legacy_hash[:12]}",
        "源URL": url,
        "指纹算法": "sha256",
        "指纹哈希": legacy_hash,
        "发现渠道": source,
        "首次发现": "2026-10-01",
        "重复次数": dup,
        "status": "已记录",
    }


def _legacy_rows() -> list[dict]:
    return [
        _legacy_row(URL_R1, READY_SRC, dup=3),           # R1 就绪来源
        _legacy_row(URL_R2, READY_SRC, dup=1),           # R2 仅跟踪参数不同
        _legacy_row(URL_R3, READY_SRC, dup=0),           # R3 功能参数值不同
        _legacy_row(URL_R1, NOT_READY_SRC, dup=2),       # R4 同地址但未就绪来源
        _legacy_row("", NOT_READY_SRC, dup=7, hash_value=R5_LEGACY_HASH),  # R5 缺原始网址
    ]


@pytest.fixture()
def registry() -> FingerprintRegistry:
    """迁移完成的注册表：5 条旧记录 → 5 张兼容回执。"""
    reg = FingerprintRegistry()
    migrate_fingerprint_registry(_legacy_rows(), POLICIES, READY_SOURCES, reg)
    return reg


def _receipt(reg: FingerprintRegistry, source: str, url: str = "", hash_value: str = ""):
    legacy = hash_value or UrlFingerprint.compute_v1_hash(url)
    return reg.receipt_for(source, legacy)


# ---------------------------------------------------------------------------
# 迁移与兼容回执
# ---------------------------------------------------------------------------

def test_migration_issues_receipts_preserving_legacy_fields(registry) -> None:
    """每条旧记录签发一张回执，旧 URL、旧摘要和次数全部保留。"""
    assert len(registry.receipts) == 5

    r1 = _receipt(registry, READY_SRC, URL_R1)
    assert r1 is not None
    assert r1.legacy_url == URL_R1, "旧 URL 应保留"
    assert r1.legacy_hash == UrlFingerprint.compute_v1_hash(URL_R1), "旧摘要应保留"
    assert r1.duplicate_count == 3, "旧次数应保留"
    assert r1.rule_version == "v2"
    assert r1.conversion_reason == REASON_CONVERTED
    assert r1.status == "已确认"


def test_migration_idempotent_on_duplicate_import(registry) -> None:
    """重复导入同来源同旧键：不新增回执，次数取累计较大值。"""
    again = migrate_fingerprint_registry(
        [_legacy_row(URL_R1, READY_SRC, dup=5)], POLICIES, READY_SOURCES, registry)

    assert again == [], "重复导入不应签发新回执"
    assert len(registry.receipts) == 5
    assert _receipt(registry, READY_SRC, URL_R1).duplicate_count == 5


def test_v2_merges_tracking_only_variants(registry) -> None:
    """仅跟踪参数不同的地址在 v2 下等价，且 v2 键不同于旧摘要。"""
    r1 = _receipt(registry, READY_SRC, URL_R1)
    r2 = _receipt(registry, READY_SRC, URL_R2)

    assert r1.v2_hash == r2.v2_hash, "仅跟踪参数不同应归为同一 v2 指纹"
    assert r1.v2_hash != r1.legacy_hash, "剔除跟踪参数后新键应不同于旧键"


def test_v2_never_merges_distinct_functional_values(registry) -> None:
    """功能参数值不同的地址不得被新规则误合并。"""
    r1 = _receipt(registry, READY_SRC, URL_R1)
    r3 = _receipt(registry, READY_SRC, URL_R3)

    assert r1.v2_hash != r3.v2_hash, "id=42 与 id=43 不得合并"
    # 未登记语义的参数一律按功能参数保留
    h_a = UrlFingerprint.compute_v2_hash(f"https://{SITE}/item?id=42&skuid=1", POLICY)
    h_b = UrlFingerprint.compute_v2_hash(f"https://{SITE}/item?id=42&skuid=2", POLICY)
    assert h_a != h_b, "未登记参数值不同也不得合并"
    # 无站点策略时退化为 v1 通用排序
    assert (UrlFingerprint.compute_v2_hash(URL_R1, None)
            == UrlFingerprint.compute_v1_hash(URL_R1))


def test_unverifiable_source_keeps_identity_with_reason(registry) -> None:
    """未就绪来源：无法核实参数语义，恒等转换并保留转换原因。"""
    r4 = _receipt(registry, NOT_READY_SRC, URL_R1)

    assert r4 is not None
    assert r4.is_identity, "未就绪来源应按旧键恒等转换"
    assert r4.rule_version == "v1"
    assert r4.conversion_reason == REASON_SOURCE_NOT_READY
    assert r4.duplicate_count == 2


def test_missing_url_record_keeps_reason_and_counts(registry) -> None:
    """缺原始网址：无法重算，保留旧键、次数与转换原因。"""
    r5 = _receipt(registry, NOT_READY_SRC, hash_value=R5_LEGACY_HASH)

    assert r5 is not None
    assert r5.legacy_url == ""
    assert r5.is_identity
    assert r5.conversion_reason == REASON_MISSING_URL
    assert r5.duplicate_count == 7, "缺网址记录的次数也应保留"


# ---------------------------------------------------------------------------
# 混合部署下的新旧消费者选址
# ---------------------------------------------------------------------------

def _entries() -> list[tuple[str, str]]:
    return [
        (URL_R1, READY_SRC),
        (URL_R2, READY_SRC),
        (URL_R3, READY_SRC),
        (URL_R1, NOT_READY_SRC),  # 同一地址，未就绪来源
    ]


def test_new_consumer_selects_by_v2_equivalence(registry) -> None:
    """新消费者（v2）：合并跟踪变体，但未就绪来源的记录不与之合并。"""
    selected = select_executable_addresses(
        _entries(), registry, POLICIES, READY_SOURCES, consumer_version="v2")

    addresses = [s.executable_address for s in selected]
    assert addresses == [URL_R1, URL_R3, URL_R1], \
        "v2 应合并 R1+R2、保留 R3，未就绪来源的 R4 单独成组"
    by_key = {s.canonical_key: s for s in selected}
    assert len(by_key) == 3, "v2 等价范围：R1/R2 一组、R3 一组、R4 恒等一组"
    r1_resolved = selected[0]
    assert r1_resolved.rule_version == "v2", "已就绪记录应记录实际使用 v2"
    assert r1_resolved.receipt_id, "应记录命中的兼容回执"
    r4_resolved = selected[2]
    assert r4_resolved.rule_version == "v1", "未就绪来源的恒等回执实际按 v1 旧键"


def test_old_consumer_selects_by_v1_equivalence(registry) -> None:
    """旧消费者（v1）：凭兼容回执继续按旧摘要等价，同次上线期间不受影响。"""
    selected = select_executable_addresses(
        _entries(), registry, POLICIES, READY_SOURCES, consumer_version="v1")

    addresses = [s.executable_address for s in selected]
    assert addresses == [URL_R1, URL_R2, URL_R3], \
        "v1 不剔除跟踪参数：R1/R2/R3 各自成组，R4 与 R1 同旧键被合并"
    assert all(s.rule_version == "v1" for s in selected)


def test_two_versions_pick_different_addresses(registry) -> None:
    """两个版本业务等价范围不同，调度实际选的地址集合随之不同。"""
    v1 = select_executable_addresses(
        _entries(), registry, POLICIES, READY_SOURCES, consumer_version="v1")
    v2 = select_executable_addresses(
        _entries(), registry, POLICIES, READY_SOURCES, consumer_version="v2")

    v1_keys = {s.canonical_key for s in v1}
    v2_keys = {s.canonical_key for s in v2}
    assert v1_keys != v2_keys, "混合部署下两个版本的等价键集合应不同"
    # v1 把 R2 当独立地址，v2 把 R2 并入 R1；v1 把 R4 并入 R1，v2 让 R4 独立
    assert [s.input_url for s in v1] == [URL_R1, URL_R2, URL_R3]
    assert [s.input_url for s in v2] == [URL_R1, URL_R3, URL_R1]


def test_unregistered_url_resolved_by_source_readiness(registry) -> None:
    """未登记的地址即时归一化：就绪来源按 v2，未就绪来源按旧键。"""
    new_url = f"https://{SITE}/item?id=99&utm_source=app"
    ready = resolve_entry_address(
        new_url, READY_SRC, registry, POLICIES, READY_SOURCES, consumer_version="v2")
    not_ready = resolve_entry_address(
        new_url, NOT_READY_SRC, registry, POLICIES, READY_SOURCES, consumer_version="v2")

    assert ready is not None and ready.rule_version == "v2"
    assert not_ready is not None and not_ready.rule_version == "v1"
    assert ready.canonical_key != not_ready.canonical_key


# ---------------------------------------------------------------------------
# 转换结果同步采集任务清单 + 执行机会确认
# ---------------------------------------------------------------------------

def test_sync_conversion_to_job_list(registry) -> None:
    """采集任务清单回写可执行地址、实际规则版本和对应旧键。"""
    job_row = {"作业编号": "JOB-MIG-001", "目标站点": SITE,
               "入口URL": f"{URL_R1},{URL_R2},{URL_R3}", "status": "待执行"}

    sync_conversion_to_job_list(
        [job_row], registry, POLICIES, READY_SOURCES, consumer_version="v2")

    assert job_row["可执行地址"] == f"{URL_R1},{URL_R3}", "v2 下 R2 被合并，只剩两个可执行地址"
    assert job_row["规则版本"] == "v2"
    old_keys = job_row["指纹旧键"].split(",")
    assert UrlFingerprint.compute_v1_hash(URL_R1)[:12] in old_keys
    assert UrlFingerprint.compute_v1_hash(URL_R3)[:12] in old_keys
    assert UrlFingerprint.compute_v1_hash(URL_R2)[:12] not in old_keys, "被合并的地址不应留下旧键"

    # 旧消费者同步同一清单：三个地址都保留
    job_row_v1 = {"作业编号": "JOB-MIG-002", "目标站点": SITE,
                  "入口URL": f"{URL_R1},{URL_R2},{URL_R3}", "status": "待执行"}
    sync_conversion_to_job_list(
        [job_row_v1], registry, POLICIES, READY_SOURCES, consumer_version="v1")
    assert job_row_v1["可执行地址"] == f"{URL_R1},{URL_R2},{URL_R3}"
    assert job_row_v1["规则版本"] == "v1"


def test_sync_writes_back_to_store_job_list(registry) -> None:
    """同步结果落到 store 的采集任务清单行上。"""
    job_row = {"id": 900001, "作业编号": "JOB-MIG-STORE", "目标站点": SITE,
               "入口URL": f"{URL_R1},{URL_R2}", "status": "待执行",
               "pending": True, "abnormal": False}
    store.rows("crawl_job").append(job_row)

    sync_conversion_to_job_list(
        [job_row], registry, POLICIES, READY_SOURCES, consumer_version="v2")

    stored = [r for r in store.rows("crawl_job") if r.get("作业编号") == "JOB-MIG-STORE"][0]
    assert stored["规则版本"] == "v2"
    assert stored["可执行地址"] == URL_R1
    assert stored["指纹旧键"] == UrlFingerprint.compute_v1_hash(URL_R1)[:12]


def test_duplicate_import_confirms_single_execution(registry) -> None:
    """重复导入只确认一个相同来源的执行机会。"""
    selected = select_executable_addresses(
        _entries(), registry, POLICIES, READY_SOURCES, consumer_version="v2")
    for resolved in selected:
        confirm_execution(resolved, registry)
    assert registry.execution_count() == 3

    # 重复导入：同一来源同一旧键再次申请
    again = resolve_entry_address(
        URL_R1, READY_SRC, registry, POLICIES, READY_SOURCES, consumer_version="v2")
    conf = confirm_execution(again, registry)

    assert registry.execution_count() == 3, "重复导入不应新增执行机会"
    assert conf.replay_count == 1
    assert conf.rule_version == "v2", "凭证记录实际使用的规则版本"
    assert conf.legacy_hash == UrlFingerprint.compute_v1_hash(URL_R1), "凭证记录对应旧键"


# ---------------------------------------------------------------------------
# 回滚与重放
# ---------------------------------------------------------------------------

def test_rollback_replay_does_not_duplicate_execution(registry) -> None:
    """数据环境回滚后重放任一已确认申请，不增加重复执行。"""
    selected = select_executable_addresses(
        _entries(), registry, POLICIES, READY_SOURCES, consumer_version="v2")
    confirmed = [confirm_execution(s, registry) for s in selected]
    assert registry.execution_count() == 3

    report = rollback_fingerprint_registry(registry)
    assert report["active_rule_version"] == "v1"
    assert report["receipts_rolled_back"] == 5
    assert report["confirmations_kept"] == 3, "确认凭证应全部保留"
    assert all(r.status == "已回滚" for r in registry.receipts.values())

    # 回滚后重放任一已确认申请：命中先前确认凭证
    replayed = resolve_entry_address(
        URL_R1, READY_SRC, registry, POLICIES, READY_SOURCES)
    assert replayed.rule_version == "v1", "回滚后按旧键消费"
    conf = confirm_execution(replayed, registry)

    assert conf is confirmed[0], "应沿用先前确认凭证"
    assert conf.replay_count == 1
    assert registry.execution_count() == 3, "重放不应增加重复执行"


def test_rollback_restores_v1_selection(registry) -> None:
    """回滚后调度恢复旧版等价范围，与旧消费者结果一致。"""
    rollback_fingerprint_registry(registry)

    selected = select_executable_addresses(
        _entries(), registry, POLICIES, READY_SOURCES)

    assert [s.input_url for s in selected] == [URL_R1, URL_R2, URL_R3]
    assert all(s.rule_version == "v1" for s in selected)
