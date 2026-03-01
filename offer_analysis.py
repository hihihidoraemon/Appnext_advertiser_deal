# -*- coding: utf-8 -*-
"""
Excel 三表分析：过去30天总收入 + 事件数据 + 流量黑名单
依赖: pip install pandas openpyxl
"""
import os
import re
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

# 以数据内最大日期为「昨日」，过去30天据此计算；若未加载数据则用运行日
REFERENCE_TODAY = None
YESTERDAY = None
# 事件率（step4/step5）按「前一天」计算
DAY_BEFORE = None

# 为 True 或环境变量 DEBUG_REJECT_RATE=1 时，在 step4 中打印 advertiser reject rate 各步中间数量
DEBUG_REJECT_RATE = os.environ.get("DEBUG_REJECT_RATE", "").strip().lower() in ("1", "true", "yes")


def _set_reference_dates_from_data(df: pd.DataFrame) -> None:
    """根据 Sheet1 的 Time 列设置 昨日 = 数据最大日期；前一天 = 昨日减一天，用于事件率。"""
    global REFERENCE_TODAY, YESTERDAY, DAY_BEFORE
    if df is None or df.empty or "Time" not in df.columns:
        REFERENCE_TODAY = datetime.now().date()
        YESTERDAY = REFERENCE_TODAY - timedelta(days=1)
        DAY_BEFORE = YESTERDAY - timedelta(days=1)  # 无数据时前一天=昨日-1
        return
    t = pd.to_datetime(df["Time"], errors="coerce").dt.date
    YESTERDAY = t.max()
    REFERENCE_TODAY = YESTERDAY + timedelta(days=1)
    DAY_BEFORE = YESTERDAY - timedelta(days=1)


def load_excel(path: str):
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"文件不存在: {path}")
    df1 = pd.read_excel(path, sheet_name="1-过去30天总收入")
    df2 = pd.read_excel(path, sheet_name="2--事件数据")
    df3 = pd.read_excel(path, sheet_name="3--流量黑名单")
    # 兼容实际列名：Adv Offer ID -> Adv Offer Id
    if "Adv Offer ID" in df1.columns and "Adv Offer Id" not in df1.columns:
        df1 = df1.rename(columns={"Adv Offer ID": "Adv Offer Id"})
    return df1, df2, df3


def extract_payin_number(ser: pd.Series) -> pd.Series:
    def _extract(x):
        if pd.isna(x):
            return np.nan
        m = re.search(r"[\d.]+", str(x).strip())
        return float(m.group()) if m else np.nan
    return ser.map(_extract)


def extract_offer_id_num(ser: pd.Series) -> pd.Series:
    def _extract(x):
        if pd.isna(x):
            return np.nan
        m = re.search(r"\[(\d+)\]", str(x))
        return int(m.group(1)) if m else np.nan
    return ser.map(_extract)


def _norm_advertiser_for_rate(x) -> str:
    """归一化 Advertiser 用于事件率分组：去掉 [xxx] 前缀并大写，使 'Appnext' 与 '[110001]APPNEXT' 归为同一组。"""
    if pd.isna(x) or x is None:
        return ""
    s = str(x).strip()
    if s.lower() == "nan":
        return ""
    m = re.match(r"\[\d+\]\s*(.+)", s)
    s = m.group(1).strip() if m else s
    return s.upper() if s else ""


# ========== 1. Sheet1 按 8 维度去重 + 去除 Payin<=0.12 ==========
# 得到「筛选保留的数据」：仅作为维度清单，后续用该清单匹配回原始表做聚合
def step1_dedupe_filter(df1: pd.DataFrame) -> pd.DataFrame:
    key_cols = ["Adv Offer Id", "Offer ID", "App ID", "Advertiser", "GEO", "Payin", "Total Caps", "Status"]
    key_cols = [c for c in key_cols if c in df1.columns]
    if not key_cols:
        raise ValueError("Sheet1 缺少关键列")
    df = df1.drop_duplicates(subset=key_cols).copy()
    if "Payin" not in df.columns:
        return df
    payin_num = extract_payin_number(df["Payin"])
    return df.loc[payin_num > 0.12].copy()


# ========== 2. 用筛选保留的维度匹配【1-过去30天总收入】再按时间聚合 ==========
def step2_aggregate(df1_raw: pd.DataFrame, offer_keys: pd.DataFrame) -> pd.DataFrame:
    key_cols = ["Adv Offer Id", "Offer ID", "App ID", "Advertiser", "GEO", "Payin", "Total Caps", "Status"]
    key_cols = [c for c in key_cols if c in offer_keys.columns]
    key_cols_raw = [c for c in key_cols if c in df1_raw.columns]
    if "Time" not in df1_raw.columns:
        raise ValueError("Sheet1 需要 Time 列")
    # 用筛选保留的维度匹配原始表，只对匹配到的行做聚合
    df_eligible = df1_raw.merge(offer_keys[key_cols], on=key_cols_raw, how="inner")
    df_eligible["_date"] = pd.to_datetime(df_eligible["Time"], errors="coerce").dt.date

    past30_end = YESTERDAY
    past30_start = past30_end - timedelta(days=29)
    df_30 = df_eligible[df_eligible["_date"].between(past30_start, past30_end)]
    df_yesterday = df_eligible[df_eligible["_date"] == YESTERDAY]
    df_day_before = df_eligible[df_eligible["_date"] == DAY_BEFORE] if DAY_BEFORE is not None else df_eligible.iloc[0:0]
    last_day = df_30["_date"].max() if not df_30.empty else None
    df_last_day = df_eligible[df_eligible["_date"] == last_day] if last_day is not None else df_eligible.iloc[0:0]

    def safe_div(a, b):
        return np.where(np.asarray(b) != 0, np.asarray(a) / np.asarray(b), np.nan)

    g30 = df_30.groupby(key_cols, dropna=False).agg(
        Total_Clicks_30=("Total Clicks", "sum"),
        Total_Conversions_30=("Total Conversions", "sum"),
        Total_Revenue_30=("Total Revenue", "sum"),
        Total_Cost_30=("Total Cost", "sum"),
        Total_Profit_30=("Total Profit", "sum"),
    ).reset_index()
    g30["Total_CR_30"] = safe_div(g30["Total_Conversions_30"], g30["Total_Clicks_30"])

    g_yesterday = df_yesterday.groupby(key_cols, dropna=False).agg(
        Total_Conversions_yesterday=("Total Conversions", "sum"),
        Total_Clicks_yesterday=("Total Clicks", "sum"),
        Total_Revenue_yesterday=("Total Revenue", "sum"),
        Total_Cost_yesterday=("Total Cost", "sum"),
        Total_Profit_yesterday=("Total Profit", "sum"),
    ).reset_index()
    g_yesterday["Total_CR_yesterday"] = safe_div(
        g_yesterday["Total_Conversions_yesterday"], g_yesterday["Total_Clicks_yesterday"]
    )

    g_day_before = df_day_before.groupby(key_cols, dropna=False).agg(
        Total_Conversions_day_before=("Total Conversions", "sum"),
        Total_Clicks_day_before=("Total Clicks", "sum"),
        Total_Revenue_day_before=("Total Revenue", "sum"),
        Total_Cost_day_before=("Total Cost", "sum"),
        Total_Profit_day_before=("Total Profit", "sum"),
    ).reset_index()
    g_day_before["Total_CR_day_before"] = safe_div(
        g_day_before["Total_Conversions_day_before"], g_day_before["Total_Clicks_day_before"]
    )

    g_last = df_last_day.groupby(key_cols, dropna=False).agg(
        Total_Clicks_last=("Total Clicks", "sum"),
        Total_Conversions_last=("Total Conversions", "sum"),
        Total_Revenue_last=("Total Revenue", "sum"),
        Total_Cost_last=("Total Cost", "sum"),
        Total_Profit_last=("Total Profit", "sum"),
    ).reset_index()
    g_last["Total_CR_last"] = safe_div(g_last["Total_Conversions_last"], g_last["Total_Clicks_last"])

    base = g30.merge(g_yesterday, on=key_cols, how="left").merge(g_day_before, on=key_cols, how="left").merge(g_last, on=key_cols, how="left")
    base["昨日剩余预算"] = base["Total Caps"] - base["Total_Conversions_yesterday"].fillna(0)
    base["Offer ID num"] = extract_offer_id_num(base["Offer ID"])
    # Sheet1 的 Offer ID 可能为纯数字(如 140513)，无 [xxx]；用 Offer ID 数值回填，便于与事件表匹配
    try:
        numeric_oid = pd.to_numeric(base["Offer ID"], errors="coerce")
        base["Offer ID num"] = base["Offer ID num"].fillna(numeric_oid)
    except Exception:
        pass
    base = base.sort_values("Total_Revenue_30", ascending=False).reset_index(drop=True)

    # Affiliate 汇总（若存在 Affiliate 列）
    if "Affiliate" in df_eligible.columns:
        for label, d in [("30天", df_30), ("最近一天", df_last_day)]:
            aff_g = d.groupby(key_cols + ["Affiliate"], dropna=False).agg(
                clicks=("Total Clicks", "sum"),
                conv=("Total Conversions", "sum"),
                rev=("Total Revenue", "sum"),
                cost=("Total Cost", "sum"),
                profit=("Total Profit", "sum"),
            ).reset_index()
            aff_g["cr"] = safe_div(aff_g["conv"], aff_g["clicks"])

            if label == "30天":
                def fmt_aff_30(g):
                    g = g.sort_values("rev", ascending=False)
                    lines = [f"{r['Affiliate']}：【最近30天总点击】{r['clicks']:.0f}、【最近30天总转化】{r['conv']:.0f}、【最近30天总CR】{r['cr']:.2%}、【最近30天总流水】{r['rev']:.2f}、【最近30天下游获得的总流水】{r['cost']:.2f}、【最近30天总利润】{r['profit']:.2f}" for _, r in g.iterrows()]
                    return "\n".join(lines) if lines else ""
                fmt_aff = fmt_aff_30
            else:
                def fmt_aff_last(g):
                    g = g.sort_values("rev", ascending=False)
                    lines = [f"{r['Affiliate']}：【昨天总点击】{r['clicks']:.0f}、【昨天总转化】{r['conv']:.0f}、【昨天总CR】{r['cr']:.2%}、【昨天总流水】{r['rev']:.2f}、【昨天下游获得的总流水】{r['cost']:.2f}、【昨天总利润】{r['profit']:.2f}" for _, r in g.iterrows()]
                    return "\n".join(lines) if lines else ""
                fmt_aff = fmt_aff_last

            col = "过去30天每个Affiliate汇总" if label == "30天" else "最近一天每个Affiliate汇总"
            def _match_row(aff_g, r, keys):
                cond = pd.Series(True, index=aff_g.index)
                for c in keys:
                    if c not in aff_g.columns or c not in r.index:
                        continue
                    cond = cond & (aff_g[c].astype(str) == str(r[c]))
                return aff_g.loc[cond]
            base[col] = base.apply(
                lambda r: fmt_aff(_match_row(aff_g, r, key_cols)),
                axis=1,
            )
    else:
        base["过去30天每个Affiliate汇总"] = ""
        base["最近一天每个Affiliate汇总"] = ""

    return base


# ========== 3. 事件表：reject 的 Time 减一天，提取 Offer ID num ==========
def step3_events(df2: pd.DataFrame) -> pd.DataFrame:
    df = df2.copy()
    # 统一解析为 datetime 再取日期，兼容 2026/2/27 23:54:25 等带时分秒格式，便于与「昨日」匹配
    dt = pd.to_datetime(df["Time"], errors="coerce").dt.normalize()
    df["_date"] = dt.dt.date
    if "Event" in df.columns:
        reject = df["Event"].astype(str).str.strip().str.lower() == "reject"
        # reject 的 Time 减一天（datetime 减 timedelta 再取 date）
        df.loc[reject, "_date"] = (dt.loc[reject] - timedelta(days=1)).dt.date
    # 事件表可能用 Offer Name 或 Offer 作为列名，优先 Offer Name，否则用 Offer
    offer_name_col = "Offer Name" if "Offer Name" in df.columns else ("Offer" if "Offer" in df.columns else None)
    df["Offer ID num"] = extract_offer_id_num(df[offer_name_col]) if offer_name_col else np.nan
    # 若事件表有 Offer ID 列，用其数值填充未解析出的 Offer ID num（便于与 base 的 Offer ID 一致）
    if "Offer ID" in df.columns:
        try:
            numeric_oid = pd.to_numeric(df["Offer ID"], errors="coerce")
            df["Offer ID num"] = df["Offer ID num"].fillna(numeric_oid)
        except Exception:
            pass
    # 统一为数值类型，避免 groupby 时 500619 与 500619.0 分成两档
    df["Offer ID num"] = pd.to_numeric(df["Offer ID num"], errors="coerce")
    return df


def _build_reject_detail_export(ev_reject_adv: pd.DataFrame) -> pd.DataFrame:
    """从 ev_reject_adv 构建可导出的 reject 明细表，便于核对少了哪些记录。"""
    cols = []
    renames = {}
    if "_date" in ev_reject_adv.columns:
        cols.append("_date")
        renames["_date"] = "日期"
    if "Time" in ev_reject_adv.columns:
        cols.append("Time")
        renames["Time"] = "Time"
    if "Offer Name" in ev_reject_adv.columns:
        cols.append("Offer Name")
        renames["Offer Name"] = "Offer Name"
    if "Offer ID num" in ev_reject_adv.columns:
        cols.append("Offer ID num")
        renames["Offer ID num"] = "Offer ID num"
    if "Event" in ev_reject_adv.columns:
        cols.append("Event")
        renames["Event"] = "Event"
    if "Advertiser" in ev_reject_adv.columns:
        cols.append("Advertiser")
        renames["Advertiser"] = "事件表Advertiser"
    if "Advertiser_y" in ev_reject_adv.columns:
        cols.append("Advertiser_y")
        renames["Advertiser_y"] = "匹配到的Advertiser"
    if "_adv_norm" in ev_reject_adv.columns:
        cols.append("_adv_norm")
        renames["_adv_norm"] = "归一化Advertiser"
    if "Affiliate" in ev_reject_adv.columns:
        cols.append("Affiliate")
    use = [c for c in cols if c in ev_reject_adv.columns]
    out = ev_reject_adv[use].copy()
    out["是否匹配到base"] = ev_reject_adv["Advertiser_y"].notna() if "Advertiser_y" in ev_reject_adv.columns else False
    out = out.rename(columns=renames)
    return out


# ========== 4. 事件率：advertiser / offer / affiliate reject rate（按「前一天」数据计算）==========
def step4_reject_rates(base: pd.DataFrame, ev: pd.DataFrame, df1_raw: pd.DataFrame = None):
    df = base.copy()

    # --- 步骤 A：筛出「前一天」事件，再筛出 reject ---
    ev_y = ev[ev["_date"].notna() & (ev["_date"].astype(str) == str(DAY_BEFORE))]
    ev_reject = ev_y[ev_y["Event"].astype(str).str.strip().str.lower() == "reject"]

    # --- 步骤 B：advertiser reject rate 分步逻辑（看 Advertiser 整体，不剔除 Payin≤0.12）---
    # B1. 用「原始 sheet1」的 (Offer ID num -> Advertiser) 映射，给每条 reject 打上 Advertiser，便于看 Advertiser 整体数据
    if (
        df1_raw is not None
        and not df1_raw.empty
        and "Offer ID" in df1_raw.columns
        and "Advertiser" in df1_raw.columns
    ):
        _raw = df1_raw.copy()
        _raw["Offer ID num"] = extract_offer_id_num(_raw["Offer ID"])
        try:
            _raw["Offer ID num"] = _raw["Offer ID num"].fillna(pd.to_numeric(_raw["Offer ID"], errors="coerce"))
        except Exception:
            pass
        oid_to_adv = _raw.drop_duplicates("Offer ID num")[["Offer ID num", "Advertiser"]].dropna(subset=["Offer ID num"]).copy()
    else:
        oid_to_adv = df.drop_duplicates("Offer ID num")[["Offer ID num", "Advertiser"]].dropna(subset=["Offer ID num"]).copy()
    oid_to_adv["Offer ID num"] = pd.to_numeric(oid_to_adv["Offer ID num"], errors="coerce")
    ev_reject_m = ev_reject.copy()
    ev_reject_m["Offer ID num"] = pd.to_numeric(ev_reject_m["Offer ID num"], errors="coerce")
    ev_reject_adv = ev_reject_m.merge(oid_to_adv, on="Offer ID num", how="left")
    # B2. 若事件表自带 Advertiser 且 merge 未匹配上，用事件表的 Advertiser 兜底
    if "Advertiser" in ev_reject_adv.columns and "Advertiser_y" in ev_reject_adv.columns:
        ev_reject_adv["_adv"] = ev_reject_adv["Advertiser_y"].fillna(ev_reject_adv["Advertiser"])
    elif "Advertiser_y" in ev_reject_adv.columns:
        ev_reject_adv["_adv"] = ev_reject_adv["Advertiser_y"]
    elif "Advertiser" in ev_reject_adv.columns:
        ev_reject_adv["_adv"] = ev_reject_adv["Advertiser"]
    else:
        ev_reject_adv["_adv"] = np.nan
    # B2b. 归一化 Advertiser：使 "Appnext" 与 "[110001]APPNEXT" 归为同一组，避免漏计 reject
    ev_reject_adv["_adv_norm"] = ev_reject_adv["_adv"].apply(
        lambda x: _norm_advertiser_for_rate(x) if pd.notna(x) and str(x).strip() else ""
    )
    # 兜底：merge 未匹配且事件表 Advertiser 含 appnext 的，归入 APPNEXT（避免漏计）
    if "Advertiser" in ev_reject_adv.columns:
        mask = (ev_reject_adv["_adv_norm"] == "") & (
            ev_reject_adv["Advertiser"].astype(str).str.upper().str.contains("APPNEXT", na=False)
        )
        ev_reject_adv.loc[mask, "_adv_norm"] = "APPNEXT"
    df["_adv_norm"] = df["Advertiser"].apply(
        lambda x: _norm_advertiser_for_rate(x) if pd.notna(x) and str(x).strip() else ""
    )
    # B4. 前一天的 Total Conversions 用于事件率分母：优先用原始 sheet1 当日按 Advertiser 汇总（与 48/2427 口径一致）
    if (
        df1_raw is not None
        and not df1_raw.empty
        and "Time" in df1_raw.columns
        and "Advertiser" in df1_raw.columns
        and "Total Conversions" in df1_raw.columns
        and DAY_BEFORE is not None
    ):
        _raw = df1_raw.copy()
        _raw["_date"] = pd.to_datetime(_raw["Time"], errors="coerce").dt.date
        _day = _raw[_raw["_date"] == DAY_BEFORE].copy()
        _day["_adv_norm"] = _day["Advertiser"].apply(
            lambda x: _norm_advertiser_for_rate(x) if pd.notna(x) and str(x).strip() else ""
        )
        conv_by_adv = _day[_day["_adv_norm"].str.len() > 0].groupby("_adv_norm", dropna=False)["Total Conversions"].sum()
    else:
        conv_by_adv = df[df["_adv_norm"].str.len() > 0].groupby("_adv_norm", dropna=False)["Total_Conversions_day_before"].sum()
    # B3. 按归一化后的 Advertiser 汇总前一天的 reject 条数（空串不参与）
    # 兜底：未归因的 reject（_adv_norm 为空）归入当日转化最大的 Advertiser，使 48/（48+2427）口径一致
    unassigned = ev_reject_adv[ev_reject_adv["_adv_norm"].str.len() == 0]
    # 先做出「reject 明细」供导出（归因前状态，便于核对少了哪些）
    ev_reject_export = _build_reject_detail_export(ev_reject_adv)
    if len(unassigned) > 0 and conv_by_adv is not None and len(conv_by_adv) > 0:
        main_adv = conv_by_adv.idxmax()
        ev_reject_adv.loc[ev_reject_adv["_adv_norm"] == "", "_adv_norm"] = main_adv
    reject_by_adv = ev_reject_adv[ev_reject_adv["_adv_norm"].str.len() > 0].groupby("_adv_norm", dropna=False).size()
    # B5. 合并所有出现过的 Advertiser，算 rate = reject_num / (reject_num + Total_Conversions)
    #     无 reject 时显示 0%，仅当 denominator=0 时为 NaN
    all_adv = conv_by_adv.index.union(reject_by_adv.index).unique()
    r = reject_by_adv.reindex(all_adv, fill_value=0)
    c = conv_by_adv.reindex(all_adv, fill_value=0)
    total = r + c
    adv_rate = pd.Series(np.where(total > 0, r / total, np.nan), index=all_adv)
    df["advertiser reject num"] = df["_adv_norm"].map(reject_by_adv).fillna(0).astype(int).values
    df["advertiser reject rate"] = df["_adv_norm"].map(adv_rate).values
    df.drop(columns=["_adv_norm"], inplace=True)

    if DEBUG_REJECT_RATE:
        print("[advertiser reject rate] 前一天:", DAY_BEFORE, "| 前一天事件行数:", len(ev_y), "| 前一天reject行数:", len(ev_reject),
              "| oid_to_adv行数:", len(oid_to_adv), "| reject_by_adv:", reject_by_adv.to_dict(),
              "| conv_by_adv:", conv_by_adv.to_dict(), "| adv_rate非空数:", pd.Series(adv_rate).notna().sum())

    # Offer: 按 Offer ID num 统计 reject，匹配 base；分母用前一天的转化
    reject_by_offer = ev_reject.groupby("Offer ID num", dropna=False).size().reset_index(name="reject_num")
    df = df.merge(reject_by_offer, on="Offer ID num", how="left")
    df["reject_num"] = df["reject_num"].fillna(0)
    df["offer reject num"] = df["reject_num"].astype(int)
    conv_y = df["Total_Conversions_day_before"].fillna(0)
    df["offer reject rate"] = np.where(
        (conv_y + df["reject_num"]) > 0,
        df["reject_num"] / (conv_y + df["reject_num"]),
        np.nan,
    )

    # Affiliate reject rate：按 (Offer ID num, Affiliate) 统计 reject，分母用该 Affiliate 在该 offer 前一天的转化
    # 正确公式：rate = reject_num / (reject_num + 该 Affiliate 昨日转化)，如 4/(4+37)=0.0975
    if "Affiliate" in ev_reject.columns:
        rej_aff = ev_reject.groupby(["Offer ID num", "Affiliate"], dropna=False).size().reset_index(name="rej")
        # 从原始 sheet1 取「前一天」按 (Offer ID num, Affiliate) 汇总的转化，用作分母
        conv_by_offer_aff = {}
        if (
            df1_raw is not None
            and not df1_raw.empty
            and "Affiliate" in df1_raw.columns
            and "Offer ID" in df1_raw.columns
            and "Total Conversions" in df1_raw.columns
            and "Time" in df1_raw.columns
            and DAY_BEFORE is not None
        ):
            _raw = df1_raw.copy()
            _raw["_date"] = pd.to_datetime(_raw["Time"], errors="coerce").dt.date
            _raw["Offer ID num"] = extract_offer_id_num(_raw["Offer ID"])
            try:
                _raw["Offer ID num"] = _raw["Offer ID num"].fillna(pd.to_numeric(_raw["Offer ID"], errors="coerce"))
            except Exception:
                pass
            _day = _raw[_raw["_date"] == DAY_BEFORE]
            for (oid, aff), conv in _day.groupby(["Offer ID num", "Affiliate"], dropna=False)["Total Conversions"].sum().items():
                if pd.notna(oid):
                    conv_by_offer_aff[(float(oid), aff)] = conv

        def aff_rate_row(r):
            oid_num = pd.to_numeric(r["Offer ID num"], errors="coerce")
            if pd.isna(oid_num):
                oid_num = float("nan")
            sub = rej_aff[pd.to_numeric(rej_aff["Offer ID num"], errors="coerce") == oid_num]
            offer_conv = r["Total_Conversions_day_before"] or 0  # 兜底：无按 Affiliate 转化时用 offer 总转化
            parts = []
            for _, s in sub.iterrows():
                aff, rej = s["Affiliate"], s["rej"]
                conv = (conv_by_offer_aff.get((float(oid_num), aff), 0) or 0) if pd.notna(oid_num) and conv_by_offer_aff else offer_conv
                if (conv + rej) > 0:
                    rate = (rej / (conv + rej))
                    parts.append(f"{aff}：【前一天reject num】{int(rej)}、【前一天reject rate】{rate:.2%}")
            return "\n".join(parts) if parts else ""
        df["Affiliate reject rate"] = df.apply(aff_rate_row, axis=1)
    else:
        df["Affiliate reject rate"] = ""

    return df, ev_reject_export


# ========== 5. offer event rate / Affiliate event rate（按「昨日」数据计算）==========
# offer event rate = event_num / offer 昨日总转化；Affiliate event rate = 该 Affiliate 在该 offer 的 event / 该 Affiliate 在该 offer 的昨日转化
def step5_event_rates(base: pd.DataFrame, ev: pd.DataFrame, df1_raw: pd.DataFrame = None) -> pd.DataFrame:
    df = base.copy()
    ev_y = ev[ev["_date"].notna() & (ev["_date"].astype(str) == str(YESTERDAY))]
    ev_ok = ev_y[ev_y["Event"].astype(str).str.strip().str.lower() != "reject"]

    # 昨日转化按 Offer ID num 汇总（offer 维度，用于 offer event rate）
    conv_offer_ser = df.groupby("Offer ID num", dropna=False)["Total_Conversions_yesterday"].sum()
    conv_offer = {float(k): v for k, v in conv_offer_ser.items() if pd.notna(k)}

    # 昨日转化按 (Offer ID num, Affiliate) 汇总（用于 Affiliate event rate 分母）
    conv_by_offer_aff = {}
    if (
        df1_raw is not None
        and not df1_raw.empty
        and "Affiliate" in df1_raw.columns
        and "Offer ID" in df1_raw.columns
        and "Total Conversions" in df1_raw.columns
        and "Time" in df1_raw.columns
        and YESTERDAY is not None
    ):
        _raw = df1_raw.copy()
        _raw["_date"] = pd.to_datetime(_raw["Time"], errors="coerce").dt.date
        _raw["Offer ID num"] = extract_offer_id_num(_raw["Offer ID"])
        try:
            _raw["Offer ID num"] = _raw["Offer ID num"].fillna(pd.to_numeric(_raw["Offer ID"], errors="coerce"))
        except Exception:
            pass
        _day = _raw[_raw["_date"] == YESTERDAY]
        for (oid, aff), conv in _day.groupby(["Offer ID num", "Affiliate"], dropna=False)["Total Conversions"].sum().items():
            if pd.notna(oid):
                conv_by_offer_aff[(float(oid), aff)] = conv

    # Offer 维度：event rate = event_num / offer 昨日总转化
    ev_offer = ev_ok.groupby(["Offer ID num", "Event"], dropna=False).size().reset_index(name="event_num")
    ev_offer["_oid"] = pd.to_numeric(ev_offer["Offer ID num"], errors="coerce")

    def offer_ev_row(r):
        oid_num = pd.to_numeric(r["Offer ID num"], errors="coerce")
        if pd.isna(oid_num):
            return ""
        conv = conv_offer.get(float(oid_num), 0) or 0
        sub = ev_offer[ev_offer["_oid"] == oid_num]
        if sub.empty or conv <= 0:
            return ""
        lines = [f"{s['Event']}:【最近一天event num】{int(s['event_num'])}，【最近一天event rate】{(s['event_num']/conv):.2%}" for _, s in sub.iterrows()]
        return "\n".join(lines)
    df["offer event rate"] = df.apply(offer_ev_row, axis=1)

    # Affiliate + Event：event rate = 该 Affiliate 在该 offer 的 event 数 / 该 Affiliate 在该 offer 的昨日转化
    if "Affiliate" in ev_ok.columns:
        ev_aff = ev_ok.groupby(["Offer ID num", "Event", "Affiliate"], dropna=False).size().reset_index(name="event_num")
        ev_aff["_oid"] = pd.to_numeric(ev_aff["Offer ID num"], errors="coerce")
        offer_conv_ser = df.groupby("Offer ID num", dropna=False)["Total_Conversions_yesterday"].sum()

        def aff_ev_row(r):
            oid_num = pd.to_numeric(r["Offer ID num"], errors="coerce")
            if pd.isna(oid_num):
                return ""
            sub = ev_aff[ev_aff["_oid"] == oid_num]
            if sub.empty:
                return ""
            offer_conv = offer_conv_ser.get(oid_num, 0) or 0  # 兜底：无按 Affiliate 转化时用 offer 总转化
            by_aff = sub.groupby("Affiliate")
            lines = []
            for aff, g in by_aff:
                parts = []
                for _, row in g.iterrows():
                    e, n = row["Event"], row["event_num"]
                    conv = (conv_by_offer_aff.get((float(oid_num), aff), 0) or 0) if conv_by_offer_aff else offer_conv
                    if conv > 0:
                        parts.append(f"{e}:【最近一天event num】{int(n)}，【最近一天event rate】{(n/conv):.2%}")
                if parts:
                    lines.append(f"{aff}:{'｜'.join(parts)}")
            return "\n".join(lines) if lines else ""
        df["Affiliate event rate"] = df.apply(aff_ev_row, axis=1)
    else:
        df["Affiliate event rate"] = ""

    return df


# ========== 6. 待办事项与优先级（仅 Status='Active' 参与规则；优先级按中国工作日） ==========
def step6_todo(base: pd.DataFrame) -> pd.DataFrame:
    df = base.copy()
    o_rej = df["offer reject rate"].fillna(0)
    a_rej = df["advertiser reject rate"].fillna(0)
    conv_y = df["Total_Conversions_yesterday"].fillna(0)
    remain = df["昨日剩余预算"].fillna(0)
    ev_rate_str = df["offer event rate"].fillna("").astype(str)
    # Status='Active' 才参与规则
    if "Status" in df.columns:
        status_active = df["Status"].astype(str).str.strip().str.lower() == "active"
    else:
        status_active = pd.Series(True, index=df.index)

    def is_ev_zero(s):
        return s in ("", "nan", "None") or (isinstance(s, float) and np.isnan(s))

    # 规则1、2 每天计算；按周维度：仅中国工作日计算规则3～6
    # 本周中国工作日第1、3天（周一、周三）→ 规则3、4、5；第2、4天（周二、周四）→ 规则6；周末只算规则1、2
    w = REFERENCE_TODAY.weekday()  # 0=周一 .. 4=周五, 5=周六, 6=周日
    is_workday = w <= 4
    day1_3 = is_workday and w in (0, 2)   # 第1、3个工作日：周一、周三，计算规则3、4、5
    day2_4 = is_workday and w in (1, 3)   # 第2、4个工作日：周二、周四，计算规则6

    todo_list = []
    prio_list = []
    for i in range(len(df)):
        o_r = o_rej.iloc[i]
        a_r = a_rej.iloc[i]
        cy = conv_y.iloc[i]
        rm = remain.iloc[i]
        ev_z = is_ev_zero(ev_rate_str.iloc[i])
        active = status_active.iloc[i]

        t, p = "", ""
        if active and (o_r - a_r) >= 0.05:
            t, p = "该预算reject比例过高，请先优化预算", "今日第一优先级"
        elif active and 0 < rm <= 10:
            t, p = "预算剩余不多，请先push产生流水下游跑满", "今日第一优先级"
        elif is_workday and active and cy >= 30 and ev_z:
            t = "该预算暂时没有产生事件，请新增其它流量测试"
            p = "今日第二优先级" if day1_3 else ""
        elif is_workday and active and cy >= 30 and not ev_z:
            t = "该预算有事件产生，按照最近一天Affiliate event rate这列推有事件的Affiliate跑满预算"
            p = "今日第二优先级" if day1_3 else ""
        elif is_workday and active and cy < 30:
            t = "请优先push已产生流水下游跑满"
            p = "今日第二优先级" if day1_3 else ""
        elif is_workday and active and rm > 10:
            t = "新增为产生流水下游测试"
            p = "今日第二优先级" if day2_4 else ""

        todo_list.append(t)
        prio_list.append(p)
    df["今日待办事项"] = todo_list
    df["今日待办优先级"] = prio_list
    return df


# ========== 7. 流量黑名单 ==========
def step7_blacklist(base: pd.DataFrame, df3: pd.DataFrame) -> pd.DataFrame:
    df = base.copy()
    if df3.empty or "Offer ID" not in df3.columns:
        df["该预算的流量黑名单"] = ""
        return df
    bl = df3.groupby("Offer ID").apply(
        lambda g: "\n".join(g["Affiliate"].astype(str).dropna().unique()) if "Affiliate" in g.columns else ""
    ).to_dict()
    df["该预算的流量黑名单"] = df["Offer ID"].map(lambda x: bl.get(x, ""))
    return df


# ========== 8. 分天分维度 reject/event rate 表（日期取两表交集）==========
def _dates_intersection(df1: pd.DataFrame, ev: pd.DataFrame):
    """【1-过去30天总收入】与【2--事件数据】的日期取交集。"""
    d1 = set(pd.to_datetime(df1["Time"], errors="coerce").dt.date.dropna())
    d2 = set(pd.to_datetime(ev["_date"].dropna(), errors="coerce").dt.date.dropna())
    return sorted(d1 & d2)


def build_sheet2_advertiser_reject_rate(df1: pd.DataFrame, ev: pd.DataFrame) -> pd.DataFrame:
    """分天分 Advertiser 的 reject rate，sheet 名：2--分Advertiser reject rate。"""
    s1 = df1.copy()
    s1["_date"] = pd.to_datetime(s1["Time"], errors="coerce").dt.date
    s1["Offer ID num"] = extract_offer_id_num(s1["Offer ID"]) if "Offer ID" in s1.columns else np.nan
    s1["Offer ID num"] = s1["Offer ID num"].fillna(pd.to_numeric(s1["Offer ID"], errors="coerce"))
    dates = _dates_intersection(df1, ev)
    if not dates or "Advertiser" not in s1.columns or "Event" not in ev.columns:
        return pd.DataFrame(columns=["Time", "Advertiser", "total conversations", "reject num", "reject rate"])
    oid_to_adv = s1.drop_duplicates("Offer ID num")[["Offer ID num", "Advertiser"]].dropna(subset=["Offer ID num"])
    ev_reject = ev[ev["Event"].astype(str).str.strip().str.lower() == "reject"].copy()
    ev_reject["Offer ID num"] = pd.to_numeric(ev_reject["Offer ID num"], errors="coerce")
    ev_reject = ev_reject.merge(oid_to_adv, on="Offer ID num", how="left")
    adv_col = "Advertiser_y" if "Advertiser_y" in ev_reject.columns else "Advertiser"
    if adv_col not in ev_reject.columns:
        ev_reject["_adv"] = np.nan
    else:
        ev_reject["_adv"] = ev_reject[adv_col]
    ev_reject = ev_reject[ev_reject["_date"].astype(str).isin([str(d) for d in dates])]
    rej_by = ev_reject.groupby(["_date", "_adv"], dropna=False).size().reset_index(name="reject_num")
    rej_by = rej_by.rename(columns={"_adv": "Advertiser"})
    conv_by = s1[s1["_date"].isin(dates)].groupby(["_date", "Advertiser"], dropna=False)["Total Conversions"].sum().reset_index(name="conversions")
    merged = conv_by.merge(rej_by, on=["_date", "Advertiser"], how="outer").fillna(0)
    merged["reject rate"] = np.where(
        merged["reject_num"] + merged["conversions"] > 0,
        merged["reject_num"] / (merged["reject_num"] + merged["conversions"]),
        np.nan,
    )
    merged = merged.rename(columns={"_date": "Time"})
    merged["total conversations"] = merged["conversions"].astype(int)
    merged["reject num"] = merged["reject_num"].astype(int)
    return merged[["Time", "Advertiser", "total conversations", "reject num", "reject rate"]]


def build_sheet3_offer_reject_rate(df1: pd.DataFrame, ev: pd.DataFrame) -> pd.DataFrame:
    """分天分 offer 的 reject rate，再分 affiliate 合并到单元格；匹配 sheet1 维度。列：Time, Adv Offer Id, App ID, Advertiser, GEO, Payin, Total Caps, Status, reject rate, 所有affiliate的reject rate。"""
    s1 = df1.copy()
    s1["_date"] = pd.to_datetime(s1["Time"], errors="coerce").dt.date
    s1["Offer ID num"] = extract_offer_id_num(s1["Offer ID"]) if "Offer ID" in s1.columns else np.nan
    s1["Offer ID num"] = s1["Offer ID num"].fillna(pd.to_numeric(s1["Offer ID"], errors="coerce"))
    dates = _dates_intersection(df1, ev)
    if not dates:
        return pd.DataFrame()
    ev_reject = ev[ev["Event"].astype(str).str.strip().str.lower() == "reject"].copy()
    ev_reject["Offer ID num"] = pd.to_numeric(ev_reject["Offer ID num"], errors="coerce")
    ev_reject = ev_reject[ev_reject["_date"].astype(str).isin([str(d) for d in dates])]
    # 分天分 offer：reject 数、转化数
    rej_offer = ev_reject.groupby(["_date", "Offer ID num"], dropna=False).size().reset_index(name="reject_num")
    conv_offer = s1[s1["_date"].isin(dates)].groupby(["_date", "Offer ID num"], dropna=False)["Total Conversions"].sum().reset_index(name="conversions")
    offer_rates = conv_offer.merge(rej_offer, on=["_date", "Offer ID num"], how="left").fillna(0)
    offer_rates["reject rate"] = np.where(
        offer_rates["reject_num"] + offer_rates["conversions"] > 0,
        offer_rates["reject_num"] / (offer_rates["reject_num"] + offer_rates["conversions"]),
        np.nan,
    )
    # 分天分 offer 分 affiliate
    if "Affiliate" in ev_reject.columns:
        rej_aff = ev_reject.groupby(["_date", "Offer ID num", "Affiliate"], dropna=False).size().reset_index(name="rej")
        conv_aff = s1[s1["_date"].isin(dates)]
        if "Affiliate" in conv_aff.columns:
            conv_aff = conv_aff.groupby(["_date", "Offer ID num", "Affiliate"], dropna=False)["Total Conversions"].sum().reset_index(name="conv")
            aff_merged = rej_aff.merge(conv_aff, on=["_date", "Offer ID num", "Affiliate"], how="left").fillna(0)
            aff_merged["aff_rate"] = np.where(aff_merged["rej"] + aff_merged["conv"] > 0, aff_merged["rej"] / (aff_merged["rej"] + aff_merged["conv"]), np.nan)
            all_aff = aff_merged.groupby(["_date", "Offer ID num"]).apply(
                lambda g: "\n".join([f"{r['Affiliate']}:【reject num】{int(r['rej'])}，【reject rate】{r['aff_rate']:.2%}" for _, r in g.iterrows() if pd.notna(r["aff_rate"])])
            ).reset_index(name="所有affiliate的reject rate")
        else:
            all_aff = offer_rates[["_date", "Offer ID num"]].copy()
            all_aff["所有affiliate的reject rate"] = ""
    else:
        all_aff = offer_rates[["_date", "Offer ID num"]].copy()
        all_aff["所有affiliate的reject rate"] = ""
    offer_rates = offer_rates.merge(all_aff, on=["_date", "Offer ID num"], how="left")
    # 匹配 sheet1 维度：取每个 (Offer ID num) 对应的一条 sheet1 行（含 Adv Offer Id, Offer ID, App ID, Advertiser, GEO, Payin, Total Caps, Status）
    dim_cols = ["Adv Offer Id", "App ID", "Advertiser", "GEO", "Payin", "Total Caps", "Status"]
    dim_cols = [c for c in dim_cols if c in s1.columns]
    dim_cols_for_merge = (["Offer ID"] if "Offer ID" in s1.columns else []) + dim_cols
    offer_dims = s1.drop_duplicates("Offer ID num", keep="first")[["Offer ID num"] + dim_cols_for_merge].dropna(subset=["Offer ID num"])
    out = offer_rates.merge(offer_dims, on="Offer ID num", how="left")
    out = out.rename(columns={"_date": "Time"})
    # 该 Offer 当天的 reject num、total conversions
    out["reject num"] = out["reject_num"].fillna(0).astype(int)
    out["total conversions"] = out["conversions"].fillna(0).astype(int)
    # Offer ID 放在 Adv Offer Id 后面；reject num、total conversions 放在 reject rate 前
    col_order = ["Time", "Adv Offer Id"] + (["Offer ID"] if "Offer ID" in out.columns else []) + [c for c in dim_cols if c != "Adv Offer Id"] + ["reject num", "total conversions", "reject rate", "所有affiliate的reject rate"]
    return out[[c for c in col_order if c in out.columns]]


def build_sheet4_offer_event_rate(df1: pd.DataFrame, ev: pd.DataFrame) -> pd.DataFrame:
    """分天分 offer 分 event 的 event rate 合并到单元格；分 affiliate 分 event 合并到单元格。列：Time, Adv Offer Id, App ID, Advertiser, GEO, Payin, Total Caps, Status, event rate, 所有affiliate的event rate。"""
    s1 = df1.copy()
    s1["_date"] = pd.to_datetime(s1["Time"], errors="coerce").dt.date
    s1["Offer ID num"] = extract_offer_id_num(s1["Offer ID"]) if "Offer ID" in s1.columns else np.nan
    s1["Offer ID num"] = s1["Offer ID num"].fillna(pd.to_numeric(s1["Offer ID"], errors="coerce"))
    dates = _dates_intersection(df1, ev)
    if not dates:
        return pd.DataFrame()
    ev_ok = ev[ev["Event"].astype(str).str.strip().str.lower() != "reject"].copy()
    ev_ok["Offer ID num"] = pd.to_numeric(ev_ok["Offer ID num"], errors="coerce")
    ev_ok = ev_ok[ev_ok["_date"].astype(str).isin([str(d) for d in dates])]
    conv_offer = s1[s1["_date"].isin(dates)].groupby(["_date", "Offer ID num"], dropna=False)["Total Conversions"].sum().reset_index(name="conversions")
    # 分天分 offer 分 event
    ev_ev = ev_ok.groupby(["_date", "Offer ID num", "Event"], dropna=False).size().reset_index(name="event_num")
    ev_ev = ev_ev.merge(conv_offer, on=["_date", "Offer ID num"], how="left").fillna(0)
    ev_ev["rate"] = np.where(ev_ev["conversions"] > 0, ev_ev["event_num"] / ev_ev["conversions"], np.nan)
    event_rate_cell = ev_ev.groupby(["_date", "Offer ID num"]).apply(
        lambda g: "\n".join([f"{r['Event']}:【event num】{int(r['event_num'])}，【event rate】{r['rate']:.2%}" for _, r in g.iterrows() if pd.notna(r["rate"])])
    ).reset_index(name="event rate")
    # 分天分 offer 分 affiliate 分 event
    if "Affiliate" in ev_ok.columns and "Affiliate" in s1.columns:
        conv_aff = s1[s1["_date"].isin(dates)].groupby(["_date", "Offer ID num", "Affiliate"], dropna=False)["Total Conversions"].sum().reset_index(name="conv")
        ev_aff = ev_ok.groupby(["_date", "Offer ID num", "Event", "Affiliate"], dropna=False).size().reset_index(name="n")
        ev_aff = ev_aff.merge(conv_aff, on=["_date", "Offer ID num", "Affiliate"], how="left").fillna(0)
        ev_aff["rate"] = np.where(ev_aff["conv"] > 0, ev_aff["n"] / ev_aff["conv"], np.nan)
        def _aff_cell(g):
            lines = []
            for aff in g["Affiliate"].unique():
                sub = g[g["Affiliate"] == aff]
                conv_aff_val = int(sub["conv"].iloc[0]) if len(sub) else 0
                parts = [f"{r['Event']}【event num】{int(r['n'])}，【event rate】{r['rate']:.2%}" for _, r in sub.iterrows() if pd.notna(r["rate"])]
                if parts:
                    lines.append(f"{aff}:【Total converstaions】{conv_aff_val}，" + "｜".join(parts))
            return "\n".join(lines)
        all_aff_ev = ev_aff.groupby(["_date", "Offer ID num"]).apply(_aff_cell).reset_index(name="所有affiliate的event rate")
    else:
        all_aff_ev = event_rate_cell[["_date", "Offer ID num"]].copy()
        all_aff_ev["所有affiliate的event rate"] = ""
    out = event_rate_cell.merge(all_aff_ev, on=["_date", "Offer ID num"], how="left")
    # 该 offer 当天的 total conversions，放在 event rate 前
    out = out.merge(conv_offer.rename(columns={"conversions": "total conversions"}), on=["_date", "Offer ID num"], how="left")
    out["total conversions"] = out["total conversions"].fillna(0).astype(int)
    dim_cols = ["Adv Offer Id", "App ID", "Advertiser", "GEO", "Payin", "Total Caps", "Status"]
    dim_cols = [c for c in dim_cols if c in s1.columns]
    dim_cols_for_merge = (["Offer ID"] if "Offer ID" in s1.columns else []) + dim_cols
    offer_dims = s1.drop_duplicates("Offer ID num", keep="first")[["Offer ID num"] + dim_cols_for_merge].dropna(subset=["Offer ID num"])
    out = out.merge(offer_dims, on="Offer ID num", how="left")
    out = out.rename(columns={"_date": "Time"})
    # Offer ID 放在 Adv Offer Id 后面；total conversions 放在 event rate 前
    col_order = ["Time", "Adv Offer Id"] + (["Offer ID"] if "Offer ID" in out.columns else []) + [c for c in dim_cols if c != "Adv Offer Id"] + ["total conversions", "event rate", "所有affiliate的event rate"]
    return out[[c for c in col_order if c in out.columns]]


# ========== 9. 最终列顺序与输出 ==========
FINAL_COLS = [
    "Adv Offer Id", "Offer ID", "App ID", "Advertiser", "GEO", "Payin", "Total Caps", "Status",
    "近30天收入排序",
    "Total_Clicks_30", "Total_Conversions_30", "Total_CR_30", "Total_Revenue_30", "Total_Cost_30", "Total_Profit_30",
    "过去30天每个Affiliate汇总",
    "Total_Clicks_last", "Total_Conversions_last", "Total_CR_last", "Total_Revenue_last", "Total_Cost_last", "Total_Profit_last",
    "最近一天每个Affiliate汇总",
    "昨日剩余预算",
    "advertiser reject num", "advertiser reject rate", "offer reject num", "offer reject rate", "Affiliate reject rate", "offer event rate", "Affiliate event rate",
    "今日待办事项", "今日待办优先级", "该预算的流量黑名单",
]

OUTPUT_RENAME = {
    "Total_Clicks_30": "最近30天总点击",
    "Total_Conversions_30": "最近30天总转化",
    "Total_CR_30": "最近30天总CR",
    "Total_Revenue_30": "最近30天总流水",
    "Total_Cost_30": "30天下游获得的总流水",
    "Total_Profit_30": "最近30天总利润",
    "Total_Clicks_last": "昨天总点击",
    "Total_Conversions_last": "昨天总转化",
    "Total_CR_last": "昨天总CR",
    "Total_Revenue_last": "昨天总流水",
    "Total_Cost_last": "昨天下游获得的总流水",
    "Total_Profit_last": "昨天总利润",
    "过去30天每个Affiliate汇总": "过去30天每个Affiliate流水汇总",
    "最近一天每个Affiliate汇总": "昨天每个Affiliate流水数据汇总",
    "昨日剩余预算": "最近一天剩余预算",
    "advertiser reject num": "前一天广告主总reject num",
    "advertiser reject rate": "前一天广告主总reject rate",
    "offer reject num": "前一天offer reject num",
    "offer reject rate": "前一天offer reject rate",
    "Affiliate reject rate": "前一天Affiliate reject rate",
    "offer event rate": "最近一天offer event rate",
    "Affiliate event rate": "最近一天Affiliate event rate",
    "该预算的流量黑名单": "流量黑名单",
}


def run(excel_path: str, output_path: str = None) -> pd.DataFrame:
    df1, df2, df3 = load_excel(excel_path)
    clean = step1_dedupe_filter(df1)
    _set_reference_dates_from_data(df1)
    base = step2_aggregate(df1, clean)
    ev = step3_events(df2)
    base, _ = step4_reject_rates(base, ev, df1_raw=df1)
    base = step5_event_rates(base, ev, df1_raw=df1)
    base = step6_todo(base)
    base = step7_blacklist(base, df3)
    out_cols = [c for c in FINAL_COLS if c in base.columns]
    result = base[out_cols].copy()
    # 近30天 Total Revenue 降序排名（1=最高），放在 Status 后
    result["近30天收入排序"] = result["Total_Revenue_30"].rank(ascending=False, method="first").astype(int)
    # 列顺序：把 近30天收入排序 放在 Status 后（FINAL_COLS 已含此项，但 result 可能缺列则需重排）
    order = [c for c in FINAL_COLS if c in result.columns]
    result = result[order]
    result = result.sort_values(by=["Status", "Total_Revenue_30"], ascending=[True, False]).reset_index(drop=True)
    result = result.rename(columns=OUTPUT_RENAME)
    if output_path:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            result.to_excel(writer, index=False, sheet_name="1-近30天收入计算概况")
            build_sheet2_advertiser_reject_rate(df1, ev).to_excel(writer, index=False, sheet_name="2--分Advertiser reject rate")
            build_sheet3_offer_reject_rate(df1, ev).to_excel(writer, index=False, sheet_name="3--分offerid reject rate")
            build_sheet4_offer_event_rate(df1, ev).to_excel(writer, index=False, sheet_name="4--分offerid event rate")
        print(f"已输出: {output_path}（1-近30天收入计算概况 + 2--分Advertiser reject rate + 3--分offerid reject rate + 4--分offerid event rate）")
    return result


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "你的文件.xlsx"
    out = sys.argv[2] if len(sys.argv) > 2 else "offer_analysis_result.xlsx"
    run(path, out)
