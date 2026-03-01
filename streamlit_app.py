# -*- coding: utf-8 -*-
"""
Streamlit 网页应用：模板下载 / 上传计算数据 / 下载计算结果
部署到 share.streamlit.io 使用，不修改原 offer_analysis.py（原代码用于本地调试）
"""
import io
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

# 模板可来源于 GitHub：在此填写 raw 地址，例如：
# https://raw.githubusercontent.com/你的用户名/你的仓库/分支/template.xlsx
# 留空则使用程序生成的最小模板（仅含表头）
GITHUB_TEMPLATE_URL = ""

# 表头定义（与 offer_analysis 所需一致）
SHEET1_NAME = "1-过去30天总收入"
SHEET1_COLUMNS = [
    "Time", "Adv Offer Id", "Offer ID", "App ID", "Advertiser", "GEO",
    "Payin", "Total Caps", "Status", "Affiliate",
    "Total Clicks", "Total Conversions", "Total Revenue", "Total Cost", "Total Profit",
]
SHEET2_NAME = "2--事件数据"
SHEET2_COLUMNS = ["Time", "Event", "Offer Name", "Offer ID", "Advertiser", "Affiliate"]
SHEET3_NAME = "3--流量黑名单"
SHEET3_COLUMNS = ["Offer ID", "Affiliate"]


def build_minimal_template() -> bytes:
    """生成最小模板 Excel（仅含表头），用于下载。"""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        pd.DataFrame(columns=SHEET1_COLUMNS).to_excel(writer, index=False, sheet_name=SHEET1_NAME)
        pd.DataFrame(columns=SHEET2_COLUMNS).to_excel(writer, index=False, sheet_name=SHEET2_NAME)
        pd.DataFrame(columns=SHEET3_COLUMNS).to_excel(writer, index=False, sheet_name=SHEET3_NAME)
    buf.seek(0)
    return buf.getvalue()


def get_template_bytes() -> tuple[bytes, str]:
    """
    获取模板文件字节。优先：1) GITHUB_TEMPLATE_URL 2) 仓库内 template.xlsx 3) 程序生成表头模板。
    返回 (bytes, 说明)。
    """
    if GITHUB_TEMPLATE_URL and GITHUB_TEMPLATE_URL.strip():
        try:
            import urllib.request
            req = urllib.request.Request(GITHUB_TEMPLATE_URL, headers={"User-Agent": "Streamlit"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = resp.read()
            return data, "来自 GitHub 的模板"
        except Exception as e:
            st.warning(f"从 GitHub 拉取模板失败: {e}，改用本地或内置模板。")
    # 同仓库下的 template.xlsx（部署后即来自 GitHub）
    template_path = Path(__file__).resolve().parent / "template.xlsx"
    if template_path.exists():
        return template_path.read_bytes(), "仓库内 template.xlsx"
    return build_minimal_template(), "内置表头模板"


def run_analysis(input_path: str, output_path: str) -> None:
    """调用原 offer_analysis 的 run，写入 output_path。"""
    from offer_analysis import run
    run(input_path, output_path)


st.set_page_config(page_title="Offer 收入计算", layout="centered")

st.title("Offer 收入计算")
st.caption("上传包含三张 sheet 的 Excel，计算近 30 天收入概况与 reject/event 率，并下载结果。")

# ---------- 1. 下载模板 ----------
st.header("1. 下载模板")
template_bytes, template_source = get_template_bytes()
st.caption(f"当前模板来源: {template_source}")
st.download_button(
    label="下载 Excel 模板",
    data=template_bytes,
    file_name="offer_计算模板.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    key="download_template",
)

st.markdown("---")

# ---------- 2. 上传计算数据 ----------
st.header("2. 上传计算数据")
uploaded = st.file_uploader(
    "上传 Excel 文件（需包含 sheet：1-过去30天总收入、2--事件数据、3--流量黑名单）",
    type=["xlsx", "xls"],
    key="upload_data",
)

if not uploaded:
    st.info("请先上传 Excel 文件后再进行计算。")
    st.stop()

# ---------- 3. 计算并下载结果 ----------
st.header("3. 计算并下载结果")

if "result_bytes" not in st.session_state:
    st.session_state.result_bytes = None

if st.button("开始计算", type="primary", key="run_calc"):
    with st.spinner("计算中，请稍候…"):
        try:
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f_in:
                f_in.write(uploaded.getvalue())
                input_path = f_in.name
            output_path = tempfile.mktemp(suffix="_结果.xlsx")
            try:
                run_analysis(input_path, output_path)
                with open(output_path, "rb") as f_out:
                    st.session_state.result_bytes = f_out.read()
                Path(output_path).unlink(missing_ok=True)
                Path(input_path).unlink(missing_ok=True)
            except FileNotFoundError:
                Path(input_path).unlink(missing_ok=True)
                raise
            except Exception:
                Path(input_path).unlink(missing_ok=True)
                if Path(output_path).exists():
                    Path(output_path).unlink(missing_ok=True)
                raise
        except FileNotFoundError as e:
            st.error(f"文件错误: {e}")
        except Exception as e:
            st.error(f"计算失败: {e}")
            st.exception(e)

if st.session_state.result_bytes is not None:
    st.success("计算完成，请下载结果文件。")
    st.download_button(
        label="下载计算结果",
        data=st.session_state.result_bytes,
        file_name="offer_计算结果.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="download_result",
    )
