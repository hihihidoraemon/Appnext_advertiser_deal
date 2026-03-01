# -*- coding: utf-8 -*-
"""
Streamlit 网页应用：模板下载 / 上传计算数据 / 下载计算结果
部署到 share.streamlit.io 使用，不修改原 offer_analysis.py（原代码用于本地调试）
"""
import tempfile
from pathlib import Path

import streamlit as st

# 模板直接从 GitHub 拉取：填写 raw 地址，例如：
# https://raw.githubusercontent.com/你的用户名/你的仓库/分支/template.xlsx
GITHUB_TEMPLATE_URL = "https://raw.githubusercontent.com/hihihidoraemon/Appnext_advertiser_deal/main/20260228--apn%E8%AE%A1%E7%AE%97%E6%95%B0%E6%8D%AE.xlsx"


def fetch_template_from_github() -> tuple[bytes | None, str | None]:
    """
    从 GITHUB_TEMPLATE_URL 拉取模板文件。
    返回 (bytes, None) 成功；(None, 错误信息) 失败。
    """
    url = (GITHUB_TEMPLATE_URL or "").strip()
    if not url:
        return None, "请在 streamlit_app.py 中配置 GITHUB_TEMPLATE_URL（模板的 GitHub raw 链接）"
    try:
        import urllib.request
        req = urllib.request.Request(url, headers={"User-Agent": "Streamlit"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read(), None
    except Exception as e:
        return None, f"从 GitHub 拉取模板失败: {e}"


def run_analysis(input_path: str, output_path: str) -> None:
    """调用原 offer_analysis 的 run，写入 output_path。"""
    from offer_analysis import run
    run(input_path, output_path)


st.set_page_config(page_title="Offer 收入计算", layout="centered")

st.title("Offer 收入计算")
st.caption("上传包含三张 sheet 的 Excel，计算近 30 天收入概况与 reject/event 率，并下载结果。")

# ---------- 1. 下载模板（直接从 GitHub 拉取） ----------
st.header("1. 下载模板")
template_bytes, err = fetch_template_from_github()
if err:
    st.error(err)
    st.caption("模板需从 GitHub 获取，请在代码中配置 GITHUB_TEMPLATE_URL 后重新部署。")
else:
    st.caption("模板来源: GitHub")
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
