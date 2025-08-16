# app.py（两表版 + 连续事件条月历 + 颜色区分 + 按日期预估库存 + 手机友好）
# -*- coding: utf-8 -*-
import io, re, calendar, zipfile
from datetime import date, timedelta
from typing import Dict, Tuple, List

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="江门仓库 | 库存与到货日历）",
    layout="wide",
    initial_sidebar_state="collapsed",
)
st.title("江门仓库库存与到货信息")

# ---------------- 参数区 ----------------
DEFAULT_WAREHOUSE_KEYWORD = "江门"   # 只看江门仓相关记录（收货地址或仓库/客户中包含此关键词）
DEFAULT_DAYS_FORWARD = 30            # 日历窗口天数

# ---------------- 规范品名映射 ----------------
RULES = [
    # —— 白色 ——
    (r"白色粉末.*(T|甜).*", "白色粉末甜味优镁粉"),
    (r"白色粉末.*(白皮).*", "白色粉末（白皮）"),
    (r"白色粉末.*(优乐粉).*", "白色粉末优乐粉"),
    (r"白色粉末.*优镁粉.*", "白色粉末优镁粉"),
    (r"白色粉末.*", "白色粉末优镁粉"),
    # —— 金黄色 ——
    (r"金黄色粉末.*(新配方).*", "金黄色粉末优镁粉2号"),
    (r"金黄色粉末.*1号.*", "金黄色粉末优镁粉1号"),
    (r"金黄色粉末.*2号.*", "金黄色粉末优镁粉2号"),
    (r"金黄色粉末.*优镁粉.*", "金黄色粉末优镁粉1号"),
    (r"金黄色粉末.*", "金黄色粉末优镁粉1号"),
    # —— 深黄色 ——
    (r"深黄色粉末.*(T|甜).*", "深黄色粉末甜味优镁粉"),
    (r"深黄色粉末.*优镁粉.*", "深黄色粉末优镁粉"),
    (r"深黄色粉末.*", "深黄色粉末优镁粉"),
    # —— 浅黄色 ——
    (r"浅黄色粉末.*", "浅黄色粉末优镁粉"),
    # —— 棕色号 ——
    (r"棕色.*1号.*", "棕色1号优镁粉"),
    (r"棕色.*(0号|2号).*", "棕色2号优镁粉"),
]

PREFERRED_ORDER = [
    "白色粉末优镁粉","白色粉末甜味优镁粉","白色粉末（白皮）",
    "金黄色粉末优镁粉1号","金黄色粉末优镁粉2号",
    "深黄色粉末优镁粉","深黄色粉末甜味优镁粉",
    "浅黄色粉末优镁粉","棕色1号优镁粉","棕色2号优镁粉"
]

# 配色：区分不同货运公司（循环使用）
PALETTE = [
    "#FAD7A0", "#AED6F1", "#A9DFBF", "#F5B7B1", "#D7BDE2",
    "#F9E79F", "#85C1E9", "#ABEBC6", "#F8C471", "#F5CBA7",
]

# ---------------- 交互：手机模式 & 简短标签 ----------------
col0a, col0b = st.columns([1,1])
with col0a:
    is_mobile = st.checkbox("📱 手机模式", value=True)
with col0b:
    short_label = st.checkbox("精简条目文本（移动端推荐）", value=True if is_mobile else False)

# 一些通用的 UI 压缩样式
if is_mobile:
    MOBILE_TITLE_SIZE = "28px"
    st.markdown(
        f"""
           <style>
           .block-container {{ padding-top: 2.3rem; padding-bottom: 0.6rem; }}
           .stDataFrame {{ font-size: 10px; }}
           div[data-testid="stHorizontalBlock"] {{ overflow-x: auto; }}

           .block-container h1 {{
               font-size: {MOBILE_TITLE_SIZE};
               line-height: 1.28;
               margin: 10px 0 6px;
               font-weight: 700;
           }}
           </style>
           """,
        unsafe_allow_html=True,
    )


def normalize_product(name: str) -> str | None:
    """把各种写法归一到规范品名；匹配不到时返回 None。"""
    s = str(name).strip()
    # 把“（白皮）<任意吨位>(装xxx)”这类后缀剪掉避免干扰
    s = re.sub(r"(（?白皮）?)\s*\d+(\.\d+)?\s*吨.*", r"\1", s)
    for pattern, target in RULES:
        if re.search(pattern, s):
            return target
    return None


# ---------------- 工具函数 ----------------

def split_product_items(row: pd.Series) -> list:
    """把“产品”里的一柜多品拆成多行；抽取吨位，归一产品名，并保留关键字段。"""
    items: List[dict] = []
    prod_text = str(row.get("产品", ""))
    parts = re.split(r"[+,，]", prod_text)
    for p in parts:
        p = p.strip()
        m = re.search(r"([0-9]+(\.[0-9]+)?)\s*吨", p)
        if not m:
            continue
        qty = float(m.group(1))
        base = normalize_product(p)
        if base is None:
            continue  # 不是规范10类则跳过
        items.append({
            "箱号/封号": row.get("箱号/封号"),
            "原始品名": p,
            "产品": base,
            "数量(吨)": qty,
            "装货日期": row.get("装货日期"),
            "预计到港时间": row.get("预计到港时间"),
            "预计到货时间": row.get("预计到货时间"),
            "仓库/客户": row.get("仓库/客户") or row.get("仓库客服"),
            "货运公司": row.get("货运公司"),
        })
    return items


def to_excel_bytes(df: pd.DataFrame, sheet_name="Sheet1") -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name=sheet_name)
    bio.seek(0)
    return bio.getvalue()


# ---------------- 页面交互 ----------------
col1, col2, col3, col4 = st.columns([1,1,1,1])
with col1:
    days_forward = st.slider("日历窗口（天）", 7, 60, DEFAULT_DAYS_FORWARD, step=1)
with col2:
    wh_keyword = st.text_input("仓库筛选", value=DEFAULT_WAREHOUSE_KEYWORD)
with col3:
    today = st.date_input("统计起始日（含当日）", value=date.today())
with col4:
    cutoff = st.date_input("库存查看日期（截止）", value=today)

st.subheader("上传两张表")
stock_file = st.file_uploader("【库存盘点】Excel（列：产品, 江门实际库存数量, 记录库存数量, 备注）", type=["xlsx","xls","csv"])
track_file = st.file_uploader("【货柜跟踪明细】Excel（列：序号, 装货日期, 装货地址, 收货地址, 仓库/客户, 产品, 箱号/封号, 预计到港时间, 预计到货时间, 货运公司）", type=["xlsx","xls","csv"])

if not stock_file or not track_file:
    st.info("请上传两张源数据表后继续。")
    st.stop()


def read_any(file):
    name = file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(file)
    else:
        return pd.read_excel(file, sheet_name=0)


stock_df = read_any(stock_file)
track_df = read_any(track_file)

# 列校验
need_track_cols = {"序号","装货日期","装货地址","收货地址","仓库/客户","产品","箱号/封号","预计到港时间","预计到货时间","货运公司"}
miss = need_track_cols - set(track_df.columns)
if miss:
    st.error(f"【货柜跟踪明细】缺少列：{', '.join(miss)}")
    st.stop()
need_stock_cols = {"产品","江门实际库存数量","记录库存数量","备注"}
miss2 = need_stock_cols - set(stock_df.columns)
if miss2:
    st.error(f"【库存盘点】缺少列：{', '.join(miss2)}")
    st.stop()

# 只保留江门仓记录
mask = track_df["收货地址"].astype(str).str.contains(wh_keyword, na=False) | \
       track_df.get("仓库/客户", pd.Series("", index=track_df.index)).astype(str).str.contains(wh_keyword, na=False)
track_df = track_df[mask].copy()

# ---------- 归一化：库存表的“产品”也归一，并做合并汇总 ----------
stock_df["产品"] = stock_df["产品"].apply(lambda x: normalize_product(x) or str(x))
# 修正棕色0号 → 2号
stock_df["产品"] = stock_df["产品"].replace({"棕色0号优镁粉": "棕色2号优镁粉"})
# 仅保留规范类目
stock_df = stock_df[stock_df["产品"].isin(PREFERRED_ORDER)]
stock_df = (stock_df
    .groupby("产品", as_index=False)
    .agg({
        "江门实际库存数量": "sum",
        "记录库存数量": "sum",
        "备注": lambda s: "；".join([str(x) for x in s if pd.notna(x)])[:200]
    })
)

# 拆分产品（一个箱号→多行）
item_rows: List[dict] = []
for _, r in track_df.iterrows():
    item_rows.extend(split_product_items(r))
items_df = pd.DataFrame(item_rows)

# 规范日期列 —— 全部使用 Timestamp，避免与 date 比较报错
for col in ["装货日期","预计到港时间","预计到货时间"]:
    if col in items_df.columns:
        items_df[col] = pd.to_datetime(items_df[col], errors="coerce")

# 比较用的今天/截止日（Timestamp）
today_ts = pd.to_datetime(today)
cutoff_ts = pd.to_datetime(cutoff)

# 在途定义：预计到货为空 或 预计到货 >= 今天
intransit_df = items_df[(items_df["预计到货时间"].isna()) | (items_df["预计到货时间"] >= today_ts)].copy()

# ================== 汇总（产品） ==================
# 1) 为每条在途记录生成“装货日期YYYY-MM-DD（N天到）”标签，其中 N = 预计到港 - 装货

def label_for(row: pd.Series) -> str | None:
    ship = row.get("装货日期")
    eta_port = row.get("预计到港时间") or row.get("预计到货时间")
    if pd.isna(ship) or pd.isna(eta_port):
        return None
    days = (eta_port.normalize() - ship.normalize()).days
    return f"装货日期{ship.strftime('%Y-%m-%d')}（{days}天到）"

intransit_df["装货标签"] = intransit_df.apply(label_for, axis=1)

# 2) 生成“装货标签”宽表（每个标签一列，值为数量）
lab_tbl = (
    intransit_df.dropna(subset=["装货标签"]) \
        .groupby(["产品","装货标签"])['数量(吨)'] \
        .sum() \
        .unstack(fill_value=0.0)
)
# 标签列按日期排序（从小到大）
if not lab_tbl.empty:
    def _key(c):
        m = re.search(r"装货日期(\d{4}-\d{2}-\d{2})", str(c))
        return m.group(1) if m else "9999-99-99"
    lab_tbl = lab_tbl.reindex(sorted(lab_tbl.columns, key=_key), axis=1)

lab_tbl = lab_tbl.reset_index()

# 3) 在途总和（运送途中数量）
intransit_sum = intransit_df.groupby("产品")["数量(吨)"].sum().rename("运送途中数量").reset_index()

# 4) 截止某日预计入库：只把“在途且预计到货<=截止日”的数量算进来
eta_col = intransit_df["预计到货时间"].fillna(intransit_df["预计到港时间"])
arrive_by_cutoff = intransit_df[eta_col <= cutoff_ts].groupby("产品")["数量(吨)"].sum().rename("截止日预计到货").reset_index()

# 5) 合并库存 + 标签宽表 + 在途合计 + 截止预计入库，并计算“预计江门库存数量（截止X日）”
summary_df = stock_df.merge(lab_tbl, on="产品", how="outer")
summary_df = summary_df.merge(intransit_sum, on="产品", how="left")
summary_df = summary_df.merge(arrive_by_cutoff, on="产品", how="left")
summary_df[["运送途中数量","截止日预计到货"]] = summary_df[["运送途中数量","截止日预计到货"]].fillna(0.0)
summary_df[["江门实际库存数量","记录库存数量","备注"]] = summary_df[["江门实际库存数量","记录库存数量","备注"]].fillna({"江门实际库存数量":0.0,"记录库存数量":0.0,"备注":""})
summary_df[f"预计江门库存数量（截止{cutoff_ts.strftime('%Y-%m-%d')}）"] = summary_df["江门实际库存数量"] + summary_df["截止日预计到货"]

# 6) 排序：先按指定产品顺序，再按装货日期列顺序
order_map = {name:i for i,name in enumerate(PREFERRED_ORDER)}
summary_df["__ord__"] = summary_df["产品"].map(order_map).fillna(9999)
summary_df = summary_df[summary_df["产品"].isin(PREFERRED_ORDER)]
summary_df = summary_df.sort_values(["__ord__","产品"]).drop(columns="__ord__")

# 7) 列顺序：产品 | 实际库存 | 各装货标签... | 运送途中数量 | 截止预计到货 | 预计库存(截止) | 记录库存 | 备注
label_cols = [c for c in summary_df.columns if str(c).startswith("装货日期")]  # 已按日期排序
final_cols = [
    "产品","江门实际库存数量", *label_cols, "运送途中数量","截止日预计到货",
    f"预计江门库存数量（截止{cutoff_ts.strftime('%Y-%m-%d')}）","记录库存数量","备注"
]
summary_df = summary_df.reindex(columns=final_cols)

# —— 手机端只展示核心列（其余下载查看）
if is_mobile:
    core_cols = [
        "产品","江门实际库存数量","运送途中数量",
        f"预计江门库存数量（截止{cutoff_ts.strftime('%Y-%m-%d')}）",
    ]
    # 最多展示前两列“装货日期”标签，避免太挤
    extra_label_cols = label_cols[:2]
    mobile_cols = [c for c in core_cols if c in summary_df.columns] + extra_label_cols
    show_df = summary_df.reindex(columns=mobile_cols)
else:
    show_df = summary_df

st.subheader("产品库存信息汇总")
st.dataframe(show_df, use_container_width=True)

# 汇总合计（可选）：
with st.expander("查看总量合计"):
    total_row = summary_df.drop(columns=[c for c in summary_df.columns if c.startswith("装货日期") or c in ["产品","备注"]]).sum(numeric_only=True)
    st.write(total_row.to_frame(name="合计").T)

st.download_button(
    "下载：汇总（产品）.xlsx",
    to_excel_bytes(summary_df, "汇总(产品)"),
    file_name=f"汇总(产品)_含装货标签_在途_预计库存_截止{cutoff_ts.strftime('%Y%m%d')}.xlsx"
)

# ================== 连续事件条：月历（按周分段，移动端友好） ==================
# 为不同货运公司准备颜色映射
carriers = [c for c in intransit_df["货运公司"].dropna().unique().tolist()]
color_map: Dict[str,str] = {c: PALETTE[i % len(PALETTE)] for i, c in enumerate(sorted(carriers))}

def make_events_for_product(prod: str, window_start: date, days: int) -> List[dict]:
    window_end = window_start + timedelta(days=days-1)
    df = intransit_df[intransit_df["产品"] == prod].copy()
    events: List[dict] = []
    for _, r in df.iterrows():
        s_ts = r.get("装货日期")
        e_ts = r.get("预计到货时间") or r.get("预计到港时间")
        if pd.isna(s_ts):
            continue
        if pd.isna(e_ts):
            e_ts = s_ts + pd.Timedelta(days=7)
        start_full = s_ts.normalize().date()
        end_full = e_ts.normalize().date()
        # 用于渲染的窗口裁剪
        s = max(start_full, window_start)
        e = min(end_full, window_end)
        if s > e:
            continue
        days_total = (end_full - start_full).days + 1
        events.append({
            "start": s, "end": e,
            "datestr": f"{start_full:%Y-%m-%d} ~ {end_full:%Y-%m-%d}",
            "days": days_total,
            "qty": r.get("数量(吨)") or 0,
            "carrier": r.get("货运公司") or "",
            "warehouse": r.get("仓库/客户") or "",
            "container": r.get("箱号/封号") or ""
        })
    return events


def build_calendar_html_events_grid(product: str, window_start: date, days: int, events: List[dict], *, compact: bool=False, short_label: bool=False) -> str:
    window_end = window_start + timedelta(days=days-1)
    css = f"""
    <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial; margin:16px; }}
    .month {{ margin-bottom: 24px; }}
    .month-title {{ font-size: {16 if compact else 18}px; font-weight: 600; margin: 8px 0; }}
    .week {{ border: 1px solid #eee; padding: 6px; border-radius: 8px; margin-bottom: 10px; }}
    .grid {{ display: grid; grid-template-columns: repeat(7, 1fr); gap: {3 if compact else 4}px; }}
    .day {{ min-height: {52 if compact else 64}px; border: 1px solid #f0f0f0; border-radius: 6px; position: relative; background:#fff; }}
    .day.muted {{ background:#fafafa; }}
    .n {{ position: absolute; top: 6px; right: 8px; font-size: {11 if compact else 12}px; color:#777; }}
    .bars {{ margin-top: 6px; display:grid; grid-template-columns: repeat(7, 1fr); row-gap: {3 if compact else 4}px; }}
    .bar {{ height: {18 if compact else 22}px; border-radius: 6px; font-size: {10 if compact else 12}px; line-height:{18 if compact else 22}px; padding: 0px 6px; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; border:1px solid rgba(0,0,0,.06); }}
    .legend {{ margin: 8px 0; font-size:{10 if compact else 12}px; }}
    .tag {{ display:inline-block; padding:{1 if compact else 2}px {6 if compact else 8}px; border-radius:10px; margin-right:6px; border:1px solid rgba(0,0,0,.06); }}
    @media (max-width: 520px) {{
        .month-title {{ font-size: 16px; }}
        .day {{ min-height: 48px; }}
        .bar {{ height: 18px; font-size: 10px; line-height: 18px; }}
    }}
    </style>
    """

    # 按月分组
    months = []
    cur = window_start.replace(day=1)
    while cur <= window_end:
        months.append((cur.year, cur.month))
        if cur.month == 12:
            cur = date(cur.year+1, 1, 1)
        else:
            cur = date(cur.year, cur.month+1, 1)

    # 头部：meta viewport + 图例
    head = (
        "<!DOCTYPE html><html><head>"
        "<meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        f"<title>{product} 到货月历</title>{css}</head><body>"
        f"<h2>{product} ：{window_start} ~ {window_end}</h2>"
        + "<div class='legend'>" + "".join([f"<span class='tag' style='background:{color_map.get(c, '#eee')}'>{c or '未知公司'}</span>" for c in color_map]) + "</div>"
    )

    html = [head]

    cal = calendar.Calendar(firstweekday=0)  # 周一开头

    for y, m in months:
        html.append(f"<div class='month'><div class='month-title'>{y}年{m}月</div>")
        for week in cal.monthdatescalendar(y, m):  # 每个 week 是 7 天的 date 列表
            week_start, week_end = week[0], week[-1]
            # 1) 先画每日方格
            day_cells = []
            for d in week:
                muted = " muted" if d.month != m else ""
                day_cells.append(f"<div class='day{muted}'><div class='n'>{d.day}</div></div>")
            # 2) bars：把与这一周相交的事件画成跨列条
            bar_divs = []
            for ev in events:
                seg_start = max(ev["start"], week_start)
                seg_end   = min(ev["end"],   week_end)
                if seg_start > seg_end:
                    continue
                start_idx = seg_start.weekday()  # 0..6
                span = (seg_end - seg_start).days + 1
                if short_label:
                    label = f"{ev['qty']:g}吨｜{ev['carrier']}｜{ev['datestr']}（{ev['days']}天）"
                else:
                    label = (
                        f"{ev['qty']:g} 吨｜{ev['carrier']}/{ev['warehouse']}｜"
                        f"箱:{ev['container']}｜{ev['datestr']}（{ev['days']}天）"
                    )
                bar_color = color_map.get(ev['carrier'], '#e8e8e8')
                bar_divs.append(
                    f"<div class='bar' title='{label}' style='grid-column:{start_idx+1} / span {span}; background:{bar_color}'>"
                    f"{label}</div>"
                )
            html.append("<div class='week'>")
            html.append("<div class='grid'>" + "".join(day_cells) + "</div>")
            html.append("<div class='bars'>" + "".join(bar_divs) + "</div>")
            html.append("</div>")
        html.append("</div>")

    html.append("</body></html>")
    return "\n".join(html)


# 页面内预览（单个产品）
st.subheader("日历视图")
products_with_data = [p for p in PREFERRED_ORDER if p in intransit_df["产品"].unique()]
prod_opt = st.selectbox("选择产品", products_with_data)
if prod_opt:
    # 移动端默认窗口短一点更清楚
    win_days = min(days_forward, 21) if is_mobile else days_forward
    evts = make_events_for_product(prod_opt, today, win_days)
    html = build_calendar_html_events_grid(prod_opt, today, win_days, evts, compact=is_mobile, short_label=short_label)
    st.components.v1.html(html, height=540 if is_mobile else 660, scrolling=True)


# 打包下载：每产品一个 HTML + index（继承手机样式）
def build_zip_calendars(all_prods, compact: bool, short_label: bool):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        index_lines = [f"<html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'><title>产品到货日历索引</title></head><body><h2>产品到货日历（连续事件条）</h2><ul>"]
        for prod in all_prods:
            evts = make_events_for_product(prod, today, days_forward)
            html = build_calendar_html_events_grid(prod, today, days_forward, evts, compact=compact, short_label=short_label)
            fname = f"calendar_{prod}.html"
            z.writestr(fname, html)
            index_lines.append(f"<li><a href='{fname}'>{prod}</a></li>")
        index_lines.append("</ul></body></html>")
        z.writestr("index.html", "\n".join(index_lines))
    buf.seek(0)
    return buf.getvalue()

st.download_button(
    "下载：日历视图（HTML 打包，连续事件条）.zip",
    build_zip_calendars(products_with_data, compact=is_mobile, short_label=short_label),
    file_name="产品到货_连续事件条_月历_HTML打包.zip",
    mime="application/zip"
)



