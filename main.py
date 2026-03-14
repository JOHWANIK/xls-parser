from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse
import pandas as pd
import tempfile
import os
import re
from typing import Dict, Any, List, Optional

app = FastAPI()

# ===== 설정 =====
TVN_CHANNEL_KEYWORDS = [
    "tvn sports",
    "tvn sports2",
    "tvn sports+",
    "tvn스포츠",
]

COMPETITOR_CHANNEL_KEYWORDS = [
    "mbc sports+",
    "sbs sports",
    "kbsn스포츠",
    "kbs n sports",
    "sbs golf",
    "jtbc golf",
    "jtbc golf&sports",
    "spotv",
    "spotv2",
    "spotv golf",
    "spotv golf&health",
]

TARGET_KEYWORDS = ["25-59남", "2559남", "25~59남", "25-59 남", "m25-59"]

MAX_SHEETS_TO_SCAN = 6
PREVIEW_ROWS = 80
FULL_READ_ROWS = 250


@app.get("/health")
def health():
    return {"ok": True}


def norm_text(v: Any) -> str:
    if pd.isna(v):
        return ""
    s = str(v).strip()
    s = s.replace("\n", " ").replace("\r", " ")
    s = re.sub(r"\s+", " ", s)
    return s


def norm_key(s: str) -> str:
    s = norm_text(s).lower()
    s = s.replace(" ", "")
    s = s.replace("_", "")
    s = s.replace("-", "")
    return s


def parse_date_from_filename(file_name: str) -> Optional[str]:
    # 예: CJ ENM260311.xls -> 2026-03-11
    m = re.search(r"(\d{2})(\d{2})(\d{2})", file_name)
    if not m:
        return None
    yy, mm, dd = m.groups()
    return f"20{yy}-{mm}-{dd}"


def parse_float(v: Any) -> Optional[float]:
    if pd.isna(v):
        return None
    s = str(v).strip().replace(",", "")
    s = s.replace("%", "")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        m = re.search(r"[-+]?\d*\.?\d+", s)
        if m:
            try:
                return float(m.group())
            except Exception:
                return None
    return None


def contains_any(text: str, keywords: List[str]) -> bool:
    t = norm_text(text).lower()
    return any(k.lower() in t for k in keywords)


def choose_candidate_sheets(xls: pd.ExcelFile) -> List[str]:
    scored = []

    for sheet_name in xls.sheet_names:
        score = 0
        low_sheet = sheet_name.lower()

        if "sports" in low_sheet or "스포츠" in low_sheet:
            score += 3
        if "tvn" in low_sheet:
            score += 2
        if "male" in low_sheet or "남" in low_sheet:
            score += 1

        try:
            preview_df = pd.read_excel(
                xls,
                sheet_name=sheet_name,
                header=None,
                nrows=40
            ).fillna("")
            preview_text = " ".join(norm_text(v) for v in preview_df.astype(str).values.flatten())
            if contains_any(preview_text, TVN_CHANNEL_KEYWORDS):
                score += 4
            if contains_any(preview_text, COMPETITOR_CHANNEL_KEYWORDS):
                score += 3
            if contains_any(preview_text, TARGET_KEYWORDS):
                score += 4
        except Exception:
            pass

        scored.append((sheet_name, score))

    scored.sort(key=lambda x: x[1], reverse=True)
    selected = [name for name, score in scored[:MAX_SHEETS_TO_SCAN]]
    return selected if selected else xls.sheet_names[:MAX_SHEETS_TO_SCAN]


def detect_header_row(df: pd.DataFrame) -> Optional[int]:
    limit = min(len(df), 40)

    best_idx = None
    best_score = -1

    for i in range(limit):
        vals = [norm_text(v) for v in df.iloc[i].tolist()]
        joined = " | ".join(vals).lower()

        score = 0

        # 헤더로 보이는 단어들
        if "채널" in joined or "channel" in joined:
            score += 2
        if "프로그램" in joined or "program" in joined or "프로명" in joined:
            score += 2
        if "시간" in joined or "time" in joined:
            score += 1
        if "시청률" in joined or "rating" in joined or "rtg" in joined:
            score += 2
        if contains_any(joined, TARGET_KEYWORDS):
            score += 3

        unique_nonempty = len([x for x in vals if x])
        if unique_nonempty >= 4:
            score += 1

        if score > best_score:
            best_score = score
            best_idx = i

    if best_score >= 3:
        return best_idx
    return None


def build_header_map(headers: List[str]) -> Dict[str, str]:
    """
    표준 키 -> 실제 컬럼명
    """
    actuals = headers
    nk_map = {norm_key(h): h for h in actuals if norm_text(h)}

    result = {}

    def pick(candidates: List[str]) -> Optional[str]:
        for c in candidates:
            nk = norm_key(c)
            if nk in nk_map:
                return nk_map[nk]
        # 부분 포함도 허용
        for nk, actual in nk_map.items():
            for c in candidates:
                c_nk = norm_key(c)
                if c_nk and c_nk in nk:
                    return actual
        return None

    result["channel"] = pick(["채널", "채널명", "channel"])
    result["program"] = pick(["프로그램", "프로그램명", "program", "프로명"])
    result["time"] = pick(["시간", "방송시간", "time", "time_slot", "타임"])
    result["rating"] = pick(["시청률", "rating", "rtg", "가구시청률", "개인시청률"])

    # 타겟 전용 컬럼들
    for target_kw in TARGET_KEYWORDS:
        col = pick([target_kw])
        if col:
            result["target_rating_column"] = col
            break

    # 성연령/타깃 컬럼
    result["target"] = pick(["성연령", "타겟", "target", "데모", "연령"])

    return result


def extract_rows_from_sheet(xls: pd.ExcelFile, sheet_name: str) -> List[Dict[str, Any]]:
    df = pd.read_excel(
        xls,
        sheet_name=sheet_name,
        header=None,
        nrows=FULL_READ_ROWS
    ).fillna("")

    if df.empty:
        return []

    header_idx = detect_header_row(df)
    rows: List[Dict[str, Any]] = []

    # 헤더를 못 찾으면 관련 키워드 있는 행만 raw로 추출
    if header_idx is None:
        for row_idx in range(min(len(df), PREVIEW_ROWS)):
            vals = [norm_text(v) for v in df.iloc[row_idx].tolist()]
            joined = " | ".join(vals)
            if contains_any(joined, TVN_CHANNEL_KEYWORDS + COMPETITOR_CHANNEL_KEYWORDS + TARGET_KEYWORDS):
                rows.append({
                    "sheet": sheet_name,
                    "row_index": row_idx,
                    "channel": "",
                    "program": "",
                    "target": "",
                    "rating": None,
                    "time": "",
                    "raw_text": joined[:1000]
                })
        return rows

    headers = [norm_text(v) for v in df.iloc[header_idx].tolist()]
    data_df = df.iloc[header_idx + 1:].copy()
    data_df.columns = headers
    header_map = build_header_map(headers)

    channel_col = header_map.get("channel")
    program_col = header_map.get("program")
    time_col = header_map.get("time")
    rating_col = header_map.get("rating")
    target_col = header_map.get("target")
    target_rating_col = header_map.get("target_rating_column")

    for row_idx, (_, row) in enumerate(data_df.iterrows(), start=header_idx + 1):
        channel = norm_text(row[channel_col]) if channel_col and channel_col in row else ""
        program = norm_text(row[program_col]) if program_col and program_col in row else ""
        time_val = norm_text(row[time_col]) if time_col and time_col in row else ""
        target_val = norm_text(row[target_col]) if target_col and target_col in row else ""

        # 우선순위: 25-59남 전용 컬럼 > 일반 시청률 컬럼
        rating = None
        if target_rating_col and target_rating_col in row:
            rating = parse_float(row[target_rating_col])
            if rating is not None and not target_val:
                target_val = "25-59남"
        elif rating_col and rating_col in row:
            rating = parse_float(row[rating_col])

        # raw 텍스트
        raw_vals = [norm_text(v) for v in row.tolist()]
        raw_joined = " | ".join([v for v in raw_vals if v])

        # 관련성 필터
        relevant = False
        if contains_any(channel, TVN_CHANNEL_KEYWORDS + COMPETITOR_CHANNEL_KEYWORDS):
            relevant = True
        if contains_any(raw_joined, TVN_CHANNEL_KEYWORDS + COMPETITOR_CHANNEL_KEYWORDS):
            relevant = True
        if contains_any(target_val, TARGET_KEYWORDS):
            relevant = True
        if contains_any(raw_joined, TARGET_KEYWORDS):
            relevant = True

        if relevant:
            rows.append({
                "sheet": sheet_name,
                "row_index": row_idx,
                "channel": channel,
                "program": program,
                "target": target_val,
                "rating": rating,
                "time": time_val,
                "raw_text": raw_joined[:1000]
            })

    return rows


def classify_rows(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    tvn_rows = []
    competitor_rows = []
    all_relevant = []

    for r in rows:
        channel_low = r.get("channel", "").lower()
        raw_low = r.get("raw_text", "").lower()
        target = r.get("target", "")
        rating = r.get("rating")

        is_tvn = any(k in channel_low or k in raw_low for k in TVN_CHANNEL_KEYWORDS)
        is_comp = any(k in channel_low or k in raw_low for k in COMPETITOR_CHANNEL_KEYWORDS)
        is_target = contains_any(target, TARGET_KEYWORDS) or contains_any(raw_low, TARGET_KEYWORDS)

        if is_tvn or is_comp or is_target:
            all_relevant.append(r)

        if is_tvn and is_target and rating is not None and rating >= 0.019:
            tvn_rows.append(r)

        if is_comp and (is_target or rating is not None):
            competitor_rows.append(r)

    # rating 높은 순 정렬
    tvn_rows.sort(key=lambda x: (x["rating"] is not None, x["rating"]), reverse=True)
    competitor_rows.sort(key=lambda x: (x["rating"] is not None, x["rating"]), reverse=True)

    return {
        "tvn_rows": tvn_rows[:100],
        "competitor_rows": competitor_rows[:150],
        "all_relevant": all_relevant[:300],
    }


@app.post("/parse-xls")
async def parse_xls(
    file: UploadFile = File(...),
    question_type: str = Form("tvn_sports_analysis"),
    date_hint: str = Form("")
):
    suffix = os.path.splitext(file.filename)[1].lower()
    if suffix not in [".xls", ".xlsx"]:
        return JSONResponse(
            status_code=400,
            content={"error": "Only .xls or .xlsx files are supported"}
        )

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        engine = "xlrd" if suffix == ".xls" else None
        xls = pd.ExcelFile(tmp_path, engine=engine)

        candidate_sheets = choose_candidate_sheets(xls)

        extracted_rows: List[Dict[str, Any]] = []
        for sheet_name in candidate_sheets:
            try:
                rows = extract_rows_from_sheet(xls, sheet_name)
                extracted_rows.extend(rows)
            except Exception:
                continue

        classified = classify_rows(extracted_rows)
        final_date = parse_date_from_filename(file.filename) or date_hint or ""

        summary_lines = [
            f"파일명: {file.filename}",
            f"질문유형: {question_type}",
            f"날짜: {final_date}",
            f"전체 시트 수: {len(xls.sheet_names)}",
            f"스캔 시트 수: {len(candidate_sheets)}",
            f"추출 행 수: {len(extracted_rows)}",
            f"tvN SPORTS 25-59남 0.019 이상 행 수: {len(classified['tvn_rows'])}",
            f"경쟁채널 관련 행 수: {len(classified['competitor_rows'])}",
        ]

        return {
            "file_name": file.filename,
            "date": final_date,
            "scanned_sheets": candidate_sheets,
            "tvn_sports_2559m_over_0019": classified["tvn_rows"],
            "competitor_rows": classified["competitor_rows"],
            "all_relevant_rows": classified["all_relevant"],
            "summary_text": "\n".join(summary_lines),
        }

    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass