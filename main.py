from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse
import pandas as pd
import tempfile
import os
import re
from typing import List, Dict, Any


@app.get("/health")
def health():
    return {"ok": True}



app = FastAPI()


COMPETITOR_CHANNELS = {
    "MBC SPORTS+",
    "SBS Sports",
    "SBS SPORTS",
    "KBSN스포츠",
    "KBS N SPORTS",
    "SBS Golf",
    "JTBC GOLF",
    "JTBC Golf",
    "SPOTV",
    "SPOTV2",
    "SPOTV Golf&Health",
}

TVN_CHANNEL_CANDIDATES = {"tvN SPORTS", "tvN SPORTS2", "tvN SPORTS+"}


def normalize_text(v):
    if pd.isna(v):
        return ""
    return str(v).strip()


def parse_rating(v):
    if pd.isna(v):
        return None
    s = str(v).strip().replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def parse_date_from_filename(file_name: str):
    m = re.search(r"(\d{2})(\d{2})(\d{2})", file_name)
    if not m:
        return None
    yy, mm, dd = m.groups()
    return f"20{yy}-{mm}-{dd}"


def extract_rows_from_sheet(df: pd.DataFrame) -> List[Dict[str, Any]]:
    rows = []
    # 모든 셀을 문자열화
    df = df.fillna("")

    # 컬럼명 후보를 찾기 쉽도록 0~n행을 탐색
    # 실제 파일 구조가 불규칙하므로 "채널/프로그램/타겟/시청률" 같은 단어가 있는 행을 헤더 후보로 본다.
    header_row_idx = None
    for i in range(min(len(df), 20)):
        joined = " ".join([str(x) for x in df.iloc[i].tolist()])
        if ("채널" in joined or "CHANNEL" in joined.upper()) and ("프로그램" in joined or "시청률" in joined):
            header_row_idx = i
            break

    if header_row_idx is None:
        # 헤더를 못 찾으면 그냥 전부 스캔
        for _, row in df.iterrows():
            vals = [normalize_text(v) for v in row.tolist()]
            rows.append({"raw": vals})
        return rows

    headers = [normalize_text(x) for x in df.iloc[header_row_idx].tolist()]
    data_df = df.iloc[header_row_idx + 1 :].copy()
    data_df.columns = headers

    for _, row in data_df.iterrows():
        row_dict = {str(k): row[k] for k in data_df.columns}
        rows.append(row_dict)

    return rows


def pick_field(row: Dict[str, Any], candidates: List[str]):
    lower_map = {str(k).strip().lower(): k for k in row.keys()}
    for c in candidates:
        k = lower_map.get(c.lower())
        if k is not None:
            return row.get(k)
    return None


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
        xls = pd.ExcelFile(tmp_path, engine="xlrd" if suffix == ".xls" else None)

        all_rows = []
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            extracted = extract_rows_from_sheet(df)
            for r in extracted:
                r["_sheet"] = sheet_name
                all_rows.append(r)

        date_value = parse_date_from_filename(file.filename) or date_hint or None

        normalized_rows = []
        for row in all_rows:
            if "raw" in row:
                continue

            channel = normalize_text(
                pick_field(row, ["채널", "channel", "채널명"])
            )
            program = normalize_text(
                pick_field(row, ["프로그램", "program", "프로그램명"])
            )
            target = normalize_text(
                pick_field(row, ["타겟", "target", "성연령", "연령", "데모"])
            )
            rating = parse_rating(
                pick_field(row, ["시청률", "rating", "rtg", "가구시청률", "개인시청률", "25-59남"])
            )
            time_val = normalize_text(
                pick_field(row, ["시간", "방송시간", "time", "time_slot"])
            )

            if channel or program or target or rating is not None:
                normalized_rows.append({
                    "sheet": row.get("_sheet", ""),
                    "channel": channel,
                    "program": program,
                    "target": target,
                    "rating": rating,
                    "time": time_val,
                    "raw": row
                })

        tvn_rows = [
            r for r in normalized_rows
            if r["channel"] in TVN_CHANNEL_CANDIDATES
            and ("25-59남" in r["target"] or r["target"] == "25-59남")
            and r["rating"] is not None
            and r["rating"] >= 0.019
        ]

        competitor_rows = [
            r for r in normalized_rows
            if r["channel"] in COMPETITOR_CHANNELS
            and (r["target"] == "25-59남" or "25-59남" in r["target"])
            and r["rating"] is not None
        ]

        summary_lines = []
        summary_lines.append(f"총 시트 수: {len(xls.sheet_names)}")
        summary_lines.append(f"정규화된 행 수: {len(normalized_rows)}")
        summary_lines.append(f"tvN SPORTS 25-59남 0.019 이상 행 수: {len(tvn_rows)}")
        summary_lines.append(f"경쟁채널 관련 행 수: {len(competitor_rows)}")

        return {
            "file_name": file.filename,
            "date": date_value,
            "tvn_sports_2559m_over_0019": tvn_rows,
            "competitor_rows": competitor_rows,
            "all_relevant_rows": normalized_rows[:300],
            "summary_text": "\n".join(summary_lines),
        }

    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass