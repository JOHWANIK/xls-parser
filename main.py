from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse
import pandas as pd
import tempfile
import os
import re

app = FastAPI()

COMPETITOR_KEYWORDS = [
    "MBC SPORTS+", "SBS SPORTS", "SBS Sports", "KBSN스포츠",
    "KBS N SPORTS", "SBS Golf", "JTBC GOLF", "JTBC Golf", "SPOTV"
]

TVN_KEYWORDS = ["tvN SPORTS", "tvN SPORTS2", "tvN SPORTS+"]

@app.get("/health")
def health():
    return {"ok": True}

def parse_date_from_filename(file_name: str):
    m = re.search(r"(\d{2})(\d{2})(\d{2})", file_name)
    if not m:
        return None
    yy, mm, dd = m.groups()
    return f"20{yy}-{mm}-{dd}"

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

        limited_sheets = xls.sheet_names[:3]  # 처음엔 최대 3개 시트만
        matched_rows = []

        for sheet_name in limited_sheets:
            # 앞 120행만 읽기
            df = pd.read_excel(
                xls,
                sheet_name=sheet_name,
                header=None,
                nrows=120
            ).fillna("")

            for row_idx in range(len(df)):
                vals = [str(v).strip() for v in df.iloc[row_idx].tolist()]
                joined = " | ".join(vals)

                has_tvn = any(k.lower() in joined.lower() for k in TVN_KEYWORDS)
                has_comp = any(k.lower() in joined.lower() for k in COMPETITOR_KEYWORDS)
                has_target = "25-59남" in joined
                has_rating_like = any("." in v for v in vals)

                if has_target or has_tvn or has_comp:
                    matched_rows.append({
                        "sheet": sheet_name,
                        "row_index": row_idx,
                        "text": joined[:1000]
                    })

        date_value = parse_date_from_filename(file.filename) or date_hint or ""

        return {
            "file_name": file.filename,
            "date": date_value,
            "matched_rows": matched_rows[:100],
            "summary_text": f"시트 {len(limited_sheets)}개, 추출행 {len(matched_rows)}개"
        }

    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass