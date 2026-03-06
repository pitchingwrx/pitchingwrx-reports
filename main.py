"""
PitchingWRX — FastAPI Backend
"""

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
import pandas as pd
import tempfile
import os
import io

from pwrx_db import ingest_xlsx, season_averages, get_game_log

app = FastAPI(title="PitchingWRX Report API")

@app.get("/")
def root():
    return {"status": "PitchingWRX API is live"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/generate")
async def generate_report(file: UploadFile = File(...)):
    """
    Upload a game XLSX file.
    Returns a generated PDF report.
    """

    # Validate file type
    if not file.filename.endswith('.xlsx'):
        raise HTTPException(status_code=400, detail="File must be a .xlsx file")

    # Save uploaded file to a temp location
    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_xlsx:
        contents = await file.read()
        tmp_xlsx.write(contents)
        xlsx_path = tmp_xlsx.name

    # Output PDF path
    pdf_path = xlsx_path.replace('.xlsx', '.pdf')

    try:
        # Pull player info from the file to name the PDF
        df_peek = pd.read_excel(xlsx_path)
        player_name = df_peek['fullName'].iloc[0] if 'fullName' in df_peek.columns else 'pitcher'
        game_date   = pd.to_datetime(df_peek['gameDate'].iloc[0]).strftime('%Y-%m-%d')
        safe_name   = player_name.replace(' ', '_').lower()

        # Build the report
        from generate_report import build_report
        build_report(
            data_path   = xlsx_path,
            logo_path   = 'pwrx_logo.png',
            output_path = pdf_path,
            db_path     = None,   # Postgre​​​​​​​​​​​
      )

        # Stream PDF back to client
        with open(pdf_path, 'rb') as f:
            pdf_bytes = f.read()

        return StreamingResponse(
            io.BytesIO(pdf_bytes),
            media_type='application/pdf',
            headers={
                'Content-Disposition': f'attachment; filename="{safe_name}_{game_date}_report.pdf"'
            }
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if os.path.exists(xlsx_path): os.remove(xlsx_path)
        if os.path.exists(pdf_path):  os.remove(pdf_path)
