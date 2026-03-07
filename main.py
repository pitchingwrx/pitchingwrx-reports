from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import StreamingResponse, JSONResponse
import tempfile, os, io, traceback
import pandas as pd

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/games")
async def list_games(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
            tmp.write(contents)
            tmp_path = tmp.name
        df = pd.read_excel(tmp_path)
        os.unlink(tmp_path)

        if 'gameDate' not in df.columns:
            return JSONResponse({"error": "No gameDate column found"}, status_code=400)

        df['gameDate'] = pd.to_datetime(df['gameDate'])
        games = (df.groupby('gameDate')
                   .agg(
                       opponent=('opponent', 'first') if 'opponent' in df.columns else ('gameDate', 'count'),
                       level=('level', 'first') if 'level' in df.columns else ('gameDate', 'count'),
                       pitches=('gameDate', 'count')
                   )
                   .reset_index()
                   .sort_values('gameDate', ascending=False))

        result = []
        for _, row in games.iterrows():
            date_str = row['gameDate'].strftime('%Y-%m-%d')
            label = row['gameDate'].strftime('%b %d, %Y')
            if 'opponent' in games.columns:
                label += f" vs {row['opponent']}"
            if 'level' in games.columns:
                label += f" ({row['level']})"
            label += f" - {row['pitches']} pitches"
            result.append({"date": date_str, "label": label})

        return {"games": result}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/generate")
async def generate(
    file: UploadFile = File(...),
    game_date: str = Form(None)
):
    tmp_xlsx = None
    tmp_pdf = None
    try:
        contents = await file.read()
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
            tmp.write(contents)
            tmp_xlsx = tmp.name

        from generate_report import build_report

        out = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
        out.close()
        tmp_pdf = out.name

        logo_path = os.path.join(os.path.dirname(__file__), 'pwrx_logo.png')

        build_report(
            data_path=tmp_xlsx,
            logo_path=logo_path,
            output_path=tmp_pdf,
            db_path=None,
            game_date=game_date
        )

        with open(tmp_pdf, 'rb') as f:
            pdf_bytes = f.read()

        return StreamingResponse(
            io.BytesIO(pdf_bytes),
            media_type='application/pdf',
            headers={'Content-Disposition': 'attachment; filename="report.pdf"'}
        )
    except Exception as e:
        traceback.print_exc()
        raise
    finally:
        if tmp_xlsx and os.path.exists(tmp_xlsx):
            os.unlink(tmp_xlsx)
        if tmp_pdf and os.path.exists(tmp_pdf):
            os.unlink(tmp_pdf)
