from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import StreamingResponse, JSONResponse
import tempfile, os, io, traceback
import pandas as pd

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/ingest")
async def ingest(file: UploadFile = File(...)):
    tmp_path = None
    try:
        contents = await file.read()
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
            tmp.write(contents)
            tmp_path = tmp.name

        from pwrx_db import ingest_xlsx
        result = ingest_xlsx(tmp_path, verbose=False)

        return {
            "status": "success",
            "inserted": result["inserted"],
            "skipped":  result["skipped"],
            "flagged":  result["flagged"],
            "warnings": result["warnings"],
            "summary":  result["summary"]
        }

    except Exception as e:
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


@app.get("/roster")
def roster():
    try:
        from pwrx_db import get_conn
        conn = get_conn()
        df = pd.read_sql(
            """
            SELECT
                pitcher_name,
                COUNT(DISTINCT game_date) AS games,
                COUNT(*) AS pitches,
                MIN(game_date) AS first_game,
                MAX(game_date) AS last_game
            FROM pitches
            GROUP BY pitcher_name
            ORDER BY pitcher_name
            """,
            conn
        )
        conn.close()
        result = []
        for _, row in df.iterrows():
            result.append({
                "player": row['pitcher_name'],
                "games": int(row['games']),
                "pitches": int(row['pitches']),
                "first_game": str(row['first_game']),
                "last_game": str(row['last_game'])
            })
        return {"roster": result}
    except Exception as e:
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/player_games")
def player_games(player: str):
    try:
        from pwrx_db import get_conn
        conn = get_conn()
        df = pd.read_sql(
            """
            SELECT
                game_date,
                opponent,
                level,
                team,
                COUNT(*) AS pitches
            FROM pitches
            WHERE pitcher_name = %s
            GROUP BY game_date, opponent, level, team
            ORDER BY game_date DESC
            """,
            conn, params=[player]
        )
        conn.close()
        games = []
        for _, row in df.iterrows():
            date_str = str(row['game_date'])
            label = pd.to_datetime(date_str).strftime('%b %d, %Y')
            if row.get('opponent'):
                label += f" vs {row['opponent']}"
            if row.get('level'):
                label += f" ({row['level']})"
            label += f" - {int(row['pitches'])} pitches"
            games.append({"date": date_str, "label": label})
        return {"games": games}
    except Exception as e:
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


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
        name_col = next((c for c in ['fullName','pitcher','pitcherName','Pitcher'] if c in df.columns), None)

        result = []
        group_cols = [name_col, 'gameDate'] if name_col else ['gameDate']
        for key, group in df.groupby(group_cols):
            player, game_date = key if name_col else ('Unknown', key)
            date_str = pd.Timestamp(game_date).strftime('%Y-%m-%d')
            label = pd.Timestamp(game_date).strftime('%b %d, %Y')
            if 'opponent' in df.columns:
                label += f" vs {group['opponent'].iloc[0]}"
            if 'level' in df.columns:
                label += f" ({group['level'].iloc[0]})"
            label += f" - {len(group)} pitches"
            result.append({"player": str(player), "date": date_str, "label": label})

        result.sort(key=lambda x: (x['player'], x['date']))
        return {"games": result}

    except Exception as e:
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/generate")
async def generate(
    file: UploadFile = File(...),
    game_date: str = Form(None),
    player_name: str = Form(None)
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
            game_date=game_date,
            player_name=player_name
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


@app.post("/generate_from_db")
async def generate_from_db(
    player_name: str = Form(...),
    game_date: str = Form(...)
):
    tmp_pdf = None
    try:
        from generate_report import build_report_from_db

        out = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
        out.close()
        tmp_pdf = out.name

        logo_path = os.path.join(os.path.dirname(__file__), 'pwrx_logo.png')

        build_report_from_db(
            player_name=player_name,
            game_date=game_date,
            logo_path=logo_path,
            output_path=tmp_pdf
        )

        with open(tmp_pdf, 'rb') as f:
            pdf_bytes = f.read()

        safe_name = player_name.replace(' ', '_')
        return StreamingResponse(
            io.BytesIO(pdf_bytes),
            media_type='application/pdf',
            headers={'Content-Disposition': f'attachment; filename="{safe_name}_{game_date}.pdf"'}
        )
    except Exception as e:
        traceback.print_exc()
        raise
    finally:
        if tmp_pdf and os.path.exists(tmp_pdf):
            os.unlink(tmp_pdf)
