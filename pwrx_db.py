"""
PitchingWRX — PostgreSQL Database Layer
"""

import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
import datetime
import os

DATABASE_URL = os.environ.get("DATABASE_URL")

SCHEMA = """
CREATE TABLE IF NOT EXISTS pitches (
    id              SERIAL PRIMARY KEY,
    pitcher_id      INTEGER,
    pitcher_name    TEXT,
    game_date       DATE,
    game_id         TEXT,
    opponent        TEXT,
    team            TEXT,
    level           TEXT,
    pitch_num       INTEGER,
    pitch_type      TEXT,
    pitch_type_full TEXT,
    batter_hand     TEXT,
    count_str       TEXT,
    inning          TEXT,
    outs            INTEGER,
    pitch_result    TEXT,
    pitch_outcome   TEXT,
    vel             REAL,
    spin            REAL,
    spin_eff        REAL,
    ivb             REAL,
    hb              REAL,
    rel_x           REAL,
    rel_z           REAL,
    extension       REAL,
    vert_appr_angle REAL,
    horz_appr_angle REAL,
    px              REAL,
    pz              REAL,
    exit_vel        REAL,
    launch_ang      REAL,
    is_swing        INTEGER,
    is_whiff        INTEGER,
    is_called_strike INTEGER,
    is_strike       INTEGER,
    uniq_pitch_id   TEXT UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_pitcher_date ON pitches(pitcher_id, game_date);
CREATE INDEX IF NOT EXISTS idx_pitcher_name ON pitches(pitcher_name);
CREATE INDEX IF NOT EXISTS idx_game_date    ON pitches(game_date);
"""

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(SCHEMA)
    conn.commit()
    cur.close()
    conn.close()

def _fix_count(val):
    if isinstance(val, str) and '-' in val and len(val) <= 3:
        return val
    if isinstance(val, datetime.datetime):
        balls   = min(val.month - 1, 3)
        strikes = min(val.day   - 1, 2)
        return f"{balls}-{strikes}"
    return str(val)

def ingest_xlsx(path, verbose=True):
    init_db()
    df = pd.read_excel(path)

    for col in ['Vel','Spin','SpinEff','IndVertBrk','HorzBrk','RelX','RelZ',
                'Extension','ExitVel','LaunchAng','VertApprAngle','HorzApprAngle']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].replace('-', np.nan), errors='coerce')

    df['count_fixed']      = df['count'].apply(_fix_count)
    df['is_whiff']         = df['pitchResult'].isin(['Strike Swinging','Strikeout (Swinging)']).astype(int)
    df['is_called_strike'] = (df['pitchResult'] == 'Strike Looking').astype(int)
    df['is_swing']         = df['pitchOutcome'].isin(['S','B']).astype(int)
    df['is_strike']        = df['pitchOutcome'].isin(['S','SL']).astype(int)
    df['px']               = -df['x']
    df['pz']               = df['y'] + 2.5
    df['gameDate']         = pd.to_datetime(df['gameDate'])

    if 'uniqPitchId' not in df.columns or df['uniqPitchId'].isna().all():
        df['uniqPitchId'] = (df['fullName'].astype(str) + '_' +
                             df['gameDate'].astype(str) + '_' +
                             df['pitchNumInGame'].astype(str))

    conn = get_conn()
    cur  = conn.cursor()
    inserted = 0; skipped = 0

    for _, row in df.iterrows():
        uid = str(row.get('uniqPitchId', ''))
        try:
            cur.execute("""
                INSERT INTO pitches (
                    pitcher_id, pitcher_name, game_date, game_id,
                    opponent, team, level,
                    pitch_num, pitch_type, pitch_type_full, batter_hand,
                    count_str, inning, outs, pitch_result, pitch_outcome,
                    vel, spin, spin_eff, ivb, hb, rel_x, rel_z, extension,
                    vert_appr_angle, horz_appr_angle,
                    px, pz, exit_vel, launch_ang,
                    is_swing, is_whiff, is_called_strike, is_strike,
                    uniq_pitch_id
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
                ON CONFLICT (uniq_pitch_id) DO NOTHING
            """, (
                row.get('pitcherId'), row.get('fullName'),
                row['gameDate'].date().isoformat(), str(row.get('gameId','')),
                row.get('opponent',''), row.get('team',''), row.get('level',''),
                row.get('pitchNumInGame'), row.get('pitchType',''), row.get('pitchTypeFull',''),
                row.get('batterHand',''), row.get('count_fixed',''),
                str(row.get('inn','')), row.get('outs'),
                row.get('pitchResult',''), row.get('pitchOutcome',''),
                row.get('Vel'), row.get('Spin'), row.get('SpinEff'),
                row.get('IndVertBrk'), row.get('HorzBrk'),
                row.get('RelX'), row.get('RelZ'), row.get('Extension'),
                row.get('VertApprAngle'), row.get('HorzApprAngle'),
                row.get('px'), row.get('pz'), row.get('ExitVel'), row.get('LaunchAng'),
                int(row.get('is_swing',0)), int(row.get('is_whiff',0)),
                int(row.get('is_called_strike',0)), int(row.get('is_strike',0)),
                uid,
            ))
            inserted += 1
        except Exception:
            skipped += 1

    conn.commit()
    cur.close()
    conn.close()

    if verbose:
        name  = df['fullName'].iloc[0]
        dates = df['gameDate'].dt.date.unique()
        print(f"✓ {name} — {len(dates)} game(s) — {inserted} inserted, {skipped} skipped")

    return inserted, skipped

def get_game_log(pitcher_id=None, pitcher_name=None):
    conn = get_conn()
    clauses = []; params = []

    if pitcher_id:
        clauses.append("pitcher_id = %s"); params.append(int(pitcher_id))
    elif pitcher_name:
        clauses.append("pitcher_name = %s"); params.append(pitcher_name)
    else:
        conn.close(); return pd.DataFrame()

    where = " AND ".join(clauses)
    df = pd.read_sql(f"""
        SELECT
            game_date, opponent, level, team, pitch_type_full,
            COUNT(*)                                            AS pitches,
            ROUND(AVG(vel)::numeric, 1)                        AS avg_vel,
            ROUND(MAX(vel)::numeric, 1)                        AS max_vel,
            ROUND(AVG(spin)::numeric, 0)                       AS avg_spin,
            ROUND(AVG(ivb)::numeric, 1)                        AS avg_ivb,
            ROUND(AVG(hb)::numeric, 1)                         AS avg_hb,
            ROUND((SUM(is_whiff)*100.0 /
                  NULLIF(SUM(is_swing),0))::numeric, 1)        AS whiff_pct,
            ROUND(((SUM(is_called_strike)+SUM(is_whiff))*100.0
                  / COUNT(*))::numeric, 1)                     AS csw_pct,
            ROUND((SUM(is_strike)*100.0 / COUNT(*))::numeric, 1) AS strike_pct
        FROM pitches
        WHERE {where}
        GROUP BY game_date, pitch_type_full, opponent, level, team
        ORDER BY game_date
    """, conn, params=params)
    conn.close()
    return df

def season_averages(pitcher_id=None, pitcher_name=None, before_date=None):
    conn = get_conn()
    clauses = []; params = []

    if pitcher_id:
        clauses.append("pitcher_id = %s"); params.append(int(pitcher_id))
    elif pitcher_name:
        clauses.append("pitcher_name = %s"); params.append(pitcher_name)

    if before_date:
        clauses.append("game_date < %s")
        params.append(before_date.strftime('%Y-%m-%d')
                      if isinstance(before_date, (datetime.date, datetime.datetime))
                      else str(before_date))

    where = " AND ".join(clauses) if clauses else "1=1"
    df = pd.read_sql(f"""
        SELECT
            pitch_type_full                                        AS "Pitch",
            COUNT(*)                                               AS season_pitches,
            ROUND(AVG(vel)::numeric, 1)                           AS season_avg_vel,
            ROUND(MAX(vel)::numeric, 1)                           AS season_max_vel,
            ROUND(AVG(spin)::numeric, 0)                          AS season_avg_spin,
            ROUND(AVG(ivb)::numeric, 1)                           AS season_avg_ivb,
            ROUND(AVG(hb)::numeric, 1)                            AS season_avg_hb,
            ROUND((SUM(is_whiff)*100.0 /
                  NULLIF(SUM(is_swing),0))::numeric, 1)           AS season_whiff_pct,
            ROUND(((SUM(is_called_strike)+SUM(is_whiff))*100.0
                  / COUNT(*))::numeric, 1)                        AS season_csw_pct,
            ROUND((SUM(is_strike)*100.0 / COUNT(*))::numeric, 1)  AS season_strike_pct
        FROM pitches
        WHERE {where}
        GROUP BY pitch_type_full
    """, conn, params=params)
    conn.close()
    return df
