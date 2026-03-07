"""
PitchingWRX -- PostgreSQL Database Layer
Ingests game XLSX files, stores pitch-level data, provides historical queries.
Includes robust column mapping, data validation, and quality safeguards.
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

# ── Column aliases ─────────────────────────────────────────────────────────────
# Maps canonical name -> list of known aliases (all lowercased for matching)
COLUMN_ALIASES = {
    'pitcherId':        ['pitcherid', 'pitcher_id', 'playerid', 'player_id'],
    'fullName':         ['fullname', 'full_name', 'pitcher', 'pitchername',
                         'pitcher_name', 'name', 'player'],
    'gameDate':         ['gamedate', 'game_date', 'date', 'gametime', 'game_time'],
    'gameId':           ['gameid', 'game_id', 'gamepk', 'game_pk'],
    'opponent':         ['opponent', 'opp', 'opposing_team', 'opposingteam'],
    'team':             ['team', 'pitching_team', 'pitchingteam'],
    'level':            ['level', 'gamelevel', 'game_level', 'league'],
    'pitchNumInGame':   ['pitchnumingame', 'pitch_num', 'pitchnum', 'pitchnumber',
                         'pitch_number', 'pitchnuminpa'],
    'pitchType':        ['pitchtype', 'pitch_type', 'pitchabbr', 'pitch_abbr', 'taggedpitchtype'],
    'pitchTypeFull':    ['pitchtypefull', 'pitch_type_full', 'autopitchtype',
                         'auto_pitch_type', 'pitchname', 'pitch_name'],
    'batterHand':       ['batterhand', 'batter_hand', 'batside', 'bat_side',
                         'batter_side', 'batterside'],
    'count':            ['count', 'count_str', 'pitchcount', 'balls_strikes'],
    'inn':              ['inn', 'inning', 'innnum', 'inning_num'],
    'outs':             ['outs', 'outswhen', 'outs_when_up'],
    'pitchResult':      ['pitchresult', 'pitch_result', 'result', 'pitchcall',
                         'pitch_call', 'call'],
    'pitchOutcome':     ['pitchoutcome', 'pitch_outcome', 'outcome', 'kzone'],
    'Vel':              ['vel', 'velocity', 'relspeed', 'rel_speed', 'speed',
                         'pitchspeed', 'pitch_speed', 'startspeed', 'releaspeed'],
    'Spin':             ['spin', 'spinrate', 'spin_rate', 'spinrpm', 'spin_rpm',
                         'relspinrate', 'topspin'],
    'SpinEff':          ['spineff', 'spin_eff', 'spinefficiency', 'spin_efficiency',
                         'spineffpct', 'truespin'],
    'IndVertBrk':       ['indvertbrk', 'ind_vert_brk', 'ivb', 'inducedvertbreak',
                         'induced_vert_break', 'inducedverticalbreak', 'vertbreak',
                         'vert_break', 'verticalbreak', 'inducedbreakvert'],
    'HorzBrk':          ['horzbrk', 'horz_brk', 'hb', 'horizbreak',
                         'horiz_break', 'horizontalbreak', 'horizontal_break',
                         'breakhorz', 'break_horz'],
    'RelX':             ['relx', 'rel_x', 'releasex', 'release_x',
                         'releaseposx', 'release_pos_x'],
    'RelZ':             ['relz', 'rel_z', 'releasez', 'release_z',
                         'releaseposy', 'release_pos_z'],
    'Extension':        ['extension', 'ext', 'releaseextension', 'release_extension'],
    'VertApprAngle':    ['vertapprangle', 'vert_appr_angle', 'vaa',
                         'verticalapproachangle', 'vertical_approach_angle'],
    'HorzApprAngle':    ['horzapprangle', 'horz_appr_angle', 'haa',
                         'horizontalapproachangle', 'horizontal_approach_angle'],
    'x':                ['x', 'platex', 'plate_x', 'locationx', 'location_x'],
    'y':                ['y', 'platez', 'plate_z', 'locationz', 'location_z',
                         'platey', 'plate_y'],
    'ExitVel':          ['exitvel', 'exit_vel', 'exitvelocity', 'exit_velocity',
                         'exitspeed', 'exit_speed', 'launchspeed', 'launch_speed'],
    'LaunchAng':        ['launchang', 'launch_ang', 'launchangle', 'launch_angle',
                         'launchvertangle', 'angle'],
}

# Required columns -- ingest will fail if these are missing
REQUIRED_COLS = ['fullName', 'gameDate']

# Numeric validation ranges (min, max) -- out of range flagged as suspicious
VALID_RANGES = {
    'Vel':       (40,  115),
    'Spin':      (500, 4000),
    'SpinEff':   (0,   100),
    'IndVertBrk':(-30, 30),
    'HorzBrk':   (-30, 30),
    'RelX':      (-4,  4),
    'RelZ':      (3,   8),
    'Extension': (4,   8),
    'ExitVel':   (20,  130),
    'LaunchAng': (-90, 90),
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(SCHEMA)
    conn.commit()
    cur.close()
    conn.close()

def _normalize_name(name):
    """Trim whitespace and title-case player names."""
    if pd.isna(name):
        return name
    return str(name).strip().title()

def _fix_count(val):
    if isinstance(val, str) and '-' in val and len(val) <= 3:
        return val
    if isinstance(val, datetime.datetime):
        balls   = min(val.month - 1, 3)
        strikes = min(val.day   - 1, 2)
        return f"{balls}-{strikes}"
    return str(val) if pd.notna(val) else ''

def _map_columns(df):
    """
    Remap DataFrame columns to canonical names using fuzzy alias matching.
    Returns (remapped_df, warnings_list).
    """
    warnings = []
    col_lower = {c.lower(): c for c in df.columns}
    rename_map = {}

    for canonical, aliases in COLUMN_ALIASES.items():
        # Already present with exact canonical name
        if canonical in df.columns:
            continue
        # Try aliases
        matched = None
        for alias in aliases:
            if alias in col_lower:
                matched = col_lower[alias]
                break
        if matched and matched != canonical:
            rename_map[matched] = canonical
        elif matched is None and canonical in REQUIRED_COLS:
            warnings.append(f"REQUIRED column '{canonical}' not found in file")
        elif matched is None:
            # Optional missing column -- will be handled gracefully downstream
            pass

    df = df.rename(columns=rename_map)

    if rename_map:
        mapped = [f"{v} (from '{k}')" for k, v in rename_map.items()]
        warnings.append(f"Remapped columns: {', '.join(mapped)}")

    return df, warnings

def _validate_data(df, warnings):
    """
    Run data quality checks. Returns (cleaned_df, warnings, flagged_count).
    """
    flagged = 0

    # Normalize player names
    if 'fullName' in df.columns:
        df['fullName'] = df['fullName'].apply(_normalize_name)

    # Validate date
    if 'gameDate' in df.columns:
        try:
            df['gameDate'] = pd.to_datetime(df['gameDate'], errors='coerce')
            bad_dates = df['gameDate'].isna().sum()
            if bad_dates:
                warnings.append(f"WARNING: {bad_dates} rows had unparseable dates and were dropped")
                df = df.dropna(subset=['gameDate'])
        except Exception as e:
            warnings.append(f"WARNING: Date parsing error: {e}")

    # Numeric range checks
    for col, (lo, hi) in VALID_RANGES.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].replace('-', np.nan), errors='coerce')
            out_of_range = ((df[col] < lo) | (df[col] > hi)) & df[col].notna()
            count = out_of_range.sum()
            if count:
                warnings.append(
                    f"WARNING: {count} rows have {col} outside valid range "
                    f"[{lo}, {hi}] -- values kept but flagged"
                )
                flagged += count

    # Check for excessive nulls in key metric columns
    key_metrics = ['Vel', 'Spin', 'IndVertBrk', 'HorzBrk']
    for col in key_metrics:
        if col in df.columns:
            null_pct = df[col].isna().mean() * 100
            if null_pct > 50:
                warnings.append(
                    f"WARNING: {col} is {null_pct:.0f}% null -- "
                    f"check that this column exported correctly"
                )

    # Coerce all numeric columns
    numeric_cols = list(VALID_RANGES.keys()) + ['RelX', 'RelZ', 'Extension',
                                                  'VertApprAngle', 'HorzApprAngle']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].replace('-', np.nan), errors='coerce')

    return df, warnings, flagged

# ── Main ingest ────────────────────────────────────────────────────────────────

def ingest_xlsx(path, verbose=True):
    """
    Load an XLSX game file and insert new pitches into the database.
    Returns dict with inserted, skipped, flagged, warnings, summary.
    """
    init_db()
    warnings = []

    # Read file
    try:
        df = pd.read_excel(path)
    except Exception as e:
        raise ValueError(f"Could not read XLSX file: {e}")

    if df.empty:
        raise ValueError("File is empty")

    # Map columns
    df, col_warnings = _map_columns(df)
    warnings.extend(col_warnings)

    # Check required columns
    missing_required = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing_required:
        raise ValueError(
            f"File is missing required columns: {missing_required}. "
            f"Found columns: {list(df.columns)}"
        )

    # Validate and clean data
    df, warnings, flagged = _validate_data(df, warnings)

    if df.empty:
        raise ValueError("No valid rows remaining after data validation")

    # Derived columns
    df['count_fixed'] = df['count'].apply(_fix_count) if 'count' in df.columns else ''
    df['is_whiff']    = (df['pitchResult'].isin(['Strike Swinging', 'Strikeout (Swinging)'])
                         .astype(int)) if 'pitchResult' in df.columns else 0
    df['is_called_strike'] = ((df['pitchResult'] == 'Strike Looking')
                               .astype(int)) if 'pitchResult' in df.columns else 0
    df['is_swing']    = (df['pitchOutcome'].isin(['S', 'B'])
                         .astype(int)) if 'pitchOutcome' in df.columns else 0
    df['is_strike']   = (df['pitchOutcome'].isin(['S', 'SL'])
                         .astype(int)) if 'pitchOutcome' in df.columns else 0

    # Coordinate flip for catcher's view
    df['px'] = -pd.to_numeric(df['x'], errors='coerce') if 'x' in df.columns else np.nan
    df['pz'] = (pd.to_numeric(df['y'], errors='coerce') + 2.5) if 'y' in df.columns else np.nan

    # Unique pitch ID
    if 'uniqPitchId' not in df.columns or df['uniqPitchId'].isna().all():
        pitch_num_col = 'pitchNumInGame' if 'pitchNumInGame' in df.columns else df.columns[0]
        df['uniqPitchId'] = (
            df['fullName'].astype(str) + '_' +
            df['gameDate'].astype(str) + '_' +
            df[pitch_num_col].astype(str)
        )

    # Insert into DB
    conn = get_conn()
    cur  = conn.cursor()
    inserted = 0
    skipped  = 0

    def _safe(row, col, default=None):
        val = row.get(col, default)
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return default
        return val

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
                _safe(row, 'pitcherId'),
                _safe(row, 'fullName'),
                row['gameDate'].date().isoformat() if hasattr(row['gameDate'], 'date') else str(row['gameDate']),
                str(_safe(row, 'gameId', '')),
                _safe(row, 'opponent', ''),
                _safe(row, 'team', ''),
                _safe(row, 'level', ''),
                _safe(row, 'pitchNumInGame'),
                _safe(row, 'pitchType', ''),
                _safe(row, 'pitchTypeFull', ''),
                _safe(row, 'batterHand', ''),
                _safe(row, 'count_fixed', ''),
                str(_safe(row, 'inn', '')),
                _safe(row, 'outs'),
                _safe(row, 'pitchResult', ''),
                _safe(row, 'pitchOutcome', ''),
                _safe(row, 'Vel'),
                _safe(row, 'Spin'),
                _safe(row, 'SpinEff'),
                _safe(row, 'IndVertBrk'),
                _safe(row, 'HorzBrk'),
                _safe(row, 'RelX'),
                _safe(row, 'RelZ'),
                _safe(row, 'Extension'),
                _safe(row, 'VertApprAngle'),
                _safe(row, 'HorzApprAngle'),
                _safe(row, 'px'),
                _safe(row, 'pz'),
                _safe(row, 'ExitVel'),
                _safe(row, 'LaunchAng'),
                int(_safe(row, 'is_swing', 0)),
                int(_safe(row, 'is_whiff', 0)),
                int(_safe(row, 'is_called_strike', 0)),
                int(_safe(row, 'is_strike', 0)),
                uid,
            ))
            if cur.rowcount > 0:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            skipped += 1
            if verbose:
                print(f"Row skipped: {e}")

    conn.commit()
    cur.close()
    conn.close()

    # Build per-player summary
    summary = []
    name_col = 'fullName' if 'fullName' in df.columns else None
    if name_col:
        for player, grp in df.groupby(name_col):
            games = grp['gameDate'].dt.date.nunique() if hasattr(grp['gameDate'].iloc[0], 'date') else grp['gameDate'].nunique()
            summary.append({
                "player": str(player),
                "games": int(games),
                "pitches": len(grp)
            })
        summary.sort(key=lambda x: x['player'])

    if verbose:
        print(f"Ingest complete: {inserted} inserted, {skipped} skipped, {flagged} flagged")
        for w in warnings:
            print(w)

    return {
        "inserted": inserted,
        "skipped": skipped,
        "flagged": flagged,
        "warnings": warnings,
        "summary": summary
    }

# ── Historical queries ─────────────────────────────────────────────────────────

def get_game_log(pitcher_id=None, pitcher_name=None):
    """Return per-game, per-pitch-type summary for trend charts."""
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
    """Per-pitch-type season averages up to before_date."""
    conn = get_conn()
    clauses = []; params = []

    if pitcher_id:
        clauses.append("pitcher_id = %s"); params.append(int(pitcher_id))
    elif pitcher_name:
        clauses.append("pitcher_name = %s"); params.append(pitcher_name)

    if before_date:
        clauses.append("game_date < %s")
        params.append(
            before_date.strftime('%Y-%m-%d')
            if isinstance(before_date, (datetime.date, datetime.datetime))
            else str(before_date)
        )

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


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print("Usage: python pwrx_db.py <file.xlsx> [file2.xlsx ...]")
        sys.exit(1)
    for path in sys.argv[1:]:
        result = ingest_xlsx(path, verbose=True)
        print(f"inserted={result['inserted']}, skipped={result['skipped']}, "
              f"flagged={result['flagged']}")
        for w in result['warnings']:
            print(" ", w)
