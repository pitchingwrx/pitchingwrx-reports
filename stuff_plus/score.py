"""
Scores pitches against the trained MLB Stuff+ model. Two entry points:

- score_internal_pitcher(): looks a pitcher up in the existing `pitches` table
  (already populated by the existing TruMedia /ingest flow) -- the path used for
  the org's own minor-league arms.
- score_uploaded_pitches(): scores a directly-supplied dataframe of pitch rows in
  TruMedia's canonical column shape -- the path a licensee's "bring your own
  data" feature would eventually call. Never returns or exposes the model
  itself, only the resulting scores -- see the plan's licensing section for why
  that boundary matters.

IMPORTANT CAVEAT, not yet verified against a real TruMedia export: the column
mapping below (units, sign conventions for HorzBrk/RelX/HorzApprAngle) is a
best-effort reading of the column names and industry-standard convention. It
has NOT been checked against real TruMedia sample values, because none were
available while building this. Before trusting scores from real TruMedia data,
run a handful of known pitches through and sanity-check the resulting features
land in plausible ranges (see `sanity_check_mapped_features` below) -- treat
early scores as provisional until that check has actually been done.
"""
import os
import json
import numpy as np
import pandas as pd
import joblib

from stuff_plus import features as feat

MODEL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'models')
FAMILIES = ['fastball', 'breaking', 'offspeed']
STAGES = ['swing', 'whiff', 'contact', 'run_value']

# TruMedia pitch_type strings -> the same family buckets features.py uses for
# Statcast's pitch_type codes. TruMedia's `pitchType`/`pitchTypeFull` values
# were not directly confirmed against a real export -- this list covers the
# common industry-standard abbreviations/names and should be extended if a
# real TruMedia file surfaces something not covered here.
TRUMEDIA_PITCH_FAMILY = {
    'FF': 'fastball', 'FA': 'fastball', 'SI': 'fastball', 'FT': 'fastball', 'FC': 'fastball',
    'SL': 'breaking', 'CU': 'breaking', 'KC': 'breaking', 'ST': 'breaking', 'SV': 'breaking', 'CB': 'breaking',
    'CH': 'offspeed', 'FS': 'offspeed', 'SP': 'offspeed', 'FO': 'offspeed',
}


class ModelBundle:
    """Loads every trained model + the fixed baseline + the feature schema once,
    reused across scoring calls instead of re-reading from disk every request."""
    _instance = None

    def __init__(self):
        self.models = {}
        for fam in FAMILIES:
            self.models[fam] = {
                stage: joblib.load(os.path.join(MODEL_DIR, f'{fam}_{stage}.joblib'))
                for stage in STAGES
            }
        with open(os.path.join(MODEL_DIR, 'baseline.json')) as f:
            self.baseline = json.load(f)
        with open(os.path.join(MODEL_DIR, 'feature_schema.json')) as f:
            schema = json.load(f)
        self.shape_features = schema['shape_features']
        self.context_features = schema['context_features']
        # The MLB training distribution's own feature ranges (5th/95th percentile
        # per family) -- computed once at train time would be cleaner, but for
        # now derived lazily the first time it's needed from a stored reference
        # file if present; falls back to None (diagnostic skipped) if missing.
        ref_path = os.path.join(MODEL_DIR, 'feature_distribution.json')
        self.feature_distribution = None
        if os.path.exists(ref_path):
            with open(ref_path) as f:
                self.feature_distribution = json.load(f)

    @classmethod
    def get(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance


def map_trumedia_columns(df, pitcher_throws):
    """Maps the `pitches` table's real Postgres column names (see pwrx_db.py's
    CREATE TABLE -- snake_case, e.g. `vel`/`ivb`/`hb`/`rel_x`, not the camelCase
    canonical names `ingest_xlsx()` uses in memory during ingestion) onto the
    exact feature names the trained model expects. Also accepts a raw upload
    dataframe straight out of `ingest_xlsx`'s in-memory shape (camelCase) by
    checking for either column naming, so this works for both the internal
    DB-lookup path and a future direct-upload path without duplicating logic.

    `pitcher_throws` ('R'/'L') is passed in explicitly rather than read from the
    file -- there's no per-pitch pitcher-handedness column in either shape, and
    a single scoring request is always about one specific pitcher whose
    throwing hand is a single known fact (already on file for the org's own
    athletes via their `primaryPosition`, or a simple required input for an
    external upload).
    """
    def col(snake, camel):
        return df[snake] if snake in df.columns else df[camel]

    out = pd.DataFrame()
    out['release_speed'] = col('vel', 'Vel')
    out['release_spin_rate'] = col('spin', 'Spin')
    # Induced vertical / horizontal break come in as TruMedia-standard inches;
    # Statcast's pfx_x/pfx_z (what the model was trained on) are in feet.
    out['pfx_z'] = col('ivb', 'IndVertBrk') / 12.0
    out['pfx_x'] = col('hb', 'HorzBrk') / 12.0
    out['release_extension'] = col('extension', 'Extension')
    out['release_pos_x'] = col('rel_x', 'RelX')
    out['release_pos_z'] = col('rel_z', 'RelZ')
    out['vaa'] = col('vert_appr_angle', 'VertApprAngle')
    out['haa'] = col('horz_appr_angle', 'HorzApprAngle')
    out['pitch_type'] = col('pitch_type', 'pitchType')
    out['pitch_family'] = out['pitch_type'].map(TRUMEDIA_PITCH_FAMILY)
    out['stand'] = col('batter_hand', 'batterHand')
    out['p_throws'] = pitcher_throws
    out['same_handed'] = (out['stand'] == out['p_throws']).astype(int)
    count_col = 'count_str' if 'count_str' in df.columns else ('count' if 'count' in df.columns else None)
    if count_col:
        parsed = df[count_col].astype(str).str.split('-', expand=True)
        out['balls'] = pd.to_numeric(parsed[0], errors='coerce').fillna(0).astype(int)
        out['strikes'] = pd.to_numeric(parsed[1], errors='coerce').fillna(0).astype(int) if parsed.shape[1] > 1 else 0
    else:
        out['balls'], out['strikes'] = 0, 0
    return out


def _compute_tunneling_for_batch(df):
    """Same tunneling-differential logic as features.py, but the fastball
    reference is computed fresh from THIS pitcher's own rows in the batch being
    scored, not the MLB-wide reference table -- scoring is always about one
    pitcher's own arsenal shape relative to their own fastball."""
    fb = df[df['pitch_family'] == 'fastball']
    if fb.empty:
        df['velo_diff_vs_fb'] = np.nan
        df['pfx_x_diff_vs_fb'] = np.nan
        df['pfx_z_diff_vs_fb'] = np.nan
        df['release_point_diff_vs_fb'] = np.nan
        df['extension_diff_vs_fb'] = np.nan
        return df
    ref = {
        'velo': fb['release_speed'].mean(), 'pfx_x': fb['pfx_x'].mean(), 'pfx_z': fb['pfx_z'].mean(),
        'rel_x': fb['release_pos_x'].mean(), 'rel_z': fb['release_pos_z'].mean(), 'ext': fb['release_extension'].mean(),
    }
    df = df.copy()
    df['velo_diff_vs_fb'] = df['release_speed'] - ref['velo']
    df['pfx_x_diff_vs_fb'] = df['pfx_x'] - ref['pfx_x']
    df['pfx_z_diff_vs_fb'] = df['pfx_z'] - ref['pfx_z']
    df['release_point_diff_vs_fb'] = np.sqrt((df['release_pos_x'] - ref['rel_x']) ** 2 + (df['release_pos_z'] - ref['rel_z']) ** 2)
    df['extension_diff_vs_fb'] = df['release_extension'] - ref['ext']
    return df


def out_of_distribution_flags(df, bundle):
    """Since this applies an MLB-trained model out of its training distribution
    to a different population, flag (per pitch) whether its raw feature values
    fall well outside the MLB training range, rather than silently trusting an
    extrapolated score. Returns a bool Series, True = worth flagging to the
    coach as "outside what the model was trained on"."""
    if bundle.feature_distribution is None:
        return pd.Series(False, index=df.index)
    flags = pd.Series(False, index=df.index)
    for fam, ranges in bundle.feature_distribution.items():
        mask = df['pitch_family'] == fam
        for feature_name, (lo, hi) in ranges.items():
            if feature_name not in df.columns:
                continue
            out_of_range = (df.loc[mask, feature_name] < lo) | (df.loc[mask, feature_name] > hi)
            flags.loc[mask] = flags.loc[mask] | out_of_range.reindex(flags.loc[mask].index).fillna(False)
    return flags


def shrink_to_prior(raw_scores, n_pitches, prior_mean, prior_strength=40):
    """Empirical-Bayes shrinkage toward the pitch-family prior mean, scaled by
    how many pitches of that type the pitcher has actually thrown -- directly
    relevant for small samples (a MiLB arm with 15 tracked sliders shouldn't be
    graded with the same confidence as one with 800). `prior_strength` is the
    number of "pseudo-pitches" of prior belief the shrinkage is worth -- higher
    means more conservative for small samples."""
    weight = n_pitches / (n_pitches + prior_strength)
    return weight * raw_scores + (1 - weight) * prior_mean


def score_pitch_family(df_fam, family, bundle):
    """Runs one pitch-family's rows through that family's 4 models, returns
    per-pitch predictions plus the raw (pre-shrinkage) Stuff+-scale score."""
    X_ab = df_fam[bundle.shape_features + bundle.context_features]
    X_c = df_fam[bundle.shape_features]

    models = bundle.models[family]
    out = df_fam.copy()
    out['p_swing'] = models['swing'].predict_proba(X_ab)[:, 1]
    out['p_whiff_given_swing'] = models['whiff'].predict_proba(X_ab)[:, 1]
    out['e_contact_value'] = models['contact'].predict(X_c)
    out['pred_run_value'] = models['run_value'].predict(X_ab)

    baseline = bundle.baseline[family]
    raw_score = -out['pred_run_value']  # lower delta_run_exp = better for the pitcher
    out['stuff_plus_raw'] = 100 + 100 * (raw_score - baseline['mean']) / baseline['sd']
    return out


def _score_mapped(mapped, bundle):
    mapped = _compute_tunneling_for_batch(mapped)
    mapped = mapped.dropna(subset=bundle.shape_features)
    if mapped.empty:
        return pd.DataFrame(), {}

    per_pitch_frames = []
    for fam in FAMILIES:
        sub = mapped[mapped['pitch_family'] == fam]
        if sub.empty:
            continue
        per_pitch_frames.append(score_pitch_family(sub, fam, bundle))
    if not per_pitch_frames:
        return pd.DataFrame(), {}
    per_pitch = pd.concat(per_pitch_frames, ignore_index=False)
    per_pitch['out_of_distribution'] = out_of_distribution_flags(per_pitch, bundle)

    summary = {}
    for pitch_type, grp in per_pitch.groupby('pitch_type'):
        fam = grp['pitch_family'].iloc[0]
        prior_mean = 100.0  # the fixed baseline is centered at 100 by construction
        shrunk = shrink_to_prior(grp['stuff_plus_raw'].mean(), len(grp), prior_mean)
        summary[pitch_type] = {
            'pitch_family': fam,
            'n_pitches': int(len(grp)),
            'raw_stuff_plus': round(float(grp['stuff_plus_raw'].mean()), 1),
            'shrunk_stuff_plus': round(float(shrunk), 1),
            'whiff_rate_pred': round(float(grp['p_whiff_given_swing'].mean()), 3),
            'pct_flagged_out_of_distribution': round(float(grp['out_of_distribution'].mean()), 3),
        }
    return per_pitch, summary


def score_uploaded_pitches(df_raw, pitcher_throws):
    """Entry point for a directly-uploaded pitch file (TruMedia canonical shape).
    Never returns model internals -- only per-pitch predictions and the
    per-pitch-type summary."""
    bundle = ModelBundle.get()
    mapped = map_trumedia_columns(df_raw, pitcher_throws)
    per_pitch, summary = _score_mapped(mapped, bundle)
    return summary


def score_internal_pitcher(pitcher_name, pitcher_throws):
    """Entry point for the internal path: looks a pitcher's rows up in the
    existing `pitches` table via the same psycopg2/pandas pattern the rest of
    this service already uses (see /roster, /player_games in main.py) -- no new
    upload needed for the org's own athletes, since their data already landed
    here via the existing TruMedia /ingest flow."""
    from pwrx_db import get_conn
    conn = get_conn()
    df_raw = pd.read_sql(
        "SELECT * FROM pitches WHERE pitcher_name = %s",
        conn, params=[pitcher_name]
    )
    conn.close()
    if df_raw.empty:
        return {}
    bundle = ModelBundle.get()
    mapped = map_trumedia_columns(df_raw, pitcher_throws)
    per_pitch, summary = _score_mapped(mapped, bundle)
    return summary
