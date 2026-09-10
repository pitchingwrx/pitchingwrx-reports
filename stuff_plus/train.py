"""
Trains the internal Stuff+ model: three interpretable diagnostic stages (swing
decision / whiff-given-swing / contact-value-given-contact) plus one primary
run-value model that actually drives the headline Stuff+ number.

Why a 4th "primary" model instead of algebraically combining stages A/B/C:
chaining swing/whiff/contact probabilities into one expected value requires
guessing at foul-ball value, HBP value, etc. -- constants that are easy to get
subtly wrong. Statcast already publishes `delta_run_exp`, the actual real-world
change in run expectancy for every single pitch (verified sign convention: more
NEGATIVE = better for the pitcher, e.g. -0.111 for a swinging strike vs. +0.059
for a ball). Model D predicts that directly from pitch shape, so the headline
number is grounded in Statcast's own already-correct run-value accounting
rather than a hand-assembled value tree. Stages A/B/C stay in the model as
genuine diagnostics -- they explain *why* a pitch is graded well or poorly
(swing-inducing? swing-and-miss? weak contact?) -- but Model D is what the
100-scale Stuff+ number is actually built from.

Trains on 2023+2024, holds out 2025 as a genuine out-of-sample calibration
check (see calibration_report() at the bottom).
"""
import sys, os, json
import numpy as np
import pandas as pd
import xgboost as xgb
import joblib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from stuff_plus import features

DATA_DIR = os.environ.get('STUFF_PLUS_DATA_DIR', '../stuff_plus_data')
MODEL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'models')

FAMILIES = ['fastball', 'breaking', 'offspeed']
# balls/strikes are numeric already; stand/p_throws are recoded to a single
# "same_handed" flag (batter/pitcher same side) -- the platoon-independent
# absolute handedness doesn't matter to a shape model, the *matchup* does.
CONTEXT_ENCODED = ['balls', 'strikes', 'same_handed']


def encode_context(df):
    df = df.copy()
    df['same_handed'] = (df['stand'] == df['p_throws']).astype(int)
    return df


def recency_weights(dates, half_life_days=548):
    """Exponential decay by days-before-the-most-recent-date-in-the-training-set
    (~1.5-year half-life) -- a lightweight mitigation of target non-stationarity
    (the league adapts to shapes over time) without needing a full rolling-refit
    pipeline. Recent pitches count more, older ones aren't thrown out entirely."""
    max_d = dates.max()
    age_days = (max_d - dates).dt.days.clip(lower=0)
    return 0.5 ** (age_days / half_life_days)


def load_training_data(seasons=('2023', '2024', '2025')):
    frames = []
    for yr in seasons:
        path = os.path.join(DATA_DIR, f'statcast_{yr}.parquet')
        df = pd.read_parquet(path)
        df['season'] = yr
        frames.append(df)
    raw = pd.concat(frames, ignore_index=True)
    df = features.build_feature_frame(raw)
    df = encode_context(df)
    df['game_date'] = pd.to_datetime(df['game_date'])
    # Down-weight (not drop) flagged pitch-classification outliers rather than
    # excluding them outright -- they're still real pitches, just less trustworthy
    # signal for what a "clean" example of that pitch type looks like.
    df['outlier_weight'] = np.where(df['pitch_type_outlier'], 0.25, 1.0)
    return df


def train_family_models(df_train, family, feature_cols):
    fam = df_train[df_train['pitch_family'] == family]
    w = recency_weights(fam['game_date']) * fam['outlier_weight']

    models = {}

    # Model A: swing decision (all rows)
    Xa = fam[feature_cols + CONTEXT_ENCODED]
    ya = fam['is_swing']
    ma = xgb.XGBClassifier(n_estimators=300, max_depth=4, learning_rate=0.05, subsample=0.8, colsample_bytree=0.8)
    ma.fit(Xa, ya, sample_weight=w)
    models['swing'] = ma

    # Model B: whiff given swing
    swung = fam[fam['is_swing'] == 1]
    ws = recency_weights(swung['game_date']) * swung['outlier_weight']
    Xb = swung[feature_cols + CONTEXT_ENCODED]
    yb = swung['is_whiff']
    mb = xgb.XGBClassifier(n_estimators=300, max_depth=4, learning_rate=0.05, subsample=0.8, colsample_bytree=0.8)
    mb.fit(Xb, yb, sample_weight=ws)
    models['whiff'] = mb

    # Model C: contact value given contact (shape features only, no count context
    # -- see features.py's note on CONTEXT_FEATURES being stage 1/2 only)
    contacted = fam[fam['contact_value'].notna()]
    wc = recency_weights(contacted['game_date']) * contacted['outlier_weight']
    Xc = contacted[feature_cols]
    yc = contacted['contact_value']
    mc = xgb.XGBRegressor(n_estimators=300, max_depth=4, learning_rate=0.05, subsample=0.8, colsample_bytree=0.8)
    mc.fit(Xc, yc, sample_weight=wc)
    models['contact'] = mc

    # Model D: primary run-value driver (all rows, real Statcast delta_run_exp)
    valid_rv = fam[fam['delta_run_exp'].notna()]
    wd = recency_weights(valid_rv['game_date']) * valid_rv['outlier_weight']
    Xd = valid_rv[feature_cols + CONTEXT_ENCODED]
    yd = valid_rv['delta_run_exp'].astype(float)
    md = xgb.XGBRegressor(n_estimators=400, max_depth=4, learning_rate=0.04, subsample=0.8, colsample_bytree=0.8)
    md.fit(Xd, yd, sample_weight=wd)
    models['run_value'] = md

    return models


def compute_fixed_baseline(df_train, family_models, feature_cols):
    """The 100-scale normalization anchor -- computed ONCE from the full training
    window (2023-2025) and never recomputed on new data, so a 110 means the same
    thing next season that it means today (this was one of the explicit critiques
    of public models: re-normalizing every season lets the scale drift as the
    league's overall stuff level changes)."""
    baseline = {}
    for fam in FAMILIES:
        sub = df_train[df_train['pitch_family'] == fam]
        X = sub[feature_cols + CONTEXT_ENCODED]
        pred_rv = family_models[fam]['run_value'].predict(X)
        raw_score = -pred_rv  # flip sign: lower (more negative) delta_run_exp = better pitch
        baseline[fam] = {'mean': float(np.mean(raw_score)), 'sd': float(np.std(raw_score))}
    return baseline


def calibration_report(df_holdout, family_models, feature_cols):
    """Genuine out-of-sample check: 2025 was never seen during training. Reports
    whiff-rate calibration (predicted decile vs actual) per family -- the most
    interpretable sanity check for whether the model generalizes."""
    report = {}
    for fam in FAMILIES:
        sub = df_holdout[(df_holdout['pitch_family'] == fam) & (df_holdout['is_swing'] == 1)]
        X = sub[feature_cols + CONTEXT_ENCODED]
        pred = family_models[fam]['whiff'].predict_proba(X)[:, 1]
        sub = sub.copy()
        sub['pred_whiff'] = pred
        sub['decile'] = pd.qcut(sub['pred_whiff'], 10, duplicates='drop', labels=False)
        cal = sub.groupby('decile').agg(predicted=('pred_whiff', 'mean'), actual=('is_whiff', 'mean'), n=('is_whiff', 'size'))
        report[fam] = cal.to_dict('index')
    return report


def compute_feature_distribution(df_train, feature_cols, lo_pct=1, hi_pct=99):
    """The MLB training distribution's own per-family feature ranges (1st/99th
    percentile) -- saved so score.py can flag a scored pitcher's pitches that
    fall well outside what the model was actually trained on (relevant for the
    minor-league/external-upload scoring path, which applies this model out of
    its training distribution by design)."""
    dist = {}
    for fam in FAMILIES:
        sub = df_train[df_train['pitch_family'] == fam]
        dist[fam] = {
            col: [float(sub[col].quantile(lo_pct / 100)), float(sub[col].quantile(hi_pct / 100))]
            for col in feature_cols
        }
    return dist


def main():
    os.makedirs(MODEL_DIR, exist_ok=True)
    feature_cols = features.ALL_MODEL_FEATURES

    print('Loading + engineering features for all 3 seasons...')
    df = load_training_data()
    df = df.dropna(subset=feature_cols)
    print(f'  {len(df):,} trainable rows after dropping feature nulls')

    df_train = df[df['season'].isin(['2023', '2024'])]
    df_holdout = df[df['season'] == '2025']
    print(f'  train: {len(df_train):,} (2023-2024)  |  holdout: {len(df_holdout):,} (2025, never trained on)')

    all_models = {}
    for fam in FAMILIES:
        print(f'Training {fam} models...')
        all_models[fam] = train_family_models(df_train, fam, feature_cols)

    print('Computing fixed normalization baseline (2023-2024 training window)...')
    baseline = compute_fixed_baseline(df_train, all_models, feature_cols)
    print(json.dumps(baseline, indent=2))

    print('Calibration check against 2025 holdout (never seen in training)...')
    cal = calibration_report(df_holdout, all_models, feature_cols)
    for fam, rows in cal.items():
        print(f'  {fam}:')
        for decile, stats in sorted(rows.items()):
            print(f"    decile {decile}: predicted={stats['predicted']:.3f} actual={stats['actual']:.3f} n={stats['n']}")

    print('Computing MLB training feature distribution (for the out-of-distribution diagnostic)...')
    feature_dist = compute_feature_distribution(df_train, feature_cols)

    print('Saving model artifacts...')
    for fam in FAMILIES:
        for stage, model in all_models[fam].items():
            joblib.dump(model, os.path.join(MODEL_DIR, f'{fam}_{stage}.joblib'))
    with open(os.path.join(MODEL_DIR, 'baseline.json'), 'w') as f:
        json.dump(baseline, f, indent=2)
    with open(os.path.join(MODEL_DIR, 'feature_distribution.json'), 'w') as f:
        json.dump(feature_dist, f, indent=2)
    with open(os.path.join(MODEL_DIR, 'feature_schema.json'), 'w') as f:
        json.dump({'shape_features': feature_cols, 'context_features': CONTEXT_ENCODED}, f, indent=2)
    print(f'Done. Models + baseline saved to {MODEL_DIR}')


if __name__ == '__main__':
    main()
