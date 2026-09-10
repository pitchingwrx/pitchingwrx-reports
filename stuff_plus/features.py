"""
Feature engineering for the internal Stuff+ model.

Takes raw pybaseball/Statcast rows (one row per pitch) and produces the model-ready
feature frame: pitch-family assignment, derived approach angles (VAA/HAA) from the
raw trajectory kinematics, per-pitcher fastball-differential ("tunneling") features,
and a pitch-classification outlier flag -- plus the three target columns the
three-stage model (swing / whiff / contact-quality) trains against.

See the plan file's "Internal Stuff+ Model" section for the architecture this
implements and why it's shaped this way (decomposition instead of one blended
run-value regression, tunneling as its own explicit term, etc).
"""
import numpy as np
import pandas as pd

# Pitch-family groupings. Cutter (FC) is grouped with fastballs -- velo-wise it sits
# much closer to a four-seam/sinker than to a true breaking ball, and public models
# generally treat it that way. FA (generic/unclassified fastball), EP (eephus),
# PO (pitchout), KN (knuckleball), UN (unknown), SC (screwball) are excluded from
# training entirely -- too rare and/or too ambiguous to model reliably.
PITCH_FAMILY = {
    'FF': 'fastball', 'SI': 'fastball', 'FC': 'fastball',
    'SL': 'breaking', 'CU': 'breaking', 'KC': 'breaking', 'ST': 'breaking', 'SV': 'breaking', 'CS': 'breaking',
    'CH': 'offspeed', 'FS': 'offspeed', 'FO': 'offspeed',
}
EXCLUDED_PITCH_TYPES = {'FA', 'EP', 'PO', 'KN', 'UN', 'SC'}

# Descriptions that represent a real swing decision. Bunt-related descriptions are
# excluded from all three stages -- bunting is a different swing-decision context
# entirely (the batter isn't trying to hit for quality of contact), so folding it
# into a "how nasty is this pitch" model would just add noise.
SWING_DESCRIPTIONS = {'foul', 'hit_into_play', 'swinging_strike', 'foul_tip', 'swinging_strike_blocked'}
TAKE_DESCRIPTIONS = {'ball', 'called_strike', 'blocked_ball', 'hit_by_pitch'}
WHIFF_DESCRIPTIONS = {'swinging_strike', 'swinging_strike_blocked'}
BUNT_DESCRIPTIONS = {'foul_bunt', 'missed_bunt', 'bunt_foul_tip'}
# automatic_ball/automatic_strike/pitchout are rare procedural edge cases (PitchCom
# issues, intentional-ball-adjacent situations) -- excluded from training, not a
# real swing-decision or shape-quality signal.
PROCEDURAL_DESCRIPTIONS = {'automatic_ball', 'automatic_strike', 'pitchout'}


def assign_pitch_family(df):
    df = df.copy()
    df['pitch_family'] = df['pitch_type'].map(PITCH_FAMILY)
    return df


def filter_trainable_rows(df):
    """Drops pitch types we don't model and procedural/bunt plate appearances that
    would add noise rather than signal to the swing/whiff/contact-quality targets."""
    df = df[~df['pitch_type'].isin(EXCLUDED_PITCH_TYPES)]
    df = df[~df['description'].isin(BUNT_DESCRIPTIONS | PROCEDURAL_DESCRIPTIONS)]
    df = df[df['pitch_family'].notna()]
    return df


def derive_approach_angles(df):
    """Vertical/horizontal approach angle at the front of the plate, derived from
    the raw trajectory kinematics (vy0/vz0/vx0 + ax/ay/az constant-acceleration
    model) rather than relying on a pre-computed column -- Statcast's public export
    doesn't include VAA/HAA directly, only the ingredients to compute it.

    Standard approach: solve the y-position equation for the time `t` at which the
    ball reaches the front of the plate (y = 17/12 ft), then evaluate the velocity
    components at that instant. VAA/HAA are the angles those velocity components
    make relative to the ball's path, in degrees -- more negative VAA means a
    steeper downward approach (harder to square up under), which is one of the
    known-important shape features most classic Stuff+ feature sets under-use.
    """
    df = df.copy()
    y0 = 50.0  # Statcast convention: initial tracked position is 50ft from plate
    yf = 17.0 / 12.0  # front of home plate, in feet

    vy0, ay = df['vy0'].to_numpy(dtype=float), df['ay'].to_numpy(dtype=float)
    vz0, az = df['vz0'].to_numpy(dtype=float), df['az'].to_numpy(dtype=float)
    vx0, ax = df['vx0'].to_numpy(dtype=float), df['ax'].to_numpy(dtype=float)

    # Solve y0 + vy0*t + 0.5*ay*t^2 = yf for the smaller positive root.
    a, b, c = 0.5 * ay, vy0, (y0 - yf)
    disc = b ** 2 - 4 * a * c
    disc = np.where(disc < 0, np.nan, disc)
    t = (-b - np.sqrt(disc)) / (2 * a)

    vy_f = vy0 + ay * t
    vz_f = vz0 + az * t
    vx_f = vx0 + ax * t

    # Community sign convention: negative VAA = descending into the zone (the
    # normal case), more negative = steeper. Positive would mean rising, which
    # never really happens for a real pitch at the plate.
    df['vaa'] = np.degrees(np.arctan2(vz_f, -vy_f))
    df['haa'] = np.degrees(np.arctan2(vx_f, -vy_f))
    return df


def compute_pitcher_fastball_reference(df):
    """Per pitcher (within the season(s) covered by `df`), the average shape of
    their own fastball-family pitches -- the reference every OTHER pitch's
    tunneling differential gets measured against. A pitcher with more than one
    fastball-family offering (e.g. both a four-seam and a sinker) gets one
    combined reference; a future refinement could split by primary/secondary
    fastball, but a single reference is a reasonable v1 approximation of "the
    shape a hitter is keyed up to see out of this pitcher's hand."
    """
    fb = df[df['pitch_family'] == 'fastball']
    ref = fb.groupby('pitcher').agg(
        fb_velo=('release_speed', 'mean'),
        fb_pfx_x=('pfx_x', 'mean'),
        fb_pfx_z=('pfx_z', 'mean'),
        fb_rel_x=('release_pos_x', 'mean'),
        fb_rel_z=('release_pos_z', 'mean'),
        fb_extension=('release_extension', 'mean'),
    ).reset_index()
    return ref


def compute_tunneling_features(df, fb_ref):
    """Joins each pitch to its own pitcher's fastball reference and computes the
    differential features -- kept as their own explicit columns (not silently
    folded into the raw shape features) so the model's use of tunneling is
    auditable rather than buried in tree splits. A pitch's velocity/movement
    differential vs. the pitcher's own fastball is a much more meaningful signal
    than its raw velocity/movement in isolation -- a low-80s slider off a mid-90s
    fastball plays very differently than the same slider off an 88mph fastball.
    """
    df = df.merge(fb_ref, on='pitcher', how='left')
    df['velo_diff_vs_fb'] = df['release_speed'] - df['fb_velo']
    df['pfx_x_diff_vs_fb'] = df['pfx_x'] - df['fb_pfx_x']
    df['pfx_z_diff_vs_fb'] = df['pfx_z'] - df['fb_pfx_z']
    df['release_point_diff_vs_fb'] = np.sqrt(
        (df['release_pos_x'] - df['fb_rel_x']) ** 2 + (df['release_pos_z'] - df['fb_rel_z']) ** 2
    )
    df['extension_diff_vs_fb'] = df['release_extension'] - df['fb_extension']
    return df


def flag_classification_outliers(df, z_thresh=3.5):
    """Cheap guard against a mislabeled pitch getting scored by the wrong
    per-family model: for each labeled pitch_type, flag rows whose velocity or
    movement is a statistical outlier for that specific type (e.g. a "slider"
    thrown at 96mph with fastball-shaped movement is far more likely a
    mislabeled cutter/fastball than a real 96mph slider). Flagged rows are kept
    in the output (informational) but should be down-weighted or excluded at
    training time -- see train.py.
    """
    df = df.copy()
    df['pitch_type_outlier'] = False
    for pt, grp in df.groupby('pitch_type'):
        cols = ['release_speed', 'pfx_x', 'pfx_z']
        mu = grp[cols].mean()
        sd = grp[cols].std().replace(0, np.nan)
        z = ((grp[cols] - mu) / sd).abs()
        outlier_mask = (z > z_thresh).any(axis=1)
        df.loc[grp.index[outlier_mask.to_numpy()], 'pitch_type_outlier'] = True
    return df


def derive_targets(df):
    """Builds the three target columns the decomposed model trains against:
    - is_swing: stage 1 target (swing decision), defined for every row
    - is_whiff: stage 2 target (whiff given swing), only meaningful where is_swing==1
    - contact_value: stage 3 target (expected value given contact), only meaningful
      where the pitch was put in play -- uses Statcast's own estimated_woba_using_
      speedangle where available (their own contact-quality estimate from exit
      velo/launch angle), falling back to actual woba_value on the rare rows where
      the estimate is missing.
    """
    df = df.copy()
    df['is_swing'] = df['description'].isin(SWING_DESCRIPTIONS).astype(int)
    df['is_whiff'] = np.where(df['is_swing'] == 1, df['description'].isin(WHIFF_DESCRIPTIONS).astype(int), np.nan)
    in_play = df['description'] == 'hit_into_play'
    df['contact_value'] = np.nan
    df.loc[in_play, 'contact_value'] = df.loc[in_play, 'estimated_woba_using_speedangle'].fillna(df.loc[in_play, 'woba_value'])
    return df


# The feature columns the trained model actually consumes -- kept as one explicit
# list (rather than "everything in the dataframe") so train.py and score.py can
# never silently drift apart on what the model expects.
SHAPE_FEATURES = [
    # Deliberately excludes Statcast's `spin_axis` (clock-face spin direction in
    # degrees) even though it has real predictive value -- TruMedia's canonical
    # schema (the data source the minor-league/bring-your-own-data scoring tool
    # depends on) has no equivalent field, only spin efficiency (%), a different
    # concept. A feature only half the model's real use cases can supply isn't
    # usable here -- cross-vendor consistency matters more than the marginal
    # accuracy spin_axis would add. `pfx_x`/`pfx_z` (raw movement) already carry
    # most of the same signal spin_axis would contribute anyway.
    'release_speed', 'release_spin_rate', 'pfx_x', 'pfx_z',
    'release_extension', 'release_pos_x', 'release_pos_z', 'vaa', 'haa',
]
TUNNELING_FEATURES = [
    'velo_diff_vs_fb', 'pfx_x_diff_vs_fb', 'pfx_z_diff_vs_fb',
    'release_point_diff_vs_fb', 'extension_diff_vs_fb',
]
CONTEXT_FEATURES = ['balls', 'strikes', 'stand', 'p_throws']  # stage 1/2 only, not stage 3
ALL_MODEL_FEATURES = SHAPE_FEATURES + TUNNELING_FEATURES


def build_feature_frame(raw_df):
    """Orchestrates the full pipeline: family assignment -> row filtering ->
    approach-angle derivation -> tunneling differential -> outlier flagging ->
    targets. Returns a single clean dataframe ready for train.py or score.py."""
    df = assign_pitch_family(raw_df)
    df = filter_trainable_rows(df)
    df = derive_approach_angles(df)
    fb_ref = compute_pitcher_fastball_reference(df)
    df = compute_tunneling_features(df, fb_ref)
    df = flag_classification_outliers(df)
    df = derive_targets(df)
    return df
