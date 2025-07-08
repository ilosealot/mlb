import argparse
import pandas as pd
import numpy as np
import unicodedata
import yaml
import os

# === PARK FACTORS AND TEAM/PARK MAPPING ===
PARK_FACTORS_2025 = {
    'Chase Field': {'runs': 1.04, 'hr': 1.08, 'woba': 1.03},
    'Truist Park': {'runs': 1.06, 'hr': 1.09, 'woba': 1.06},
    'Oriole Park at Camden Yards': {'runs': 0.94, 'hr': 0.91, 'woba': 0.95},
    'Fenway Park': {'runs': 1.10, 'hr': 0.90, 'woba': 1.08},
    'Guaranteed Rate Field': {'runs': 1.07, 'hr': 1.16, 'woba': 1.07},
    'Wrigley Field': {'runs': 1.07, 'hr': 1.16, 'woba': 1.07},
    'Great American Ball Park': {'runs': 1.13, 'hr': 1.36, 'woba': 1.13},
    'Progressive Field': {'runs': 0.95, 'hr': 0.83, 'woba': 0.94},
    'Coors Field': {'runs': 1.28, 'hr': 1.35, 'woba': 1.15},
    'Comerica Park': {'runs': 0.93, 'hr': 0.80, 'woba': 0.93},
    'Minute Maid Park': {'runs': 0.98, 'hr': 1.09, 'woba': 0.99},
    'Kauffman Stadium': {'runs': 1.07, 'hr': 0.92, 'woba': 1.03},
    'Angel Stadium': {'runs': 0.95, 'hr': 1.07, 'woba': 0.96},
    'Dodger Stadium': {'runs': 1.01, 'hr': 1.19, 'woba': 1.01},
    'loanDepot park': {'runs': 1.12, 'hr': 1.12, 'woba': 1.08},
    'American Family Field': {'runs': 1.09, 'hr': 1.22, 'woba': 1.08},
    'Target Field': {'runs': 0.99, 'hr': 1.06, 'woba': 0.98},
    'Citi Field': {'runs': 0.91, 'hr': 0.93, 'woba': 0.91},
    'Yankee Stadium': {'runs': 1.06, 'hr': 1.23, 'woba': 1.07},
    'Oakland Coliseum': {'runs': 0.87, 'hr': 0.74, 'woba': 0.87},
    'Citizens Bank Park': {'runs': 1.10, 'hr': 1.22, 'woba': 1.09},
    'PNC Park': {'runs': 1.05, 'hr': 0.94, 'woba': 1.03},
    'Petco Park': {'runs': 0.82, 'hr': 0.90, 'woba': 0.85},
    'Oracle Park': {'runs': 0.93, 'hr': 0.77, 'woba': 0.90},
    'T-Mobile Park': {'runs': 0.80, 'hr': 0.88, 'woba': 0.81},
    'Busch Stadium': {'runs': 0.95, 'hr': 0.85, 'woba': 0.95},
    'Steinbrenner Field': {'runs': 1.10, 'hr': 1.13, 'woba': 1.09},
    'Sutter Health Park': {'runs': 1.02, 'hr': 0.95, 'woba': 1.01},
    'Globe Life Field': {'runs': 0.97, 'hr': 0.95, 'woba': 0.97},
    'Rogers Centre': {'runs': 1.05, 'hr': 1.14, 'woba': 1.05},
    'Nationals Park': {'runs': 1.01, 'hr': 1.08, 'woba': 1.01},
}

TEAM_TO_PARK = {
    'ARI': 'Chase Field','ATL': 'Truist Park','BAL': 'Oriole Park at Camden Yards','BOS': 'Fenway Park','CHC': 'Wrigley Field','CWS': 'Guaranteed Rate Field','CHW': 'Guaranteed Rate Field','CIN': 'Great American Ball Park','CLE': 'Progressive Field','COL': 'Coors Field','DET': 'Comerica Park','HOU': 'Minute Maid Park','KC': 'Kauffman Stadium','KCR': 'Kauffman Stadium','LAA': 'Angel Stadium','LAD': 'Dodger Stadium','MIA': 'loanDepot park','MIL': 'American Family Field','MIN': 'Target Field','NYM': 'Citi Field','NYY': 'Yankee Stadium','OAK': 'Sutter Health Park','PHI': 'Citizens Bank Park','PIT': 'PNC Park','SD': 'Petco Park','SDP': 'San Diego Padres','SF': 'Oracle Park','SFG': 'Oracle Park','SEA': 'T-Mobile Park','STL': 'Busch Stadium','TB': 'Steinbrenner Field','TBR': 'Steinbrenner Field','TEX': 'Globe Life Field','TOR': 'Rogers Centre','WSH': 'Nationals Park','WAS': 'Nationals Park',
}

TEAM_ABBR_TO_NAME = {
    'ARI': 'Arizona Diamondbacks','ATL': 'Atlanta Braves','BAL': 'Baltimore Orioles','BOS': 'Boston Red Sox','CHC': 'Chicago Cubs','CWS': 'Chicago White Sox','CHW': 'Chicago White Sox','CIN': 'Cincinnati Reds','CLE': 'Cleveland Guardians','COL': 'Colorado Rockies','DET': 'Detroit Tigers','HOU': 'Houston Astros','KC': 'Kansas City Royals','KCR': 'Kansas City Royals','LAA': 'Los Angeles Angels','LAD': 'Los Angeles Dodgers','MIA': 'Miami Marlins','MIL': 'Milwaukee Brewers','MIN': 'Minnesota Twins','NYM': 'New York Mets','NYY': 'New York Yankees','OAK': 'Oakland Athletics','PHI': 'Philadelphia Phillies','PIT': 'Pittsburgh Pirates','SD': 'San Diego Padres','SDP': 'San Diego Padres','SF': 'San Francisco Giants','SFG': 'San Francisco Giants','SEA': 'Seattle Mariners','STL': 'St. Louis Cardinals','TB': 'Tampa Bay Rays','TBR': 'Tampa Bay Rays','TEX': 'Texas Rangers','TOR': 'Toronto Blue Jays','WSH': 'Washington Nationals','WAS': 'Washington Nationals',
}

# === CSV LOADING ===
csv_files = {}

classic_csvs = {
    "cum_pitching": "Player_Cumulative_Pitching.cvs",
    "batting_against": "Player_Batting_Against.cvs",
    "team_batting_against": "Team_Batting_Against.cvs",
    "relief_pitching": "Player_Relief_Pitching.cvs",
    "pitching_pitches": "Player_Pitching_Pitches.cvs.txt",
    "team_pitching_pitches": "Team_Pitching_Pitches.cvs",
    "team_relievers": "Team_Relief_Pitching.cvs",
    "starting_pitching": "Player_Starting_Pitching.cvs",
    "team_starting_pitching": "Team_Starting_Pitching.cvs",
    "baserunning_situ": "Player_Baserunning_Situ.cvs",
    "team_baserunning_situ": "Team_Baserunning_Situ.cvs",
    "pitching_ratios": "Player_Pitching_Ratios.cvs",
    "team_pitching_ratios": "Team_Pitching_Ratios.cvs",
    "adv_pitching": "Player_Advanced_Pitching.cvs",
    "std_pitching": "Player_Standard_Pitching.csv",
    "team_std_pitching": "Team_Standard_Pitching.cvs",
    "pitcher_splits_lhb": "pitcher_splits_2025_vsLHB.csv",
    "pitcher_splits_rhb": "pitcher_splits_2025_vsRHB.csv",
    "home_awy_pitcher": "home_awy_pitcher.cvs",
    "battingagaisnthomeaway": "battingagaisnthomeaway.cvs",
    "traditionalpitchingvsRHB": "traditionalpitchingvsRHB.cvs",
    "battingagaisntvsRHB": "battingagaisntvsRHB.cvs",
    "pitchingandbattingvsRHB": "pitchingandbattingvsRHB.cvs",
    "pitchingandbattingvsLHB": "pitchingandbattingvsLHB.cvs",
    "battingagaisntvsLHB": "battingagaisntvsLHB.cvs",
    "traditionalpitchingvsLHB": "traditionalpitchingvsLHB.cvs",
    "pitchingandbattinghomeandaway": "pitchingandbattinghomeandaway.cvs",
    "seasonstotal": "seasonstotal.cvs",
    "platoonsplitsmatchanylisted": "platoonsplitsmatchanylisted.cvs",
    "homeandawatbatter": "homeandawatbatter.cvs",
}

advanced_csvs = {
    "pitcher_arm_angles": "pitcher_arm_angles.csv",
    "active_spin": "active-spin.csv",
    "exit_velocity": "exit_velocity.csv",
    "spin_direction_pitches": "spin-direction-pitches.csv",
    "pitch_movement": "pitch_movement.csv",
    "spin_direction": "spin-direction.csv",
    "bat_tracking": "bat-tracking.csv",
    "bat_tracking_last30": "bat-tracking-last30days.csv",
    "expected_stats": "expected_stats.csv",
    "homeruns": "homeruns.csv",
    "percentile_rankings": "percentile_rankings.csv",
    "pitcher_running_game": "pitcher_running_game.csv",
    "swing_take": "swing-take.csv",
}

for key, fname in classic_csvs.items():
    if os.path.isfile(fname):
        try:
            csv_files[key] = pd.read_csv(fname)
        except Exception as e:
            print(f"Warning: Could not load {fname}: {e}")
    else:
        print(f"Warning: File {fname} not found. Skipping.")

for key, fname in advanced_csvs.items():
    if os.path.isfile(fname):
        try:
            csv_files[key] = pd.read_csv(fname)
        except Exception as e:
            print(f"Warning: Could not load {fname}: {e}")
    else:
        print(f"Warning: File {fname} not found. Skipping.")

if os.path.isfile("last3dayspitching.csv"):
    df = pd.read_csv("last3dayspitching.csv")
    df.columns = df.columns.str.strip()
    df = df[df['Date'].astype(str).str[:10].str.match(r'\d{4}-\d{2}-\d{2}', na=False)]
    df['Date'] = pd.to_datetime(df['Date'].astype(str).str[:10])
    csv_files['last3dayspitching'] = df
else:
    print("Warning: last3dayspitching.csv not found.")

# === UTILITY FUNCTIONS ===

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = name.replace('*', '').replace(',', '')
    return ''.join(
        c for c in unicodedata.normalize('NFD', name)
        if unicodedata.category(c) != 'Mn'
    ).replace('.', '').replace('-', '').replace("'", '').replace(' ', '').lower()

def all_name_variants(name):
    name = name.strip()
    parts = [p.strip() for p in name.replace(',', '').split()]
    variants = []
    if len(parts) == 2:
        first, last = parts[0], parts[1]
        variants.append(f"{first} {last}")
        variants.append(f"{last} {first}")
        variants.append(f"{last}, {first}")
    variants.append(name)
    return set(normalize_name(v) for v in variants)

def advanced_csv_lookup(player_name, df, csv_key=None, year=None):
    ADVANCED_CSV_PLAYER_COLUMNS = {
        'expected_stats': 'last_name, first_name',
        'percentile_rankings': 'player_name',
        'bat_tracking': 'name',
        'bat_tracking_last30': 'name',
        'swing_take': 'last_name, first_name',
        'pitch_movement': 'last_name, first_name',
        'pitcher_running_game': 'player_name',
        'active_spin': 'entity_name',
        'pitcher_arm_angles': 'pitcher_name',
        'exit_velocity': 'last_name, first_name',
        'spin_direction_pitches': 'last_name, first_name',
        'homeruns': 'player',
    }
    if csv_key is not None and csv_key in ADVANCED_CSV_PLAYER_COLUMNS:
        name_column = ADVANCED_CSV_PLAYER_COLUMNS[csv_key]
    else:
        name_column = 'last_name, first_name'
    if name_column not in df.columns:
        raise KeyError(f"Column '{name_column}' not found in DataFrame for {csv_key}")
    norm_variants = all_name_variants(player_name)
    for idx, row in df.iterrows():
        csv_name = row[name_column]
        norm_csv_name = normalize_name(csv_name)
        if norm_csv_name in norm_variants:
            if year is not None and 'year' in row and str(row['year']) != str(year):
                continue
            return row
    return None

def get_percentile(player_name, stat):
    stat_map = {
        'K%': 'k_percent',
        'xwOBA': 'xwoba',
        'Barrel%': 'brl_percent',
        'fb_velocity': 'fb_velocity',
        'fb_spin': 'fb_spin',
        'hard_hit_percent': 'hard_hit_percent',
        'xERA': 'xera',
    }
    df = csv_files.get('percentile_rankings')
    if df is None:
        return None
    col_name = stat_map.get(stat, stat)
    row = advanced_csv_lookup(player_name, df, csv_key='percentile_rankings')
    if row is not None and col_name in row.index:
        return row[col_name]
    return None

def get_expected_stats(player_name, year=2025):
    df = csv_files.get('expected_stats')
    if df is None:
        return {}
    row = advanced_csv_lookup(player_name, df, csv_key='expected_stats', year=year)
    if row is not None:
        return {
            'xwOBA': row.get('est_woba', None),
            'xERA': row.get('xera', None),
            'SIERA': None
        }
    return {}

def get_classic_pitcher_stats(player_name):
    df = csv_files.get('std_pitching')
    if df is None:
        return {}
    for idx, row in df.iterrows():
        if normalize_name(row.get('Player', '')) == normalize_name(player_name):
            return {
                'ERA': row.get('ERA', None),
                'WHIP': row.get('WHIP', None),
                'IP': row.get('IP', None),
                'K/9': row.get('SO9', None),
                'BB/9': row.get('BB9', None),
                'HR/9': row.get('HR9', None),
            }
    return {}
def get_pitcher_arm_angle(player_name):
    df = csv_files.get('pitcher_arm_angles')
    if df is None:
        return {}
    row = advanced_csv_lookup(player_name, df, csv_key='pitcher_arm_angles')
    if row is not None:
        return {
            'ball_angle': row.get('ball_angle', None),
            'release_ball_z': row.get('release_ball_z', None),
            'release_ball_x': row.get('relative_release_ball_x', None),
            'shoulder_z': row.get('shoulder_z', None),
            'shoulder_x': row.get('relative_shoulder_x', None),
        }
    return {}

def get_active_spin(player_name):
    df = csv_files.get('active_spin')
    if df is None:
        return {}
    row = advanced_csv_lookup(player_name, df, csv_key='active_spin')
    if row is not None:
        return {
            'active_spin_fourseam': row.get('active_spin_fourseam', None),
            'active_spin_curve': row.get('active_spin_curve', None),
            'active_spin_slider': row.get('active_spin_slider', None),
        }
    return {}

def get_running_game(player_name):
    df = csv_files.get('pitcher_running_game')
    if df is None:
        return {}
    row = advanced_csv_lookup(player_name, df, csv_key='pitcher_running_game')
    if row is not None:
        return {
            'runs_prevented_on_running_attr': row.get('runs_prevented_on_running_attr', None),
            'rate_sbx': row.get('rate_sbx', None),
            'n_sb': row.get('n_sb', None),
            'n_cs': row.get('n_cs', None),
        }
    return {}

def get_classic_hitter_stats(player_name):
    df = csv_files.get('homeandawatbatter')
    if df is None:
        return {}
    for idx, row in df.iterrows():
        if normalize_name(row.get('Player', '')) == normalize_name(player_name):
            return {
                'OPS': row.get('OPS', None),
                'HR': row.get('HR', None),
                'AVG': row.get('AVG', None),
                'OBP': row.get('OBP', None),
                'SLG': row.get('SLG', None),
            }
    return {}

def get_bat_tracking(player_name, recent=False):
    key = 'bat_tracking_last30' if recent else 'bat_tracking'
    df = csv_files.get(key)
    if df is None:
        return {}
    row = advanced_csv_lookup(player_name, df, csv_key=key)
    if row is not None:
        return {
            'swing_speed': row.get('avg_bat_speed', None),
            'attack_angle': row.get('swing_length', None),
            'bat_speed_percentile': None
        }
    return {}

def get_swing_take(player_name):
    df = csv_files.get('swing_take')
    if df is None:
        return None
    row = advanced_csv_lookup(player_name, df, csv_key='swing_take')
    if row is not None:
        return {
            'runs_all': row.get('runs_all', None),
            'runs_heart': row.get('runs_heart', None),
            'runs_shadow': row.get('runs_shadow', None),
            'runs_chase': row.get('runs_chase', None),
            'runs_waste': row.get('runs_waste', None),
        }
    return None

def get_pitch_movement(player_name, pitch_type='FF'):
    df = csv_files.get('pitch_movement')
    if df is None:
        return {}
    row = advanced_csv_lookup(player_name, df, csv_key='pitch_movement')
    if row is not None and (('pitch_type' in row and row['pitch_type'] == pitch_type) or pitch_type is None):
        return {
            'IVB': row.get('pitcher_break_z_induced', None),
            'HB': row.get('pitcher_break_x', None),
            'Usage%': row.get('pitch_per', None),
            'SpinRate': None
        }
    return {}

def get_pitcher_vs_hand_stats(pitcher_name, batter_hand):
    if batter_hand == 'L':
        df = csv_files.get('pitcher_splits_lhb')
    else:
        df = csv_files.get('pitcher_splits_rhb')
    if df is None:
        return {}
    for idx, row in df.iterrows():
        if normalize_name(row.get('Player', '')) == normalize_name(pitcher_name):
            return {
                'ERA': row.get('ERA', None),
                'WHIP': row.get('WHIP', None),
                'OPS': row.get('OPS', None),
                'K/9': row.get('SO/9', None),
                'HR/9': row.get('HR.1', None),
            }
    return {}

def platoon_matchup_analysis(pitcher_name, opp_lineup_handedness):
    lhb_count = opp_lineup_handedness.count('L')
    rhb_count = opp_lineup_handedness.count('R')
    lhb_split = get_pitcher_vs_hand_stats(pitcher_name, 'L')
    rhb_split = get_pitcher_vs_hand_stats(pitcher_name, 'R')
    lhb_ops = float(lhb_split.get('OPS', 0) or 0)
    rhb_ops = float(rhb_split.get('OPS', 0) or 0)
    total = lhb_count + rhb_count
    if total == 0:
        return "No handedness data for lineup."
    expected_ops = (lhb_count * lhb_ops + rhb_count * rhb_ops) / total
    notes = []
    if lhb_count >= 5 and lhb_ops > 0.800:
        notes.append(f"VULNERABLE: {lhb_count} LHB vs pitcher OPS {lhb_ops:.3f}")
    if rhb_count >= 5 and rhb_ops > 0.800:
        notes.append(f"VULNERABLE: {rhb_count} RHB vs pitcher OPS {rhb_ops:.3f}")
    return f"Expected lineup OPS vs this pitcher: {expected_ops:.3f}" + (" | " + "; ".join(notes) if notes else "")

# === BULLPEN AND TRIGGER LOGIC ===

def get_team_bullpen_stats(team_abbr):
    df = csv_files.get('team_relievers')
    if df is None:
        return {}
    full_team_name = TEAM_ABBR_TO_NAME.get(team_abbr, team_abbr)
    for col in ['Team', 'Tm', 'team', 'TEAM']:
        if col in df.columns:
            row = df[(df[col] == team_abbr) | (df[col] == full_team_name)]
            if not row.empty:
                row = row.iloc[0]
                return {
                    'ERA': row.get('ERA', None),
                    'WHIP': row.get('WHIP', None),
                }
    print("No valid team column found in team_relievers CSV! Columns are:", list(df.columns))
    return {}

def get_recent_relievers_for_team(team_abbr, last3days_df):
    team_df = last3days_df[last3days_df['Team'] == team_abbr].copy()
    def ip_to_float(ip):
        if pd.isna(ip): return 0.0
        ip = str(ip)
        if '.' in ip:
            whole, frac = ip.split('.')
            return float(whole) + (1 if frac == '1' else 2 if frac == '2' else 0)/3
        return float(ip)
    team_df['IP_float'] = team_df['IP'].apply(ip_to_float)
    reliever_mask = team_df.groupby('Player')['IP_float'].max() <= 2.0
    relievers = reliever_mask[reliever_mask].index.tolist()
    return relievers

def last_3_days_usage(reliever_name):
    df = csv_files['last3dayspitching']
    for player_col in ['Player', 'Name', 'player_name', 'Pitcher', 'pitcher_name']:
        if player_col in df.columns:
            break
    else:
        print("No player name column found in last3dayspitching!")
        return {'ip': 0.0, 'appearances': 0}
    mask = df[player_col].apply(normalize_name) == normalize_name(reliever_name)
    reliever_rows = df[mask]
    def ip_to_float(ip):
        if pd.isna(ip): return 0.0
        ip = str(ip)
        if '.' in ip:
            whole, frac = ip.split('.')
            return float(whole) + (1 if frac == '1' else 2 if frac == '2' else 0)/3
        return float(ip)
    total_ip = reliever_rows['IP'].apply(ip_to_float).sum()
    appearances = reliever_rows.shape[0]
    return {'ip': total_ip, 'appearances': appearances}

class BullpenAnalysis:
    def __init__(self, reliever_names, team):
        self.reliever_names = reliever_names
        self.team = team
        self.stats = []
        self.risk = False

    def analyze(self):
        tired_threshold_ip = 2.0
        tired_threshold_app = 2
        for name in self.reliever_names:
            usage = last_3_days_usage(name)
            tired = usage['ip'] > tired_threshold_ip or usage['appearances'] > tired_threshold_app
            self.stats.append({
                'name': name,
                'ip_last3': usage['ip'],
                'appearances_last3': usage['appearances'],
                'tired': tired
            })
        self.risk = any(r['tired'] for r in self.stats)

    def summary(self):
        return {
            'risk': self.risk,
            'relievers': self.stats
        }

def get_recent_team_ops(days=7):
    for key, df in csv_files.items():
        if not isinstance(df, pd.DataFrame):
            continue
        if 'Date' in df.columns and 'Team' in df.columns:
            print(f"Using {key} for recent OPS calculation.")
            try:
                df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
                cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
                recent = df[df['Date'] >= cutoff]
                if 'OBP' in recent.columns and 'SLG' in recent.columns:
                    recent['OPS'] = pd.to_numeric(recent['OBP'], errors='coerce') + pd.to_numeric(recent['SLG'], errors='coerce')
                elif {'H', 'BB', 'AB', 'TB'}.issubset(recent.columns):
                    recent['OBP'] = (pd.to_numeric(recent['H'], errors='coerce') + pd.to_numeric(recent['BB'], errors='coerce')) / (pd.to_numeric(recent['AB'], errors='coerce') + pd.to_numeric(recent['BB'], errors='coerce'))
                    recent['SLG'] = pd.to_numeric(recent['TB'], errors='coerce') / pd.to_numeric(recent['AB'], errors='coerce')
                    recent['OPS'] = recent['OBP'] + recent['SLG']
                else:
                    print(f"{key} does not have OBP/SLG or H/BB/AB/TB columns.")
                    continue
                team_ops = recent.groupby('Team')['OPS'].mean().to_dict()
                return team_ops
            except Exception as e:
                print(f"Error processing {key} for recent OPS: {e}")
    print("No suitable CSV found for recent OPS calculation.")
    return {}

def bullpen_at_risk(bullpen, team_bullpen_stats=None):
    tired_count = sum(1 for r in bullpen.stats if r.get('tired'))
    if tired_count >= 2:
        return True
    if team_bullpen_stats:
        try:
            era = float(team_bullpen_stats.get('ERA', 0))
            whip = float(team_bullpen_stats.get('WHIP', 0))
            if era > 4.50 or whip > 1.40:
                return True
        except Exception:
            pass
    return False

def is_ace(sp):
    try:
        xera = float(sp.advanced.get('xERA', 99) or 99)
        whip = float(sp.classic.get('WHIP', 99) or 99)
        k9 = float(sp.classic.get('K/9', 0) or 0)
        return (xera < 2.75 and whip < 1.05 and k9 > 10)
    except Exception:
        return False

def is_cold(team_abbr):
    global recent_ops
    ops = recent_ops.get(team_abbr)
    return ops is not None and ops < 0.700
def update_pitcher_flags_from_advanced(pitcher, adv_triggers):
    # Set vulnerable if hard hit or barrel danger trigger is True
    pitcher.is_vulnerable = bool(adv_triggers.get('hard_hit_danger') or adv_triggers.get('barrel_danger'))
    # Set auto-fade if both hard hit and barrel danger or low K% trigger is True
    pitcher.is_auto_fade = bool(
        (adv_triggers.get('hard_hit_danger') and adv_triggers.get('barrel_danger')) or
        adv_triggers.get('low_k_percentile')
    )

def print_triggers_and_bets(
    home_sp, away_sp, home_hitters, away_hitters,
    home_bullpen, away_bullpen, park_factors, home_team_abbr, away_team_abbr,
    home_sp_vulnerability_score, away_sp_vulnerability_score
):
    print("\n=== MODEL TRIGGERS & BETTING LOGIC ===")
    triggers = {}
    bets = []

    # --- Pitcher Triggers (now driven by new vulnerability scores) ---
    FADE_THRESHOLD = 2.0
    VULNERABLE_THRESHOLD = 1.0

    triggers['home_sp_auto_fade'] = home_sp_vulnerability_score >= FADE_THRESHOLD
    triggers['home_sp_vulnerable'] = home_sp_vulnerability_score >= VULNERABLE_THRESHOLD
    triggers['away_sp_auto_fade'] = away_sp_vulnerability_score >= FADE_THRESHOLD
    triggers['away_sp_vulnerable'] = away_sp_vulnerability_score >= VULNERABLE_THRESHOLD

    triggers['home_sp_elite'] = home_sp_vulnerability_score <= -1.0
    triggers['away_sp_elite'] = away_sp_vulnerability_score <= -1.0

    # --- Bullpen Analysis Triggers (Using home_bullpen.risk directly) ---
    triggers['home_bullpen_risk'] = home_bullpen.risk
    triggers['away_bullpen_risk'] = away_bullpen.risk
    triggers['bullpen_risk'] = triggers['home_bullpen_risk'] or triggers['away_bullpen_risk']

    # --- Park Factors Triggers ---
    triggers['pitchers_park'] = park_factors.get('runs', 1.0) < 0.95 and park_factors.get('hr', 1.0) < 0.95
    triggers['hitters_park'] = park_factors.get('runs', 1.0) > 1.05 and park_factors.get('hr', 1.0) > 1.05

    # --- Other Triggers ---
    triggers['dead_ball_lockout'] = False

    home_strong_hitters_count = sum(1 for hitter in home_hitters if hitter.classic.get('OPS') is not None and float(str(hitter.classic['OPS']).replace('%','').replace(',','').strip()) > 0.800)
    away_strong_hitters_count = sum(1 for hitter in away_hitters if hitter.classic.get('OPS') is not None and float(str(hitter.classic['OPS']).replace('%','').replace(',','').strip()) > 0.800)

    triggers['home_firepower'] = home_strong_hitters_count >= 3
    triggers['away_firepower'] = away_strong_hitters_count >= 3

    triggers['home_sp_velo_anomaly'] = home_sp.velo_anomaly
    triggers['away_sp_velo_anomaly'] = away_sp.velo_anomaly

    triggers['home_ace'] = triggers['home_sp_elite']
    triggers['away_ace'] = triggers['away_sp_elite']

    recent_ops = get_recent_team_ops()
    triggers['home_cold'] = recent_ops.get(home_team_abbr, 1.0) < 0.650
    triggers['away_cold'] = recent_ops.get(away_team_abbr, 1.0) < 0.650

    def avg_ops(hitters):
        ops_vals = []
        for h in hitters:
            try:
                ops = h.classic.get('OPS')
                if ops is not None:
                    ops_float = float(str(ops).replace('%','').replace(',','').strip())
                    ops_vals.append(ops_float)
            except Exception:
                continue
        return np.mean(ops_vals) if ops_vals else 0.7

    home_avg_ops = avg_ops(home_hitters)
    away_avg_ops = avg_ops(away_hitters)

    home_tired = [r['name'] for r in getattr(home_bullpen, 'stats', []) if r.get('tired')]
    away_tired = [r['name'] for r in getattr(away_bullpen, 'stats', []) if r.get('tired')]

    home_bullpen_stats = get_team_bullpen_stats(home_team_abbr)
    away_bullpen_stats = get_team_bullpen_stats(away_team_abbr)
    home_bullpen_risk = bullpen_at_risk(home_bullpen, home_bullpen_stats)
    away_bullpen_risk = bullpen_at_risk(away_bullpen, away_bullpen_stats)

    home_firepower = (
        not is_cold(home_team_abbr) and
        sum(1 for h in home_hitters if h.classic.get('OPS') and float(str(h.classic['OPS']).replace('.','0.',1)) > 0.800) >= 2
    )
    away_firepower = (
        not is_cold(away_team_abbr) and
        sum(1 for h in away_hitters if h.classic.get('OPS') and float(str(h.classic['OPS']).replace('.','0.',1)) > 0.800) >= 2
    )

    triggers = {
    "bullpen_risk": home_bullpen_risk or away_bullpen_risk,
    "home_bullpen_risk": home_bullpen_risk,
    "away_bullpen_risk": away_bullpen_risk,
    "pitchers_park": park_factors['runs'] < 0.95,
    "dead_ball_lockout": ((home_avg_ops < 0.68 or away_avg_ops < 0.68) and park_factors.get('hr',1.0) < 1.0),
    "home_sp_auto_fade": home_sp.is_auto_fade,  # CHANGED THIS LINE
    "away_sp_auto_fade": away_sp.is_auto_fade,  # CHANGED THIS LINE
    "home_sp_vulnerable": home_sp.is_vulnerable, # CHANGED THIS LINE
    "away_sp_vulnerable": away_sp.is_vulnerable, # CHANGED THIS LINE
    "home_firepower": home_firepower,
    "away_firepower": away_firepower,
    "home_sp_velo_anomaly": getattr(home_sp, 'velo_anomaly', False),
    "away_sp_velo_anomaly": getattr(away_sp, 'velo_anomaly', False),
    "home_ace": is_ace(home_sp),
    "away_ace": is_ace(away_sp),
    "home_cold": is_cold(home_team_abbr),
    "away_cold": is_cold(away_team_abbr),
}

    print("--- TRIGGERS ---")
    for k, v in triggers.items():
        print(f"{k}: {v}")

    print("\n--- BULLPEN FATIGUE (last 3 days, full game impact only) ---")
    print(f"Home bullpen tired relievers ({len(home_tired)}): {', '.join(home_tired) if home_tired else 'None'}")
    print(f"Away bullpen tired relievers ({len(away_tired)}): {', '.join(away_tired) if away_tired else 'None'}")

    print("\n--- BETS ---")
    bets_fired = []

    if home_sp.velo_anomaly and home_bullpen_risk and home_firepower and away_firepower:
        bets_fired.append(f"OVER: Home SP Velo Anomaly + Both Bullpens Tired + Both Teams Hot")

# Apply these changes:
    if home_sp.is_auto_fade and away_firepower: # Use the attribute
        bets_fired.append(f"OVER: Auto-fade Home SP {home_sp.name} vs. strong Away lineup")
    if away_sp.is_auto_fade and home_firepower: # Use the attribute
        bets_fired.append(f"OVER: Auto-fade Away SP {away_sp.name} vs. strong Home lineup")
    if home_sp.is_auto_fade and home_bullpen_risk: # Use the attribute
        bets_fired.append(f"AWAY TEAM TOTAL OVER & AWAY MONEYLINE: Fade Home SP {home_sp.name} + Tired Home Bullpen")
    if away_sp.is_auto_fade and away_bullpen_risk: # Use the attribute
        bets_fired.append(f"HOME TEAM TOTAL OVER & HOME MONEYLINE: Fade Away SP {away_sp.name} + Tired Away Bullpen")

# Keep is_ace as a global function call, as you prefer:
    if is_ace(home_sp) and is_ace(away_sp) and triggers["pitchers_park"]:
        bets_fired.append(f"UNDER: Both Aces pitching in a pitcher's park")

    if triggers["dead_ball_lockout"] and not home_firepower and not away_firepower:
        bets_fired.append(f"UNDER: Dead Ball Lockout conditions and cold offenses")
    
    home_sp_platoon_analysis = platoon_matchup_analysis(home_sp.name, away_lineup_handedness)
    if "VULNERABLE" in home_sp_platoon_analysis:
        bets_fired.append(f"AWAY TEAM TOTAL OVER / AWAY TEAM STACK: Home SP {home_sp.name} is {home_sp_platoon_analysis}")
    away_sp_platoon_analysis = platoon_matchup_analysis(away_sp.name, home_lineup_handedness)
    if "VULNERABLE" in away_sp_platoon_analysis:
        bets_fired.append(f"HOME TEAM TOTAL OVER / HOME TEAM STACK: Away SP {away_sp.name} is {away_sp_platoon_analysis}")

    if not bets_fired:
        print("No strong betting triggers identified based on current analysis.")
    else:
        for bet in bets_fired:
         print(f"- {bet}")

# === UTILITY FUNCTIONS ===

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = name.replace('*', '').replace(',', '')
    return ''.join(
        c for c in unicodedata.normalize('NFD', name)
        if unicodedata.category(c) != 'Mn'
    ).replace('.', '').replace('-', '').replace("'", '').replace(' ', '').lower()

def all_name_variants(name):
    name = name.strip()
    parts = [p.strip() for p in name.replace(',', '').split()]
    variants = []
    if len(parts) == 2:
        first, last = parts[0], parts[1]
        variants.append(f"{first} {last}")
        variants.append(f"{last} {first}")
        variants.append(f"{last}, {first}")
    variants.append(name)
    return set(normalize_name(v) for v in variants)

def advanced_csv_lookup(player_name, df, csv_key=None, year=None):
    ADVANCED_CSV_PLAYER_COLUMNS = {
        'expected_stats': 'last_name, first_name',
        'percentile_rankings': 'player_name',
        'bat_tracking': 'name',
        'bat_tracking_last30': 'name',
        'swing_take': 'last_name, first_name',
        'pitch_movement': 'last_name, first_name',
        'pitcher_running_game': 'player_name',
        'active_spin': 'entity_name',
        'pitcher_arm_angles': 'pitcher_name',
        'exit_velocity': 'last_name, first_name',
        'spin_direction_pitches': 'last_name, first_name',
        'homeruns': 'player',
    }
    if csv_key is not None and csv_key in ADVANCED_CSV_PLAYER_COLUMNS:
        name_column = ADVANCED_CSV_PLAYER_COLUMNS[csv_key]
    else:
        name_column = 'last_name, first_name'
    if name_column not in df.columns:
        raise KeyError(f"Column '{name_column}' not found in DataFrame for {csv_key}")
    norm_variants = all_name_variants(player_name)
    for idx, row in df.iterrows():
        csv_name = row[name_column]
        norm_csv_name = normalize_name(csv_name)
        if norm_csv_name in norm_variants:
            if year is not None and 'year' in row and str(row['year']) != str(year):
                continue
            return row
    return None

def get_percentile(player_name, stat):
    stat_map = {
        'K%': 'k_percent',
        'xwOBA': 'xwoba',
        'Barrel%': 'brl_percent',
        'fb_velocity': 'fb_velocity',
        'fb_spin': 'fb_spin',
        'hard_hit_percent': 'hard_hit_percent',
        'xERA': 'xera',
    }
    df = csv_files.get('percentile_rankings')
    if df is None:
        return None
    col_name = stat_map.get(stat, stat)
    row = advanced_csv_lookup(player_name, df, csv_key='percentile_rankings')
    if row is not None and col_name in row.index:
        return row[col_name]
    return None

def get_expected_stats(player_name, year=2025):
    df = csv_files.get('expected_stats')
    if df is None:
        return {}
    row = advanced_csv_lookup(player_name, df, csv_key='expected_stats', year=year)
    if row is not None:
        return {
            'xwOBA': row.get('est_woba', None),
            'xERA': row.get('xera', None),
            'SIERA': None
        }
    return {}

def get_classic_pitcher_stats(player_name):
    df = csv_files.get('std_pitching')
    if df is None:
        return {}
    for idx, row in df.iterrows():
        if normalize_name(row.get('Player', '')) == normalize_name(player_name):
            return {
                'ERA': row.get('ERA', None),
                'WHIP': row.get('WHIP', None),
                'IP': row.get('IP', None),
                'K/9': row.get('SO9', None),
                'BB/9': row.get('BB9', None),
                'HR/9': row.get('HR9', None),
            }
    return {}

def get_classic_hitter_stats(player_name):
    df = csv_files.get('homeandawatbatter')
    if df is None:
        return {}
    for idx, row in df.iterrows():
        if normalize_name(row.get('Player', '')) == normalize_name(player_name):
            return {
                'OPS': row.get('OPS', None),
                'HR': row.get('HR', None),
                'AVG': row.get('AVG', None),
                'OBP': row.get('OBP', None),
                'SLG': row.get('SLG', None),
            }
    return {}

def get_bat_tracking(player_name, recent=False):
    key = 'bat_tracking_last30' if recent else 'bat_tracking'
    df = csv_files.get(key)
    if df is None:
        return {}
    row = advanced_csv_lookup(player_name, df, csv_key=key)
    if row is not None:
        return {
            'swing_speed': row.get('avg_bat_speed', None),
            'attack_angle': row.get('swing_length', None),
            'bat_speed_percentile': None
        }
    return {}

def get_swing_take(player_name):
    df = csv_files.get('swing_take')
    if df is None:
        return None
    row = advanced_csv_lookup(player_name, df, csv_key='swing_take')
    if row is not None:
        return {
            'runs_all': row.get('runs_all', None),
            'runs_heart': row.get('runs_heart', None),
            'runs_shadow': row.get('runs_shadow', None),
            'runs_chase': row.get('runs_chase', None),
            'runs_waste': row.get('runs_waste', None),
        }
    return None

def get_pitch_movement(player_name, pitch_type='FF'):
    df = csv_files.get('pitch_movement')
    if df is None:
        return {}
    row = advanced_csv_lookup(player_name, df, csv_key='pitch_movement')
    if row is not None and (('pitch_type' in row and row['pitch_type'] == pitch_type) or pitch_type is None):
        return {
            'IVB': row.get('pitcher_break_z_induced', None),
            'HB': row.get('pitcher_break_x', None),
            'Usage%': row.get('pitch_per', None),
            'SpinRate': None
        }
    return {}

def get_pitcher_vs_hand_stats(pitcher_name, batter_hand):
    if batter_hand == 'L':
        df = csv_files.get('pitcher_splits_lhb')
    else:
        df = csv_files.get('pitcher_splits_rhb')
    if df is None:
        return {}
    for idx, row in df.iterrows():
        if normalize_name(row.get('Player', '')) == normalize_name(pitcher_name):
            return {
                'ERA': row.get('ERA', None),
                'WHIP': row.get('WHIP', None),
                'OPS': row.get('OPS', None),
                'K/9': row.get('SO/9', None),
                'HR/9': row.get('HR.1', None),
            }
    return {}

def platoon_matchup_analysis(pitcher_name, opp_lineup_handedness):
    lhb_count = opp_lineup_handedness.count('L')
    rhb_count = opp_lineup_handedness.count('R')
    lhb_split = get_pitcher_vs_hand_stats(pitcher_name, 'L')
    rhb_split = get_pitcher_vs_hand_stats(pitcher_name, 'R')
    lhb_ops = float(lhb_split.get('OPS', 0) or 0)
    rhb_ops = float(rhb_split.get('OPS', 0) or 0)
    total = lhb_count + rhb_count
    if total == 0:
        return "No handedness data for lineup."
    expected_ops = (lhb_count * lhb_ops + rhb_count * rhb_ops) / total
    notes = []
    if lhb_count >= 5 and lhb_ops > 0.800:
        notes.append(f"VULNERABLE: {lhb_count} LHB vs pitcher OPS {lhb_ops:.3f}")
    if rhb_count >= 5 and rhb_ops > 0.800:
        notes.append(f"VULNERABLE: {rhb_count} RHB vs pitcher OPS {rhb_ops:.3f}")
    return f"Expected lineup OPS vs this pitcher: {expected_ops:.3f}" + (" | " + "; ".join(notes) if notes else "")

def get_pitch_movement_details(player_name, pitch_type='FF'):
    df = csv_files.get('pitch_movement')
    if df is None:
        return {}
    row = advanced_csv_lookup(player_name, df, csv_key='pitch_movement')
    if row is not None and (('pitch_type' in row and row['pitch_type'] == pitch_type) or pitch_type is None):
        return {
            'IVB': row.get('pitcher_break_z_induced', None),
            'HB': row.get('pitcher_break_x', None),
            'Usage%': row.get('pitch_per', None),
            'SpinRate': row.get('spin_rate', None)
        }
    return {}

def get_spin_direction(player_name, pitch_type='FF'):
    df = csv_files.get('spin_direction_pitches') # Or 'spin_direction' if that's the correct key for your CSV
    if df is None:
        return {}
    row = advanced_csv_lookup(player_name, df, csv_key='spin_direction_pitches') # Or 'spin_direction'
    if row is not None and (('pitch_type' in row and row['pitch_type'] == pitch_type) or pitch_type is None):
        return {
            'spin_direction': row.get('spin_direction', None),
            'spin_axis': row.get('spin_axis', None)
        }
    return {}

def get_exit_velocity(player_name):
    df = csv_files.get('exit_velocity')
    if df is None:
        return {}
    # Assuming advanced_csv_lookup can handle 'exit_velocity' as a key
    row = advanced_csv_lookup(player_name, df, csv_key='exit_velocity')
    if row is not None:
        return {
            'avg_hit_speed': row.get('avg_hit_speed', None),
            'max_hit_speed': row.get('max_hit_speed', None),
            'brl_percent': row.get('brl_percent', None)
        }
    return {}

def check_pitcher_triggers(pitcher):
    triggers = {}
    pitcher_vulnerability_score = 0.0 # Initialize score as float

    # --- Your Existing Trigger Logic (as you provided) ---
    # Barrel% Danger: High HR risk (major penalty)
    if pitcher.percentiles.get('Barrel%', 0) >= 80:
        triggers['barrel_danger'] = True
        pitcher_vulnerability_score += 3 # Significant penalty
    elif pitcher.percentiles.get('Barrel%', 0) >= 60: # Moderate risk
        triggers['barrel_caution'] = True
        pitcher_vulnerability_score += 1

    # Hard Hit% Danger: Likely to allow damage (medium penalty)
    if pitcher.percentiles.get('Hard Hit%', 0) >= 80:
        triggers['hard_hit_danger'] = True
        pitcher_vulnerability_score += 2
    elif pitcher.percentiles.get('Hard Hit%', 0) >= 60: # Moderate risk
        triggers['hard_hit_caution'] = True
        pitcher_vulnerability_score += 0.5

    # xwOBA Red Flag: Overall vulnerability (major penalty)
    if pitcher.percentiles.get('xwOBA', 0) >= 75:
        triggers['xwoba_red_flag'] = True
        pitcher_vulnerability_score += 2.5
    elif pitcher.percentiles.get('xwOBA', 0) >= 65: # Moderate risk
        triggers['xwoba_caution'] = True
        pitcher_vulnerability_score += 1

    # Exit Velo Red Flag: At risk for hard contact (medium penalty)
    if pitcher.exit_velocity and float(pitcher.exit_velocity.get('avg_hit_speed', 0)) >= 90:
        triggers['exit_velo_red_flag'] = True
        pitcher_vulnerability_score += 1.5

    # Elite Contact Suppression: Good at limiting damage (bonus)
    if pitcher.percentiles.get('Barrel%', 100) <= 20 and pitcher.percentiles.get('Hard Hit%', 100) <= 20:
        triggers['elite_contact_suppression'] = True
        pitcher_vulnerability_score -= 2 # This is a positive for the pitcher, so subtract from vulnerability

    # HR Quality Danger (Requires 'hr_quality' attribute in PitcherAnalysis)
    if hasattr(pitcher, 'hr_quality') and pitcher.hr_quality and int(pitcher.hr_quality.get('no_doubters', 0)) > 5:
        triggers['hr_quality_danger'] = True
        pitcher_vulnerability_score += 1.5 # Moderate penalty for giving up easy HRs

    # Add other triggers you might have, like high xERA, low K%, poor platoon splits, velo anomaly, etc.
    # For example, from our previous discussions:
    if pitcher.advanced.get('xERA') is not None and pitcher.advanced.get('xERA') > 4.5:
        triggers['high_xera'] = True
        pitcher_vulnerability_score += 1.5
    if pitcher.percentiles.get('K%', 0) < 30:
        triggers['low_k_percentile'] = True
        pitcher_vulnerability_score += 1.0
    if pitcher.velo_anomaly:
        triggers['velo_anomaly_detected'] = True
        pitcher_vulnerability_score += 1.5

    # You can also add conditions for good performance to *reduce* the score
    if pitcher.percentiles.get('K%', 0) >= 80: # High strikeout pitcher (good)
        pitcher_vulnerability_score -= 1
    # --- END of Your Existing Trigger Logic ---


    # --- NEW LOGIC TO SET IS_VULNERABLE AND IS_AUTO_FADE ON PITCHERANALYSIS OBJECT ---
    # These thresholds determine when a pitcher is flagged as vulnerable or auto-fade.
    # Adjust these values based on your betting strategy.
    AUTO_FADE_SCORE_THRESHOLD = 7.0
    VULNERABLE_SCORE_THRESHOLD = 4.0

    if pitcher_vulnerability_score >= AUTO_FADE_SCORE_THRESHOLD:
        pitcher.is_auto_fade = True
        pitcher.is_vulnerable = True  # If auto-fade, it's also vulnerable
    elif pitcher_vulnerability_score >= VULNERABLE_SCORE_THRESHOLD:
        pitcher.is_vulnerable = True
    else:
        # Explicitly set to False if below thresholds, in case they were True from some other logic
        pitcher.is_auto_fade = False
        pitcher.is_vulnerable = False

    return triggers, pitcher_vulnerability_score
# === CLASS DEFINITIONS ===

class PitcherAnalysis:
    def __init__(self, name, team, opp_lineup_handedness=None):
        self.name = name
        self.team = team
        self.advanced = {}
        self.percentiles = {}
        self.pitch_movement = {}
        self.swing_take = {}
        self.metrics = {}
        self.classic = {}
        self.velo_anomaly = False
        self.season_velo = None
        self.recent_velo = None
        self.opp_lineup_handedness = opp_lineup_handedness
        self.arm_angle = {}
        self.active_spin = {}
        self.running_game = {}
        self.movement_details = {}
        self.spin_direction = {}
        # New addition for exit velocity
        self.exit_velocity = {}
        self.is_vulnerable = False  # NEW: Initialize to False
        self.is_auto_fade = False   # NEW: Initialize to False

    def detect_velocity_anomaly(self):
        df = csv_files.get('pitching_pitches')
        if df is None:
            return None, None, False
        for player_col in ['Player', 'Name', 'player_name', 'Pitcher', 'pitcher_name']:
            if player_col in df.columns:
                break
        else:
            print("No player name column found in pitching_pitches!")
            return None, None, False
        mask = df[player_col].apply(normalize_name) == normalize_name(self.name)
        player_rows = df[mask]
        if player_rows.empty:
            return None, None, False
        season_velo = player_rows[player_rows['Year'] == 2025]['FBv'].mean() if 'Year' in player_rows.columns and 'FBv' in player_rows.columns else None
        if 'Date' in player_rows.columns and 'FBv' in player_rows.columns:
            player_rows = player_rows.sort_values('Date', ascending=False)
            recent_velo = player_rows.head(3)['FBv'].mean()
        else:
            recent_velo = None
        anomaly = False
        if season_velo is not None and recent_velo is not None:
            try:
                anomaly = (season_velo - recent_velo) > 1.0
            except:
                anomaly = False
        return season_velo, recent_velo, anomaly

    def analyze(self):
        self.classic = get_classic_pitcher_stats(self.name)
        self.advanced = get_expected_stats(self.name)
        # These percentiles are already being fetched, we'll just use them in print_report
        self.percentiles['K%'] = get_percentile(self.name, 'K%')
        self.percentiles['xwOBA'] = get_percentile(self.name, 'xwOBA')
        self.percentiles['Barrel%'] = get_percentile(self.name, 'Barrel%') # This will be used for Barrel% Allowed
        self.percentiles['FB Velo%'] = get_percentile(self.name, 'fb_velocity')
        self.percentiles['FB Spin%'] = get_percentile(self.name, 'fb_spin')
        self.percentiles['Hard Hit%'] = get_percentile(self.name, 'hard_hit_percent') # This will be used for Hard Hit% Allowed
        
        self.pitch_movement = get_pitch_movement(self.name, 'FF')
        self.swing_take = get_swing_take(self.name)
        self.season_velo, self.recent_velo, self.velo_anomaly = self.detect_velocity_anomaly()
        self.arm_angle = get_pitcher_arm_angle(self.name)
        self.active_spin = get_active_spin(self.name)
        self.running_game = get_running_game(self.name)
        self.movement_details = get_pitch_movement_details(self.name)
        self.spin_direction = get_spin_direction(self.name)
        # New call to fetch exit velocity data
        self.exit_velocity = get_exit_velocity(self.name)

    def print_report(self):
        print(f"{self.name} Classic Stats:")
        print(f"  ERA: {self.classic.get('ERA')}, WHIP: {self.classic.get('WHIP')}, IP: {self.classic.get('IP')}, K/9: {self.classic.get('K/9')}, BB/9: {self.classic.get('BB/9')}, HR/9: {self.classic.get('HR/9')}")
        print(f"{self.name} Advanced Metrics:")
        print(f"  xwOBA: {self.advanced.get('xwOBA')}, xERA: {self.advanced.get('xERA')}, SIERA: {self.advanced.get('SIERA')}")
        print(f"  Percentiles: K% {self.percentiles['K%']}, xwOBA {self.percentiles['xwOBA']}, Barrel% {self.percentiles['Barrel%']}, FB Velo% {self.percentiles['FB Velo%']}, FB Spin% {self.percentiles['FB Spin%']}, Hard Hit% {self.percentiles['Hard Hit%']}")
        print(f"  FB IVB: {self.pitch_movement.get('IVB')}, HB: {self.pitch_movement.get('HB')}, Usage: {self.pitch_movement.get('Usage%')}, Spin: {self.pitch_movement.get('SpinRate')}")
        if self.swing_take:
            print(f"  Swing/Take Runs: All {self.swing_take['runs_all']}, Heart {self.swing_take['runs_heart']}, Shadow {self.swing_take['runs_shadow']}, Chase {self.swing_take['runs_chase']}, Waste {self.swing_take['runs_waste']}")
        else:
            print("  Swing/Take Runs: Not found")
        print(f"  Fastball Velo (season): {self.season_velo}, Recent: {self.recent_velo}, Velo Anomaly: {self.velo_anomaly}")
        print(f"  Splits vs LHB: {get_pitcher_vs_hand_stats(self.name, 'L')}")
        print(f"  Splits vs RHB: {get_pitcher_vs_hand_stats(self.name, 'R')}")
        if self.opp_lineup_handedness:
            platoon_analysis_result = platoon_matchup_analysis(self.name, self.opp_lineup_handedness)
            print(f"  PLATOON MATCHUP: {platoon_analysis_result}")
        if self.arm_angle:
            print(f"  Arm Angle: Ball {self.arm_angle.get('ball_angle')}, Release Z {self.arm_angle.get('release_ball_z')}, Release X {self.arm_angle.get('release_ball_x')}")
        if self.active_spin:
            print(f"  Active Spin: 4S {self.active_spin.get('active_spin_fourseam')}, Curve {self.active_spin.get('active_spin_curve')}, Slider {self.active_spin.get('active_spin_slider')}")
        if self.running_game:
            print(f"  Running Game: Runs Prevented {self.running_game.get('runs_prevented_on_running_attr')}, SB Rate {self.running_game.get('rate_sbx')}, SB {self.running_game.get('n_sb')}, CS {self.running_game.get('n_cs')}")
        if self.movement_details:
            print(f"  Movement: IVB {self.movement_details.get('IVB')}, HB {self.movement_details.get('HB')}, Usage {self.movement_details.get('Usage%')}, SpinRate {self.movement_details.get('SpinRate')}")
        if self.spin_direction:
            print(f"  Spin Direction: {self.spin_direction.get('spin_direction')}, Spin Axis: {self.spin_direction.get('spin_axis')}")

        # New prints for Allowed Quality of Contact
        print(f"  Allowed Quality of Contact:")
        print(f"    Barrel%: {self.percentiles.get('Barrel%')}, Hard Hit%: {self.percentiles.get('Hard Hit%')}, xwOBA: {self.percentiles.get('xwOBA')}")
        if self.exit_velocity:
            print(f"    Avg Exit Velo: {self.exit_velocity.get('avg_hit_speed')}, Max Exit Velo: {self.exit_velocity.get('max_hit_speed')}, Barrel%: {self.exit_velocity.get('brl_percent')}")
        # Note: self.advanced.get('xwOBA') and self.advanced.get('xERA') are already printed under "Advanced Metrics"
        # If you want to explicitly duplicate them under "Allowed Quality of Contact", you can uncomment the line below.
        # print(f"    xwOBA (expected): {self.advanced.get('xwOBA')}, xERA: {self.advanced.get('xERA')}")
        # NEW: Print vulnerability flags
        print(f"    Is Vulnerable: {self.is_vulnerable}")
        print(f"    Is Auto-Fade: {self.is_auto_fade}")
        # Removed debug prints for cleaner output based on prior successful run.
class HitterAnalysis:
    def __init__(self, name, team):
        self.name = name
        self.team = team
        self.advanced = {}
        self.percentiles = {}
        self.bat_tracking = {}
        self.metrics = {}
        self.classic = {}

    def analyze(self):
        self.classic = get_classic_hitter_stats(self.name)
        self.advanced = get_expected_stats(self.name)
        self.percentiles['xwOBA'] = get_percentile(self.name, 'xwOBA')
        self.percentiles['Barrel%'] = get_percentile(self.name, 'Barrel%')
        self.bat_tracking = get_bat_tracking(self.name, recent=True)

    def print_report(self):
        print(f"{self.name} Classic Stats:")
        print(f"  OPS: {self.classic.get('OPS')}, HR: {self.classic.get('HR')}, AVG: {self.classic.get('AVG')}, OBP: {self.classic.get('OBP')}, SLG: {self.classic.get('SLG')}")
        print(f"{self.name} Advanced Metrics:")
        print(f"  xwOBA: {self.advanced.get('xwOBA')}, SIERA: {self.advanced.get('SIERA')}")
        print(f"  Percentiles: xwOBA {self.percentiles['xwOBA']}, Barrel% {self.percentiles['Barrel%']}")
        print(f"  Bat Speed: {self.bat_tracking.get('swing_speed')}, Attack Angle: {self.bat_tracking.get('attack_angle')}")
# === MAIN SCRIPT ===

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze MLB game using pitcher/hitter data.")
    parser.add_argument('game_data_file', type=str, help='Path to the YAML file containing game data.')
    args = parser.parse_args()

    # Load game data from YAML
    try:
        with open(args.game_data_file, 'r') as f:
            game_data = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: Game data file '{args.game_data_file}' not found.")
        exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}")
        exit(1)

    # Extract data from YAML
    game_info = game_data.get('game', {})
    home_team_abbr = game_info.get('home_team')
    away_team_abbr = game_info.get('away_team')
    home_sp_name = game_info.get('home_starting_pitcher')
    away_sp_name = game_info.get('away_starting_pitcher')

    home_lineup_data = game_info.get('home_lineup', [])
    away_lineup_data = game_info.get('away_lineup', [])

    home_lineup_handedness = [hitter['hand'] for hitter in home_lineup_data]
    away_lineup_handedness = [hitter['hand'] for hitter in away_lineup_data]

    if not all([home_team_abbr, away_team_abbr, home_sp_name, away_sp_name, home_lineup_handedness, away_lineup_handedness]):
        print("Error: Missing essential game data in the YAML file. Please check 'home_team', 'away_team', 'home_starting_pitcher', 'away_starting_pitcher', 'home_lineup', and 'away_lineup'.")
        exit(1)

    print(f"Analyzing matchup: {away_team_abbr} vs {home_team_abbr}")

    # Initialize Pitcher Analysis
    home_sp = PitcherAnalysis(home_sp_name, home_team_abbr, opp_lineup_handedness=away_lineup_handedness)
    away_sp = PitcherAnalysis(away_sp_name, away_team_abbr, opp_lineup_handedness=home_lineup_handedness)

    home_sp.analyze()
    away_sp.analyze()

    # Initialize Hitter Analysis for each player in the lineup
    home_hitters = []
    for hitter_data in home_lineup_data:
        hitter = HitterAnalysis(hitter_data['name'], home_team_abbr)
        hitter.analyze()
        home_hitters.append(hitter)

    away_hitters = []
    for hitter_data in away_lineup_data:
        hitter = HitterAnalysis(hitter_data['name'], away_team_abbr)
        hitter.analyze()
        away_hitters.append(hitter)

    # Initialize Bullpen Analysis (requires 'last3dayspitching.csv' to be present)
    home_recent_relievers = []
    away_recent_relievers = []
    if 'last3dayspitching' in csv_files:
        home_recent_relievers = get_recent_relievers_for_team(home_team_abbr, csv_files['last3dayspitching'])
        away_recent_relievers = get_recent_relievers_for_team(away_team_abbr, csv_files['last3dayspitching'])
    else:
        print("Cannot perform bullpen analysis: last3dayspitching.csv not loaded.")

    home_bullpen = BullpenAnalysis(home_recent_relievers, home_team_abbr)
    away_bullpen = BullpenAnalysis(away_recent_relievers, away_team_abbr)

    home_bullpen.analyze()
    away_bullpen.analyze()

    # Get recent team OPS for 'cold team' check
    recent_ops = get_recent_team_ops() # Now global or can be passed if preferred

    # Get park factors
    home_park_name = TEAM_TO_PARK.get(home_team_abbr)
    park_factors = PARK_FACTORS_2025.get(home_park_name, {'runs': 1.0, 'hr': 1.0, 'woba': 1.0})

    print("--- HOME STARTING PITCHER ANALYSIS ---")
    home_sp.print_report()
    print("\n--- AWAY STARTING PITCHER ANALYSIS ---")
    away_sp.print_report()

     # --- THIS IS WHERE YOU INSERT THE NEW ADVANCED PITCHER TRIGGERS CODE ---
    home_sp_triggers, home_sp_vulnerability_score = check_pitcher_triggers(home_sp)
    away_sp_triggers, away_sp_vulnerability_score = check_pitcher_triggers(away_sp)

   
    print("\n--- ADVANCED PITCHER TRIGGERS ---")
    print("Home SP:", home_sp_triggers, f"(Vulnerability Score: {home_sp_vulnerability_score:.2f})")
    print("Away SP:", away_sp_triggers, f"(Vulnerability Score: {away_sp_vulnerability_score:.2f})")

    update_pitcher_flags_from_advanced(home_sp, home_sp_triggers)
    update_pitcher_flags_from_advanced(away_sp, away_sp_triggers)


    print("\n--- HOME HITTERS ANALYSIS (Sample) ---")
    for hitter in home_hitters:
        hitter.print_report()
    print("\n--- AWAY HITTERS ANALYSIS (Sample) ---")
    for hitter in away_hitters:
        hitter.print_report()

    print_triggers_and_bets(
        home_sp, away_sp, home_hitters, away_hitters,
        home_bullpen, away_bullpen, park_factors, home_team_abbr, away_team_abbr, # <--- Correct team abbr variables
        home_sp_vulnerability_score, away_sp_vulnerability_score # <--- New vulnerability scores
    )
