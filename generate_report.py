# PitchingWRX Game Outing Report Generator v34

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.patheffects as pe
from matplotlib.patches import Circle, FancyArrowPatch
import datetime, io, os, urllib.request, tempfile
from pwrx_db import ingest_xlsx, season_averages, get_game_log

from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
    TableStyle, Image as RLImage, HRFlowable,
    PageBreak, KeepTogether)
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.pdfgen import canvas as rl_canvas_mod

def _dark_bg(canv, doc):
    canv.saveState()
    canv.setFillColor(colors.HexColor('#1A1F2E'))
    canv.rect(0, 0, letter[0], letter[1], fill=1, stroke=0)
    canv.restoreState()

#  Brand palette 
BG    = '#1A1F2E'; BG2   = '#222838'; BG3   = '#2A3045'
BORDER= '#3A4060'; OR    = '#F47920'; OR2   = '#FF9A4D'
WHT   = '#FFFFFF'; OFF_WHT='#E8EAF0'; GR   = '#8A90A8'; MG='#4A5070'

RL_BG    = colors.HexColor(BG);   RL_BG2  = colors.HexColor(BG2)
RL_BG3   = colors.HexColor(BG3);  RL_OR   = colors.HexColor(OR)
RL_OR2   = colors.HexColor(OR2);  RL_WHT  = colors.white
RL_OWHT  = colors.HexColor(OFF_WHT); RL_GR = colors.HexColor(GR)
RL_BORDER= colors.HexColor(BORDER)

PITCH_PALETTE = {
    'Four-Seam':'#FF1744','Fastball':'#FF1744','Sinker':'#00BFFF',
    'Cutter':'#CC00FF','Changeup':'#00E676','Slider':'#9E9E9E',
    'Sweeper':'#FFE000','Curveball':'#FF6D00','Knuckle Curve':'#FF6D00',
    'Knuckleball':'#A07850','Splitter':'#8D6E63',
}
_PITCH_ALIASES = {
    'four seamer':'#FF1744','four-seamer':'#FF1744','4-seam':'#FF1744',
    '4 seam':'#FF1744','two-seam':'#00BFFF','two seam':'#00BFFF',
    '2-seam':'#00BFFF','si':'#00BFFF','ff':'#FF1744','fs':'#8D6E63',
    'ch':'#00E676','cu':'#FF6D00','kc':'#FF6D00','sl':'#9E9E9E',
    'st':'#FFE000','fc':'#CC00FF',
}

def pc(pt):
    pt_lower = pt.lower().strip()
    if pt_lower in _PITCH_ALIASES: return _PITCH_ALIASES[pt_lower]
    for k, v in PITCH_PALETTE.items():
        if k.lower() in pt_lower or pt_lower in k.lower(): return v
    pt_words = set(pt_lower.replace('-',' ').split())
    for k, v in PITCH_PALETTE.items():
        k_words = set(k.lower().replace('-',' ').split())
        if pt_words & k_words: return v
    colors_fallback = ['#FF6B9D','#C0FF4D','#4DCCFF','#FF4DC0','#FFE04D']
    return colors_fallback[abs(hash(pt)) % len(colors_fallback)]

PITCH_ABBR = {
    'four seam':'4S','four-seam':'4S','four seamer':'4S','fastball':'4S',
    'sinker':'SI','two-seam':'SI','two seam':'SI','cutter':'CT',
    'slider':'SL','sweeper':'SW','changeup':'CH','change-up':'CH',
    'change up':'CH','splitter':'SP','curveball':'CU',
    'knuckle curve':'CU','curve':'CU','knuckleball':'KB',
}

def pitch_abbr(pt):
    return PITCH_ABBR.get(pt.lower().strip(), pt.split()[0][:2].upper())

def rl_color(h):
    r,g,b = int(h[1:3],16)/255, int(h[3:5],16)/255, int(h[5:7],16)/255
    return colors.Color(r,g,b)

#  Image fetching 
def _fetch_url(url, suffix='.png', timeout=8):
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; PitchingWRX/1.0)'})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
        if len(data) < 500: return None
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        tmp.write(data); tmp.close()
        return tmp.name
    except Exception:
        return None

def fetch_player_headshot(player_id, level='MLB'):
    level_key = 'milb' if str(level).upper() not in ('MLB',) else '67'
    if level_key == 'milb':
        url = (f"https://img.mlbstatic.com/mlb-photos/image/upload/"
               f"w_180,g_auto,c_fill/v1/people/{player_id}/headshot/milb/current")
    else:
        url = (f"https://img.mlbstatic.com/mlb-photos/image/upload/"
               f"d_people:generic:headshot:67:current.png/"
               f"w_213,q_auto:best/v1/people/{player_id}/headshot/67/current")
    path = _fetch_url(url)
    if path is None and level_key == 'milb':
        fallback = (f"https://img.mlbstatic.com/mlb-photos/image/upload/"
                    f"d_people:generic:headshot:67:current.png/"
                    f"w_213,q_auto:best/v1/people/{player_id}/headshot/67/current")
        path = _fetch_url(fallback)
    return path

def fetch_team_logo(team_id):
    png_direct = f"https://a.espncdn.com/combiner/i?img=/i/teamlogos/mlb/500/{_team_abbr_espn(team_id)}.png&w=120&h=120&transparent=true"
    path = _fetch_url(png_direct, suffix='.png')
    if path is None:
        cap_png = f"https://www.mlbstatic.com/team-logos/team-cap-on-dark/{team_id}.svg"
        path = _fetch_url(cap_png, suffix='.svg')
    return path

_ESPN_ABBR = {
    108:'laa',109:'ari',110:'bal',111:'bos',112:'chc',113:'cin',
    114:'cle',115:'col',116:'det',117:'hou',118:'kc', 119:'lad',
    120:'was',121:'nym',133:'oak',134:'pit',135:'sd', 136:'sea',
    137:'sf', 138:'stl',139:'tb', 140:'tex',141:'tor',142:'min',
    143:'phi',144:'atl',145:'chw',146:'mia',147:'nyy',158:'mil',
}
def _team_abbr_espn(team_id):
    return _ESPN_ABBR.get(int(team_id), str(team_id))

#  Matplotlib dark style 
def set_dark_style():
    plt.rcParams.update({
        'figure.facecolor':BG,'axes.facecolor':BG2,'axes.edgecolor':BORDER,
        'axes.labelcolor':GR,'xtick.color':GR,'ytick.color':GR,
        'text.color':WHT,'grid.color':MG,'grid.linewidth':0.6,'grid.alpha':0.5,
        'font.family':'DejaVu Sans','axes.spines.top':False,'axes.spines.right':False,
        'legend.facecolor':BG2,'legend.edgecolor':BORDER,'legend.labelcolor':OFF_WHT,
    })
set_dark_style()

#  Data loading 
def fix_count(val):
    if isinstance(val, str) and '-' in val and len(val) <= 3: return val
    if isinstance(val, datetime.datetime):
        balls = min(val.month-1, 3); strikes = min(val.day-1, 2)
        return f"{balls}-{strikes}"
    return str(val)

def load_data(path):
    df = pd.read_excel(path)
    df['count_fixed']      = df['count'].apply(fix_count)
    df['is_whiff']         = df['pitchResult'].isin(['Strike Swinging','Strikeout (Swinging)'])
    df['is_called_strike'] = df['pitchResult'] == 'Strike Looking'
    df['is_foul']          = df['pitchResult'] == 'Foul'
    df['is_swing']         = df['pitchOutcome'].isin(['S','B'])
    df['is_in_play']       = df['pitchOutcome'] == 'B'
    df['is_strike']        = df['pitchOutcome'].isin(['S','SL'])
    df['ExitVel_num']      = pd.to_numeric(df['ExitVel'].replace('-', np.nan), errors='coerce')
    for col in ['Vel','Spin','SpinEff','IndVertBrk','HorzBrk','RelX','RelZ',
                'Extension','ExitVel','LaunchAng','VertApprAngle','HorzApprAngle']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].replace('-', np.nan), errors='coerce')
    df['px'] = -df['x']   # negate for catcher's view (mirror pitcher's perspective)
    df['pz'] = df['y'] + 2.5
    return df

def buf(fig, dpi=150):
    b = io.BytesIO()
    fig.savefig(b, format='png', dpi=dpi, bbox_inches='tight',
                facecolor=fig.get_facecolor())
    b.seek(0); plt.close(fig); return b

#  Strike zone 
def draw_zone(ax, lw=1.5):
    zone = mpatches.Rectangle((-0.833,1.5),1.666,2.0,
        linewidth=lw,edgecolor=WHT,facecolor='none',zorder=3,alpha=0.85)
    ax.add_patch(zone)
    for xv in [-0.278,0.278]:
        ax.plot([xv,xv],[1.5,3.5],color=WHT,lw=0.5,zorder=3,alpha=0.35)
    for yv in [2.167,2.833]:
        ax.plot([-0.833,0.833],[yv,yv],color=WHT,lw=0.5,zorder=3,alpha=0.35)
    top=0.9; px=[-0.708,0.708,0.708,0.0,-0.708,-0.708]; py=[top,top,top-0.32,top-0.58,top-0.32,top]
    ax.fill(px,py,color=BORDER,zorder=2); ax.plot(px,py,color=WHT,lw=1.2,zorder=3,alpha=0.7)

#  Charts 
def chart_movement_radial(df, w=4.2, h=4.2):
    fig, ax = plt.subplots(figsize=(w,h), facecolor=BG)
    ax.set_facecolor(BG); lim=28
    ax.set_xlim(-lim,lim); ax.set_ylim(-lim,lim); ax.set_aspect('equal')
    for r, alpha in [(12,0.25),(24,0.18)]:
        ring = Circle((0,0),r,fill=False,edgecolor=GR,lw=0.8,linestyle='--',alpha=alpha,zorder=1)
        ax.add_patch(ring)
        ax.text(0,r+0.8,f'{r}"',ha='center',va='bottom',fontsize=6.5,color=GR,alpha=0.7)
    ax.axhline(0,color=MG,lw=0.8,alpha=0.5,zorder=1); ax.axvline(0,color=MG,lw=0.8,alpha=0.5,zorder=1)
    label_kw = dict(fontsize=8,fontweight='bold',color=GR,alpha=0.9)
    ax.text(0,lim-1,'Rise',ha='center',va='top',**label_kw)
    ax.text(0,-lim+1,'Drop',ha='center',va='bottom',**label_kw)
    ax.text(lim-1,0,'Arm Side',ha='right',va='center',**label_kw)
    ax.text(-lim+1,0,'Glove\nSide',ha='left',va='center',**label_kw)
    for pt in df['pitchTypeFull'].unique():
        sub = df[df['pitchTypeFull']==pt].dropna(subset=['HorzBrk','IndVertBrk'])
        if sub.empty: continue
        c=pc(pt); hb=sub['HorzBrk']; ivb=sub['IndVertBrk']
        ax.scatter(hb,ivb,c=c,s=38,alpha=0.55,zorder=4,edgecolors='none')
        mx,my=hb.mean(),ivb.mean()
        ax.scatter(mx,my,c=c,s=110,zorder=6,edgecolors=BG,linewidths=1.5)
        ax.annotate(pt[:3].upper(),(mx,my),fontsize=6,fontweight='bold',color=BG,
                    ha='center',va='center',zorder=7)
    for v in [12,24]:
        for sign in [1,-1]:
            ax.text(sign*v,-0.8,f'{v}"',ha='center',va='top',fontsize=6,color=GR,alpha=0.6)
    fig.suptitle('Movement Profile',fontsize=10,fontweight='bold',color=WHT,y=1.0,ha='center')
    ax.tick_params(left=False,bottom=False,labelleft=False,labelbottom=False)
    for sp in ax.spines.values(): sp.set_visible(False)
    handles=[mpatches.Patch(color=pc(pt),label=pt) for pt in df['pitchTypeFull'].unique()]
    ax.legend(handles=handles,loc='lower right',fontsize=6.5,framealpha=0.85,
              edgecolor=BORDER,labelcolor=OFF_WHT,handlelength=1.0,handleheight=0.8)
    fig.tight_layout(pad=0.5,rect=[0,0,1,0.94]); return fig

def chart_location(df, title='Pitch Locations', w=3.0, h=3.8, show_avg=False):
    fig, ax = plt.subplots(figsize=(w,h), facecolor=BG)
    ax.set_facecolor(BG); draw_zone(ax)
    for pt in df['pitchTypeFull'].unique():
        sub = df[df['pitchTypeFull']==pt]
        ax.scatter(sub['px'],sub['pz'],c=pc(pt),s=30,alpha=0.80,label=pt,
                   zorder=5,edgecolors=BG,linewidths=0.4)
        if show_avg:
            mx=sub['px'].mean(); mz=sub['pz'].mean(); c=pc(pt)
            ax.scatter(mx,mz,s=320,color=c,alpha=0.18,zorder=6,edgecolors='none')
            ax.scatter(mx,mz,s=160,color=c,alpha=0.85,zorder=7,edgecolors=WHT,linewidths=1.2)
            ax.text(mx,mz,pitch_abbr(pt),ha='center',va='center',
                    fontsize=5.5,fontweight='bold',color=BG,zorder=8)
    ax.set_xlim(-2.5,2.5); ax.set_ylim(0.1,5.0); ax.set_aspect('equal')
    ax.tick_params(left=False,bottom=False,labelleft=False,labelbottom=False)
    for sp in ax.spines.values(): sp.set_visible(False)
    ax.set_title(title,fontsize=9,fontweight='bold',color=WHT,pad=6)
    ax.legend(loc='upper center',bbox_to_anchor=(0.5,-0.04),ncol=3,fontsize=6,
              framealpha=0.0,edgecolor='none',markerscale=0.85,handletextpad=0.3,
              columnspacing=0.8,labelcolor=OFF_WHT)
    ax.text(0,0.02,"Catcher's View",ha='center',fontsize=6.5,color=GR,style='italic')
    fig.tight_layout(pad=0.5); return fig

def chart_release(df, w=3.5, h=3.2):
    fig, ax = plt.subplots(figsize=(w,h), facecolor=BG)
    ax.set_facecolor(BG); ax.grid(True,zorder=0,alpha=0.15,linewidth=0.6)
    for pt in df['pitchTypeFull'].unique():
        sub = df[df['pitchTypeFull']==pt].dropna(subset=['RelX','RelZ'])
        if sub.empty: continue
        c=pc(pt)
        ax.scatter(sub['RelX'],sub['RelZ'],c=c,s=32,alpha=0.70,label=pt,
                   zorder=4,edgecolors=BG,linewidths=0.3)
        if len(sub)>2:
            ellipse=mpatches.Ellipse((sub['RelX'].mean(),sub['RelZ'].mean()),
                width=sub['RelX'].std()*2,height=sub['RelZ'].std()*2,
                edgecolor=c,facecolor=c+'28',lw=1.0,zorder=3)
            ax.add_patch(ellipse)
    all_x=df['RelX'].dropna(); all_z=df['RelZ'].dropna()
    xpad=(all_x.max()-all_x.min())*0.30; zpad=(all_z.max()-all_z.min())*0.30
    ax.set_xlim(all_x.min()-xpad,all_x.max()+xpad)
    ax.set_ylim(all_z.min()-zpad,all_z.max()+zpad)
    import matplotlib.ticker as ticker
    ax.xaxis.set_major_locator(ticker.MaxNLocator(nbins=3,integer=False))
    ax.yaxis.set_major_locator(ticker.MaxNLocator(nbins=3,integer=False))
    ax.set_xlabel('Horizontal Release (in)',fontsize=7.5)
    ax.set_ylabel('Release Height (in)',fontsize=7.5)
    fig.suptitle('Release Point',fontsize=10,fontweight='bold',color=WHT,y=1.0,ha='center')
    ax.tick_params(labelsize=7); ax.spines['left'].set_color(BORDER); ax.spines['bottom'].set_color(BORDER)
    ax.legend(fontsize=6.5,framealpha=0.85,edgecolor=BORDER,markerscale=0.85,handletextpad=0.4)
    fig.tight_layout(pad=0.6,rect=[0,0,1,0.94]); return fig

def chart_dot_range_single(df, metric, label, unit, w=3.88, h=3.0):
    pts=list(df['pitchTypeFull'].unique())
    fig, ax = plt.subplots(figsize=(w,h), facecolor=BG)
    ax.set_facecolor(BG)
    ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False); ax.spines['bottom'].set_color(BORDER)
    ax.yaxis.grid(True,color=BORDER,alpha=0.25,linewidth=0.6,zorder=0); ax.set_axisbelow(True)
    y_pos=np.arange(len(pts))
    for i, pt in enumerate(pts):
        vals=df[df['pitchTypeFull']==pt][metric].dropna()
        if vals.empty: continue
        c=pc(pt); mn=vals.min(); mx=vals.max(); avg=vals.mean()
        p25=vals.quantile(0.25); p75=vals.quantile(0.75)
        ax.plot([mn,mx],[i,i],color=c,lw=1.2,alpha=0.35,zorder=2,solid_capstyle='round')
        ax.plot([p25,p75],[i,i],color=c,lw=5,alpha=0.40,zorder=3,solid_capstyle='round')
        ax.scatter(avg,i,color=c,s=90,zorder=5,edgecolors=BG,linewidths=1.2)
        for xv in [mn,mx]:
            ax.plot([xv,xv],[i-0.18,i+0.18],color=c,lw=1.0,alpha=0.45,zorder=2)
        ax.text(avg,i+0.28,f'{avg:.1f}',ha='center',va='bottom',fontsize=7,color=c,fontweight='bold')
        span=mx-mn if mx!=mn else 1
        ax.text(mn-span*0.02,i,f'{mn:.0f}',ha='right',va='center',fontsize=6,color=GR,alpha=0.75)
        ax.text(mx+span*0.02,i,f'{mx:.0f}',ha='left',va='center',fontsize=6,color=GR,alpha=0.75)
    ax.set_yticks(y_pos); ax.set_yticklabels(pts,fontsize=8,fontweight='bold')
    for tick,pt in zip(ax.get_yticklabels(),pts): tick.set_color(pc(pt))
    ax.tick_params(axis='x',labelsize=7,colors=GR); ax.tick_params(axis='y',length=0)
    ax.set_xlabel(f'{label} ({unit})',fontsize=8,color=GR)
    ax.text(0.99,0.02,' avg   IQR   range',transform=ax.transAxes,
            ha='right',va='bottom',fontsize=6.5,color=GR,style='italic')
    all_vals=df[metric].dropna(); span=all_vals.max()-all_vals.min()
    ax.set_xlim(all_vals.min()-span*0.18,all_vals.max()+span*0.18)
    ax.set_ylim(-0.6,len(pts)-0.4)
    fig.suptitle(f'{label} Range',fontsize=10,fontweight='bold',color=WHT,x=0.5,y=1.0,ha='center')
    fig.tight_layout(pad=0.8,rect=[0,0,1,0.94]); return fig

def chart_usage_count(df, w=8.0, h=3.0):
    order=['0-0','0-1','0-2','1-0','1-1','1-2','2-0','2-1','2-2','3-0','3-1','3-2']
    present=[c for c in order if c in df['count_fixed'].values]
    pts=list(df['pitchTypeFull'].unique())
    usage,totals={},[]
    for cnt in present:
        sub=df[df['count_fixed']==cnt]; totals.append(len(sub))
        for pt in pts:
            usage.setdefault(pt,[]).append(
                len(sub[sub['pitchTypeFull']==pt])/len(sub)*100 if len(sub) else 0)
    fig, ax = plt.subplots(figsize=(w,h), facecolor=BG)
    ax.set_facecolor(BG); x=np.arange(len(present)); bar_w=0.60; bottom=np.zeros(len(present))
    for pt in pts:
        vals=np.array(usage[pt]); c=pc(pt)
        ax.bar(x,vals,bottom=bottom,color=c,label=pt,width=bar_w,
               edgecolor=BG,linewidth=0.8,alpha=0.82,zorder=3)
        for i,(v,b) in enumerate(zip(vals,bottom)):
            if v>11:
                ax.text(x[i],b+v/2,f'{v:.0f}%',ha='center',va='center',
                        fontsize=6.8,color=BG,fontweight='bold',zorder=4)
        bottom+=vals
    for i,(cnt,tot) in enumerate(zip(present,totals)):
        ax.text(x[i],bottom[i]+1.5,f'Pitches: {tot}',ha='center',va='bottom',fontsize=5.8,color=GR)
    for i,cnt in enumerate(present):
        if cnt in ['1-0','2-0','3-0','2-1','3-1']:
            ax.axvspan(x[i]-bar_w/2-0.04,x[i]+bar_w/2+0.04,0,0.95,color='#FF4C4C',alpha=0.04,zorder=0)
        elif cnt in ['0-1','0-2','1-2']:
            ax.axvspan(x[i]-bar_w/2-0.04,x[i]+bar_w/2+0.04,0,0.95,color='#4DFF91',alpha=0.04,zorder=0)
    for i in range(1,len(present)):
        if present[i][0]!=present[i-1][0]:
            xb=(x[i-1]+x[i])/2; ax.axvline(xb,color=BORDER,lw=0.8,alpha=0.6,zorder=1)
    ax.set_xticks(x); ax.set_xticklabels(present,fontsize=8.5,fontweight='bold',color=OFF_WHT)
    ax.set_ylabel('Usage %',fontsize=8,color=GR); ax.set_ylim(0,116)
    fig.suptitle('Pitch Usage by Count',fontsize=10,fontweight='bold',color=WHT,y=1.0,ha='center')
    ax.tick_params(axis='y',labelsize=7,colors=GR); ax.tick_params(axis='x',length=0)
    ax.legend(fontsize=7.5,loc='upper center',bbox_to_anchor=(0.5,1.18),ncol=len(pts),
              frameon=False,labelcolor=OFF_WHT,handlelength=1.0,handletextpad=0.4,columnspacing=1.2)
    ax.spines['left'].set_color(BORDER); ax.spines['bottom'].set_color(BORDER)
    ax.yaxis.grid(True,color=BORDER,alpha=0.3,linewidth=0.6,zorder=0); ax.set_axisbelow(True)
    fig.tight_layout(pad=0.6,rect=[0,0,1,0.94]); return fig

def chart_usage_count_single(df, hand, w=3.88, h=3.0):
    order=['0-0','0-1','0-2','1-0','1-1','1-2','2-0','2-1','2-2','3-0','3-1','3-2']
    sub_df=df[df['batterHand']==hand]; label=f'vs {"R" if hand=="R" else "L"}HH'
    present=[c for c in order if c in sub_df['count_fixed'].values]
    pts=list(df['pitchTypeFull'].unique())
    usage,totals={},[]
    for cnt in present:
        sub=sub_df[sub_df['count_fixed']==cnt]; totals.append(len(sub))
        for pt in pts:
            usage.setdefault(pt,[]).append(
                len(sub[sub['pitchTypeFull']==pt])/len(sub)*100 if len(sub) else 0)
    fig, ax = plt.subplots(figsize=(w,h), facecolor=BG)
    ax.set_facecolor(BG); x=np.arange(len(present)); bar_w=0.60; bottom=np.zeros(len(present))
    for pt in pts:
        vals=np.array(usage.get(pt,[0]*len(present)))
        ax.bar(x,vals,bottom=bottom,color=pc(pt),label=pt,width=bar_w,
               edgecolor=BG,linewidth=0.8,alpha=0.82,zorder=3)
        for i,(v,b) in enumerate(zip(vals,bottom)):
            if v>13:
                ax.text(x[i],b+v/2,f'{v:.0f}%',ha='center',va='center',
                        fontsize=6.5,color=BG,fontweight='bold',zorder=4)
        bottom+=vals
    for i,(cnt,tot) in enumerate(zip(present,totals)):
        ax.text(x[i],bottom[i]+1.5,f'{tot}',ha='center',va='bottom',fontsize=5.8,color=GR)
    for i,cnt in enumerate(present):
        if cnt in ['1-0','2-0','3-0','2-1','3-1']:
            ax.axvspan(x[i]-bar_w/2-0.04,x[i]+bar_w/2+0.04,0,0.95,color='#FF4C4C',alpha=0.04,zorder=0)
        elif cnt in ['0-1','0-2','1-2']:
            ax.axvspan(x[i]-bar_w/2-0.04,x[i]+bar_w/2+0.04,0,0.95,color='#4DFF91',alpha=0.04,zorder=0)
    for i in range(1,len(present)):
        if present[i][0]!=present[i-1][0]:
            xb=(x[i-1]+x[i])/2; ax.axvline(xb,color=BORDER,lw=0.8,alpha=0.6,zorder=1)
    ax.set_xticks(x); ax.set_xticklabels(present,fontsize=7.5,fontweight='bold',color=OFF_WHT)
    ax.set_ylabel('Usage %',fontsize=7.5,color=GR); ax.set_ylim(0,116)
    fig.suptitle(f'Usage by Count    {label}',fontsize=10,fontweight='bold',color=WHT,y=1.0,ha='center')
    ax.tick_params(axis='y',labelsize=7,colors=GR); ax.tick_params(axis='x',length=0)
    ax.spines['left'].set_color(BORDER); ax.spines['bottom'].set_color(BORDER)
    ax.yaxis.grid(True,color=BORDER,alpha=0.3,linewidth=0.6,zorder=0); ax.set_axisbelow(True)
    fig.tight_layout(pad=0.6,rect=[0,0,1,0.94]); return fig

def chart_usage_by_hand_single(df, hand, w=3.9, h=2.6):
    pts=list(df['pitchTypeFull'].unique()); sub=df[df['batterHand']==hand]
    n_total=len(sub); label=f'vs {"R" if hand=="R" else "L"}HH'
    fig, ax = plt.subplots(figsize=(w,h), facecolor=BG)
    ax.set_facecolor(BG); ax.grid(axis='x',zorder=0,alpha=0.15,color=BORDER,linewidth=0.6)
    if n_total>0:
        counts=[len(sub[sub['pitchTypeFull']==pt]) for pt in pts]
        pcts=[c/n_total*100 for c in counts]; clrs=[pc(pt) for pt in pts]
        y_pos=np.arange(len(pts))
        bars=ax.barh(y_pos,pcts,color=clrs,edgecolor=BG,linewidth=0.6,alpha=0.90,height=0.55)
        for bar,val,cnt in zip(bars,pcts,counts):
            ax.text(bar.get_width()+0.8,bar.get_y()+bar.get_height()/2,
                    f'{val:.1f}%  (Pitches: {cnt})',va='center',fontsize=7,color=OFF_WHT)
        ax.set_yticks(y_pos); ax.set_yticklabels(pts,fontsize=8,fontweight='bold')
        for tick,pt in zip(ax.get_yticklabels(),pts): tick.set_color(pc(pt))
        ax.set_xlim(0,max(pcts)*1.6); ax.invert_yaxis()
    ax.set_xlabel('Usage %',fontsize=8,color=GR)
    fig.suptitle(f'Pitch Usage    {label}    Pitches: {n_total}',
                 fontsize=10,fontweight='bold',color=WHT,y=1.0,ha='center')
    ax.tick_params(axis='x',labelsize=7,colors=GR); ax.tick_params(axis='y',length=0)
    ax.spines['left'].set_color(BORDER); ax.spines['bottom'].set_color(BORDER)
    fig.tight_layout(pad=0.7,rect=[0,0,1,0.94]); return fig

def chart_lr_bar_single(df, metric, ylabel, w=3.9, h=2.6):
    pts=list(df['pitchTypeFull'].unique()); splits=[]
    for hand in ['R','L']:
        sub=df[df['batterHand']==hand]
        for pt in pts:
            s=sub[sub['pitchTypeFull']==pt]
            if s.empty: continue
            sw=s['is_swing'].sum(); wh=s['is_whiff'].sum()
            splits.append({'Hand':f'vs {"R" if hand=="R" else "L"}HH','Pitch':pt,
                           'Whiff%':wh/sw*100 if sw else 0,
                           'CSW%':(s['is_called_strike'].sum()+wh)/len(s)*100})
    sp=pd.DataFrame(splits)
    fig, ax = plt.subplots(figsize=(w,h), facecolor=BG)
    ax.set_facecolor(BG); ax.grid(axis='y',zorder=0,alpha=0.15,color=BORDER,linewidth=0.6)
    x=np.arange(len(pts)); width=0.35
    for j,(hand,clr) in enumerate([('vs RHH','#4DA6FF'),('vs LHH','#FF4C4C')]):
        vals=[]
        for pt in pts:
            row=sp[(sp['Hand']==hand)&(sp['Pitch']==pt)]
            vals.append(row[metric].values[0] if not row.empty else 0)
        bars=ax.bar(x+j*width-width/2,vals,width,label=hand,color=clr,
                    alpha=0.85,edgecolor=BG,linewidth=0.5)
        for bar,val in zip(bars,vals):
            if val>4:
                ax.text(bar.get_x()+bar.get_width()/2,bar.get_height()+0.5,
                        f'{val:.0f}',ha='center',fontsize=6.5,color=GR)
    ax.set_xticks(x); ax.set_xticklabels([p.split()[0] for p in pts],fontsize=7.5,rotation=15,ha='right')
    ax.set_ylabel(ylabel,fontsize=8,color=GR)
    fig.suptitle(f'{ylabel} by Batter Hand',fontsize=10,fontweight='bold',color=WHT,y=1.0,ha='center')
    ax.tick_params(labelsize=7,colors=GR); ax.spines['left'].set_color(BORDER); ax.spines['bottom'].set_color(BORDER)
    ax.legend(fontsize=7.5,framealpha=0.85,edgecolor=BORDER)
    fig.tight_layout(pad=0.7,rect=[0,0,1,0.94]); return fig

def chart_location_single_hand(df, hand, w=3.7, h=3.8, show_avg=True):
    sub=df[df['batterHand']==hand]; label=f'vs {"R" if hand=="R" else "L"}HH'
    fig, ax = plt.subplots(figsize=(w,h), facecolor=BG)
    ax.set_facecolor(BG); draw_zone(ax)
    for pt in df['pitchTypeFull'].unique():
        s2=sub[sub['pitchTypeFull']==pt]
        if s2.empty: continue
        ax.scatter(s2['px'],s2['pz'],c=pc(pt),s=30,alpha=0.80,label=pt,
                   zorder=5,edgecolors=BG,linewidths=0.3)
        if show_avg:
            mx=s2['px'].mean(); mz=s2['pz'].mean(); c=pc(pt)
            ax.scatter(mx,mz,s=320,color=c,alpha=0.18,zorder=6,edgecolors='none')
            ax.scatter(mx,mz,s=160,color=c,alpha=0.85,zorder=7,edgecolors=WHT,linewidths=1.2)
            ax.text(mx,mz,pitch_abbr(pt),ha='center',va='center',
                    fontsize=5.5,fontweight='bold',color=BG,zorder=8)
    ax.set_xlim(-2.5,2.5); ax.set_ylim(0.1,5.0); ax.set_aspect('equal')
    ax.tick_params(left=False,bottom=False,labelleft=False,labelbottom=False)
    for sp in ax.spines.values(): sp.set_visible(False)
    ax.set_title(f'{label}    Pitches: {len(sub)}',fontsize=9.5,fontweight='bold',color=WHT,pad=6)
    ax.legend(loc='upper center',bbox_to_anchor=(0.5,-0.04),ncol=3,fontsize=6,
              framealpha=0.0,edgecolor='none',markerscale=0.85,handletextpad=0.3,
              columnspacing=0.8,labelcolor=OFF_WHT)
    ax.text(0,0.02,"Catcher's View",ha='center',fontsize=6.5,color=GR,style='italic')
    fig.tight_layout(pad=0.5); return fig

def chart_velo_seq(sub, pt, w=4.4, h=2.2):
    c=pc(pt); fig, ax = plt.subplots(figsize=(w,h), facecolor=BG)
    ax.set_facecolor(BG); velos=sub['Vel'].reset_index(drop=True)
    xs=list(range(1,len(velos)+1)); avg=velos.mean()
    ax.fill_between(xs,velos,avg,alpha=0.15,color=c,zorder=2)
    ax.plot(xs,velos,color=c,lw=1.6,zorder=3,solid_capstyle='round')
    ax.scatter(xs,velos,color=c,s=28,zorder=4,edgecolors=BG,lw=0.6)
    ax.hlines(avg,xs[0],xs[-1],colors=GR,lw=0.9,linestyles='--',alpha=0.55,zorder=1)
    ax.annotate(f'avg {avg:.1f}',xy=(1.01,avg),xycoords=('axes fraction','data'),
                fontsize=6,color=GR,va='center')
    ax.set_xlabel('Pitch #',fontsize=6.5,color=GR); ax.set_ylabel('Velocity (mph)',fontsize=6.5,color=GR)
    ax.set_xlim(0.5,len(velos)+0.5); ax.xaxis.set_major_locator(plt.MaxNLocator(integer=True))
    ax.tick_params(axis='both',labelsize=6,colors=GR,length=3)
    for spine in ax.spines.values(): spine.set_edgecolor(BORDER); spine.set_linewidth(0.8)
    ax.yaxis.grid(True,color=BORDER,lw=0.5,alpha=0.5,zorder=0); ax.set_axisbelow(True)
    fig.suptitle(f'{pt}  Velocity Sequence',fontsize=10,fontweight='bold',color=WHT,y=1.0,ha='center')
    fig.tight_layout(pad=0.8,rect=[0,0,0.97,0.94]); return fig

#  Season trends chart 
def chart_trends(game_log_df, current_date, pitch_types):
    # Monthly-average trend charts. Level badges. Gaps >60 days break line.
    import matplotlib.dates as mdates
    set_dark_style()
    results = []

    LEVEL_COLORS = {'MLB': OR, 'AAA': '#4DA6FF', 'EXB': '#A0A0A0', 'SP': '#A0A0A0'}

    metrics = [
        ('avg_vel',   'Velo (mph)', True),
        ('whiff_pct', 'Whiff%',     False),
        ('csw_pct',   'CSW%',       False),
    ]

    current_month = pd.to_datetime(current_date).to_period('M')

    for pt in pitch_types:
        sub = game_log_df[game_log_df['pitch_type_full'] == pt].copy()
        if len(sub) < 2:
            continue

        sub['game_date'] = pd.to_datetime(sub['game_date'])
        sub['month']     = sub['game_date'].dt.to_period('M')

        # Aggregate to monthly  carry level from majority level that month
        def agg_month(g):
            level_mode = g['level'].mode().iloc[0] if 'level' in g.columns and not g['level'].isna().all() else ''
            return pd.Series({
                'avg_vel':   g['avg_vel'].mean(),
                'whiff_pct': g['whiff_pct'].mean(),
                'csw_pct':   g['csw_pct'].mean(),
                'level':     level_mode,
                'pitches':   g['pitches'].sum(),
            })

        monthly = sub.groupby('month').apply(agg_month).reset_index().sort_values('month')

        if len(monthly) < 1:
            continue

        c = pc(pt)
        monthly['dt'] = monthly['month'].apply(lambda m: m.to_timestamp())
        dts   = monthly['dt'].tolist()
        levels= monthly['level'].tolist() if 'level' in monthly.columns else ['']*len(dts)

        # Break segments at gaps > 60 days
        breaks = set()
        for i in range(1, len(dts)):
            if (dts[i] - dts[i-1]).days > 60:
                breaks.add(i)

        cur_dt = current_month.to_timestamp() \
            if current_month in monthly['month'].values else None

        fig, axes = plt.subplots(1, 3, figsize=(11, 2.4))
        fig.patch.set_facecolor(BG)

        for ax, (col, label, _) in zip(axes, metrics):
            vals = pd.to_numeric(monthly[col], errors='coerce').tolist()
            all_valid = [v for v in vals if pd.notna(v)]
            if not all_valid:
                ax.set_visible(False); continue

            szn_avg = np.nanmean(all_valid)

            # Build segments (break at long gaps)
            segments = []
            seg_dt, seg_v, seg_lv = [], [], []
            for i, (dt, v, lv) in enumerate(zip(dts, vals, levels)):
                if i in breaks and seg_dt:
                    segments.append((seg_dt[:], seg_v[:], seg_lv[:]))
                    seg_dt, seg_v, seg_lv = [], [], []
                if pd.notna(v):
                    seg_dt.append(dt); seg_v.append(v); seg_lv.append(lv)
            if seg_dt:
                segments.append((seg_dt, seg_v, seg_lv))

            for seg_dts, seg_ys, seg_lvs in segments:
                if len(seg_dts) == 1:
                    lv = seg_lvs[0]
                    dot_c = LEVEL_COLORS.get(lv, c)
                    ax.scatter(seg_dts, seg_ys, color=dot_c, s=44, zorder=5,
                               edgecolors=BG, lw=0.8)
                else:
                    ax.plot(seg_dts, seg_ys, color=c, lw=1.6, zorder=3,
                            solid_capstyle='round')
                    ax.fill_between(seg_dts, seg_ys, szn_avg,
                                    alpha=0.12, color=c, zorder=2)
                    # Color-coded dots per level
                    for dt, v, lv in zip(seg_dts, seg_ys, seg_lvs):
                        dot_c = LEVEL_COLORS.get(lv, c)
                        ax.scatter([dt], [v], color=dot_c, s=32, zorder=5,
                                   edgecolors=BG, lw=0.6)

            # Season avg dashed line per segment
            for seg_dts, _, _ in segments:
                ax.hlines(szn_avg, seg_dts[0], seg_dts[-1],
                          colors=GR, lw=0.9, linestyles='--', alpha=0.55, zorder=1)
            ax.annotate(f'avg {szn_avg:.1f}',
                        xy=(1.01, szn_avg), xycoords=('axes fraction','data'),
                        fontsize=6, color=GR, va='center')

            # Current month highlight
            if cur_dt is not None:
                cur_idx = next((i for i, dt in enumerate(dts) if dt == cur_dt), None)
                if cur_idx is not None and pd.notna(vals[cur_idx]):
                    ax.scatter([cur_dt],[vals[cur_idx]],color=OR,s=90,
                               zorder=6,edgecolors=WHT,lw=0.9)
                    ax.annotate(f'{vals[cur_idx]:.1f}',(cur_dt,vals[cur_idx]),
                                textcoords='offset points',xytext=(0,8),
                                fontsize=6.5,color=OR,ha='center',fontweight='bold')

            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
            plt.setp(ax.xaxis.get_majorticklabels(),rotation=40,ha='right',fontsize=5.5,color=GR)
            ax.set_title(label,fontsize=7.5,color=OFF_WHT,pad=4,fontweight='bold')
            ax.set_facecolor(BG); ax.tick_params(axis='y',labelsize=6,colors=GR)
            ax.tick_params(axis='x',colors=GR,length=3)
            for spine in ax.spines.values():
                spine.set_edgecolor(BORDER); spine.set_linewidth(0.8)
            ax.yaxis.grid(True,color=BORDER,lw=0.5,alpha=0.5,zorder=0); ax.set_axisbelow(True)
            ymin,ymax=np.nanmin(all_valid),np.nanmax(all_valid)
            pad=max((ymax-ymin)*0.3,1.0)
            ax.set_ylim(ymin-pad,ymax+pad)
            ax.set_xlim(min(dts)-pd.Timedelta(days=10),max(dts)+pd.Timedelta(days=10))

        # Level legend
        present_levels = list(dict.fromkeys(lv for lv in levels if lv))
        legend_patches = [mpatches.Patch(color=LEVEL_COLORS.get(lv, GR), label=lv)
                          for lv in present_levels]
        if legend_patches:
            fig.legend(handles=legend_patches, loc='upper right',
                       bbox_to_anchor=(0.99, 1.02), ncol=len(legend_patches),
                       fontsize=6, framealpha=0.0, labelcolor=OFF_WHT,
                       handlelength=0.8, handletextpad=0.4, columnspacing=0.8)

        fig.tight_layout(pad=0.7, rect=[0, 0, 0.97, 0.96])
        b = io.BytesIO()
        fig.savefig(b, format='png', dpi=150, bbox_inches='tight', facecolor=BG)
        b.seek(0); plt.close(fig)
        results.append((pt, b))

    return results

#  Stats helpers 
def pitch_stats(df):
    rows=[]
    for pt in df['pitchTypeFull'].unique():
        s=df[df['pitchTypeFull']==pt]; n=len(s)
        sw=s['is_swing'].sum(); wh=s['is_whiff'].sum()
        rows.append({'Pitch':pt,'n':n,'Usage%':f'{n/len(df)*100:.1f}%',
            'Velo Avg':f"{s['Vel'].mean():.1f}",'Velo Max':f"{s['Vel'].max():.1f}",
            'Spin':f"{s['Spin'].mean():.0f}",
            'SpinEff%':f"{s['SpinEff'].mean()*100:.1f}%" if pd.notna(s['SpinEff'].mean()) else '',
            'IVB':f"{s['IndVertBrk'].mean():.1f}\"",
            'HB':f"{s['HorzBrk'].mean():.1f}\"",
            'Whiff%':f'{wh/sw*100:.1f}%' if sw else '',
            'CSW%':f'{(s["is_called_strike"].sum()+wh)/n*100:.1f}%',
            'Ext':f"{s['Extension'].mean():.2f}'"})
    return pd.DataFrame(rows)

def arsenal_comparison_table(game_df, szn_df, game_log_df=None):
    if szn_df.empty: return []
    szn_lookup=szn_df.set_index('Pitch')
    COLS=[
        ('Usage%','usage',None,True,True,None),
        ('Velo Avg','Vel','season_avg_vel',True,False,'avg_vel'),
        ('Velo Max','Vel_max','season_max_vel',True,False,None),
        ('Spin','Spin','season_avg_spin',True,False,'avg_spin'),
        ('IVB','IndVertBrk','season_avg_ivb',True,False,None),
        ('HB','HorzBrk','season_avg_hb',False,False,None),
        ('Whiff%','whiff','season_whiff_pct',True,True,'whiff_pct'),
        ('CSW%','csw','season_csw_pct',True,True,'csw_pct'),
        ('Ext','Extension',None,True,False,None),
    ]
    def make_cell(gv_raw, sv_raw, higher_is_better, pct):
        try:
            gv=float(str(gv_raw).replace('%','').replace('"','').replace("'",''))
            dec=0 if abs(gv)>=100 and not pct else 1
            sfx='%' if pct else ''
            game_str=f'{gv:.{dec}f}{sfx}'
            if sv_raw is not None and pd.notna(sv_raw):
                sv=float(str(sv_raw).replace('%','').replace('"','').replace("'",''))
                diff=gv-sv; szn_str=f'{sv:.{dec}f}{sfx}'
                if abs(diff)<0.05: arrow='\u25cf'; acolor=GR
                elif diff>0: arrow='\u25b2'; acolor='#00E676' if higher_is_better else '#FF4C4C'
                else: arrow='\u25bc'; acolor='#FF4C4C' if higher_is_better else '#00E676'
            else:
                szn_str=''; arrow=''; acolor=GR
            return Paragraph(
                f'<b><font size="8" color="{OFF_WHT}">{game_str}</font></b>'
                f'<font size="7" color="{acolor}"> {arrow}</font><br/>'
                f'<font size="5.5" color="{GR}">{szn_str}</font>',
                style(f'cv_{game_str}',alignment=TA_CENTER,leading=11,spaceAfter=0,spaceBefore=0))
        except Exception:
            return Paragraph(f'<b><font size="8" color="{OFF_WHT}">{gv_raw}</font></b>',
                             style('cv_fb',alignment=TA_CENTER,leading=11))

    header_row=[
        Paragraph('<b>Pitch</b>',style('arh0',alignment=TA_LEFT,fontSize=7.5)),
        Paragraph('<b>n</b>',style('arhn',alignment=TA_CENTER,fontSize=7.5)),
    ]+[Paragraph(f'<b>{c[0]}</b>',style(f'arh{i}',alignment=TA_CENTER,fontSize=7.5))
       for i,c in enumerate(COLS)]

    rows=[header_row]; row_bgs=[]; total_n=len(game_df)
    game_stats=pitch_stats(game_df)

    for idx,(_,gr) in enumerate(game_stats.iterrows()):
        pt=gr['Pitch']; c=pc(pt)
        sub=game_df[game_df['pitchTypeFull']==pt]
        sw=sub['is_swing'].sum(); wh=sub['is_whiff'].sum(); n=len(sub)
        game_vals={'usage':n/total_n*100,'Vel':sub['Vel'].mean(),'Vel_max':sub['Vel'].max(),
                   'Spin':sub['Spin'].mean(),'IndVertBrk':sub['IndVertBrk'].mean(),
                   'HorzBrk':sub['HorzBrk'].mean(),'whiff':wh/sw*100 if sw else None,
                   'csw':(sub['is_called_strike'].sum()+wh)/n*100,'Extension':sub['Extension'].mean()}
        pitch_cell=Paragraph(f'<b><font color="{c}">{pt}</font></b>',
                             style(f'arpn_{pt}',fontSize=8,leading=11))
        n_cell=Paragraph(
            f'<b><font size="8" color="{OFF_WHT}">{n}</font></b><br/>'
            f'<font size="5.5" color="{GR}">'
            f'{int(szn_lookup.loc[pt,"season_pitches"]) if pt in szn_lookup.index else ""}'
            f'</font>',style(f'arnc_{pt}',alignment=TA_CENTER,leading=10))
        data_cells=[]
        for _,gk,sk,hib,pct,spk in COLS:
            gval=game_vals.get(gk)
            sval=szn_lookup.loc[pt,sk] if (sk and pt in szn_lookup.index) else None
            if gval is None or (not pct and pd.isna(gval) if not isinstance(gval,str) else False):
                data_cells.append(Paragraph(f'<font size="7" color="{GR}"></font>',
                                            style('arna',alignment=TA_CENTER)))
            else:
                data_cells.append(make_cell(gval,sval,hib,pct))
        rows.append([pitch_cell,n_cell]+data_cells)
        row_bgs.append((len(rows)-1,BG2 if idx%2==0 else BG3))

    cw=[1.30*inch,0.38*inch,0.60*inch,0.82*inch,0.72*inch,0.68*inch,
        0.68*inch,0.68*inch,0.82*inch,0.72*inch,0.60*inch]
    t=Table(rows,colWidths=cw,repeatRows=1)
    s=[('BACKGROUND',(0,0),(-1,0),rl_color(OR)),('TEXTCOLOR',(0,0),(-1,0),rl_color(BG)),
       ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('ALIGN',(0,0),(-1,0),'CENTER'),
       ('ALIGN',(0,0),(0,0),'LEFT'),('TOPPADDING',(0,0),(-1,-1),4),
       ('BOTTOMPADDING',(0,0),(-1,-1),4),('LEFTPADDING',(0,0),(-1,-1),3),
       ('RIGHTPADDING',(0,0),(-1,-1),3),('GRID',(0,0),(-1,-1),0.3,rl_color(BORDER))]
    for row_idx,bg in row_bgs:
        s.append(('BACKGROUND',(0,row_idx),(-1,row_idx),rl_color(bg)))
    t.setStyle(TableStyle(s)); return t

#  xwOBA 
_W={'BB':0.690,'HBP':0.722,'1B':0.880,'2B':1.254,'3B':1.586,'HR':2.048}

def _xwoba_con(ev, la):
    if pd.isna(ev) or pd.isna(la): return np.nan
    if ev>=98 and 8<=la<=50:
        barrel_score=min(1.0,(ev-98)/20*0.5+0.5)
        return _W['HR']*barrel_score+_W['2B']*(1-barrel_score)
    if la<-10: return 0.05+max(0,ev-60)/200
    if la<10:
        if ev>=95: return 0.55
        if ev>=80: return 0.35
        return 0.15
    if la<25:
        if ev>=95: return 1.40
        if ev>=80: return 0.90
        return 0.55
    if la<50:
        if ev>=103: return _W['HR']
        if ev>=95:  return 1.20
        if ev>=80:  return 0.45
        return 0.10
    return 0.02

def calc_xwoba(s):
    terminal={'Walk','Strikeout (Swinging)','Strikeout (Looking)',
              'Ground Out','Fly Out','Line Out','Pop Out',
              'Single on a Line Drive','Single on a Ground Ball',
              'Double on a Line Drive','Double on a Ground Ball',
              'Triple','Home Run',"Fielder's Choice",
              'Reached on Error on a Ground Ball','Double Play','Hit By Pitch'}
    pa_rows=s[s['pitchResult'].isin(terminal)]; n_pa=len(pa_rows)
    if n_pa==0: return ''
    total_xw=0.0
    for _,row in pa_rows.iterrows():
        res=row['pitchResult']
        ev=pd.to_numeric(row['ExitVel'],errors='coerce')
        la=pd.to_numeric(row['LaunchAng'],errors='coerce')
        if 'Walk' in str(res): total_xw+=_W['BB']
        elif 'Hit By Pitch' in str(res): total_xw+=_W['HBP']
        elif not pd.isna(ev) and not pd.isna(la): total_xw+=_xwoba_con(ev,la)
    return f'{total_xw/n_pa:.3f}'.lstrip('0')

def splits_stats(df):
    rows=[]
    for hand in ['R','L']:
        sub=df[df['batterHand']==hand]
        if sub.empty: continue
        for pt in df['pitchTypeFull'].unique():
            s=sub[sub['pitchTypeFull']==pt]
            if s.empty: continue
            n=len(s); sw=s['is_swing'].sum(); wh=s['is_whiff'].sum()
            xw=calc_xwoba(s)
            ev=pd.to_numeric(s['ExitVel'],errors='coerce').dropna()
            la=pd.to_numeric(s['LaunchAng'],errors='coerce').dropna()
            n_contact=len(ev)
            hard_hit=f'{(ev>=95).sum()/n_contact*100:.1f}%' if n_contact else ''
            gb_pct=f'{(la<10).sum()/len(la)*100:.1f}%' if len(la) else ''
            soft_pct=f'{(ev<80).sum()/n_contact*100:.1f}%' if n_contact else ''
            whiff=f'{wh/sw*100:.1f}%' if sw else ''
            csw=f'{(s["is_called_strike"].sum()+wh)/n*100:.1f}%'
            two_k=s[s['count_fixed'].str.endswith('-2',na=False)]
            ks=two_k['pitchResult'].str.contains('Strikeout',na=False).sum()
            putaway=f'{ks/len(two_k)*100:.1f}%' if len(two_k) else ''
            rows.append({'Pitch':pt,'Hand':f'vs {hand}HH','Pitches':n,'xwOBA':xw,
                         'Whiff%':whiff,'CSW%':csw,'Hard Hit%':hard_hit,
                         'GB%':gb_pct,'Soft%':soft_pct,'PutAway%':putaway})
    return pd.DataFrame(rows)

def splits_stats_season(db_path, pitcher_id, pitcher_name, before_date):
    if not db_path or not os.path.exists(db_path): return pd.DataFrame()
    from pwrx_db import get_conn
    conn=get_conn(db_path); clauses=['1=1']; params=[]
    if pitcher_id: clauses.append('pitcher_id = ?'); params.append(int(pitcher_id))
    elif pitcher_name: clauses.append('pitcher_name = ?'); params.append(pitcher_name)
    if before_date: clauses.append('game_date < ?'); params.append(str(before_date))
    where=' AND '.join(clauses)
    df_db=pd.read_sql(("SELECT batter_hand,pitch_type_full,is_swing,is_whiff,"
        " is_called_strike,exit_vel,launch_ang,pitch_result,count_str"
        f" FROM pitches WHERE {where}"),conn,params=params)
    conn.close()
    if df_db.empty: return pd.DataFrame()
    df_db=df_db.rename(columns={'batter_hand':'batterHand','pitch_type_full':'pitchTypeFull',
        'exit_vel':'ExitVel','launch_ang':'LaunchAng','pitch_result':'pitchResult','count_str':'count_fixed'})
    for col in ['ExitVel','LaunchAng']:
        df_db[col]=pd.to_numeric(df_db[col],errors='coerce')
    rows=[]
    for hand in ['R','L']:
        sub=df_db[df_db['batterHand']==hand]
        if sub.empty: continue
        for pt in df_db['pitchTypeFull'].unique():
            s=sub[sub['pitchTypeFull']==pt]
            if s.empty: continue
            n=len(s); sw=s['is_swing'].sum(); wh=s['is_whiff'].sum()
            ev=s['ExitVel'].dropna(); la=s['LaunchAng'].dropna(); n_contact=len(ev)
            hard_hit=f'{(ev>=95).sum()/n_contact*100:.1f}%' if n_contact else ''
            gb_pct=f'{(la<10).sum()/len(la)*100:.1f}%' if len(la) else ''
            soft_pct=f'{(ev<80).sum()/n_contact*100:.1f}%' if n_contact else ''
            whiff=f'{wh/sw*100:.1f}%' if sw else ''
            csw=f'{(s["is_called_strike"].sum()+wh)/n*100:.1f}%'
            two_k=s[s['count_fixed'].str.endswith('-2',na=False)]
            ks=two_k['pitchResult'].str.contains('Strikeout',na=False).sum()
            putaway=f'{ks/len(two_k)*100:.1f}%' if len(two_k) else ''
            xw=calc_xwoba(s)
            rows.append({'Pitch':pt,'Hand':f'vs {hand}HH','Pitches':n,'xwOBA':xw,
                         'Whiff%':whiff,'CSW%':csw,'Hard Hit%':hard_hit,
                         'GB%':gb_pct,'Soft%':soft_pct,'PutAway%':putaway})
    return pd.DataFrame(rows)

#  ReportLab helpers 
def style(name, **kw):
    defaults=dict(fontName='Helvetica',fontSize=9,textColor=RL_OWHT,leading=12)
    defaults.update(kw); return ParagraphStyle(name,**defaults)

def dark_table(data, col_widths, hdr_color=None):
    if hdr_color is None: hdr_color=RL_OR
    t=Table(data,colWidths=col_widths,repeatRows=1)
    s=[('BACKGROUND',(0,0),(-1,0),hdr_color),('TEXTCOLOR',(0,0),(-1,0),RL_BG),
       ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),('FONTSIZE',(0,0),(-1,0),7.5),
       ('ALIGN',(0,0),(-1,0),'CENTER'),('VALIGN',(0,0),(-1,-1),'MIDDLE'),
       ('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4),
       ('LEFTPADDING',(0,0),(-1,-1),5),('RIGHTPADDING',(0,0),(-1,-1),5),
       ('FONTSIZE',(0,1),(-1,-1),7.5),('FONTNAME',(0,1),(-1,-1),'Helvetica'),
       ('TEXTCOLOR',(0,1),(-1,-1),RL_OWHT),('ALIGN',(1,1),(-1,-1),'CENTER'),
       ('ALIGN',(0,1),(0,-1),'LEFT'),('FONTNAME',(0,1),(0,-1),'Helvetica-Bold'),
       ('GRID',(0,0),(-1,-1),0.3,RL_BORDER)]
    for i in range(1,len(data)):
        bg=RL_BG2 if i%2==1 else RL_BG3
        s.append(('BACKGROUND',(0,i),(-1,i),bg))
    t.setStyle(TableStyle(s)); return t

def thin_div(color=BORDER, after=4, before=4):
    return HRFlowable(width='100%',thickness=0.5,color=rl_color(color),
                      spaceAfter=after,spaceBefore=before)

def framed_chart(img, cell_w, cell_h, pad=4):
    t=Table([[img]],colWidths=[cell_w],rowHeights=[cell_h])
    t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),RL_BG2),
        ('BOX',(0,0),(-1,-1),0.6,RL_BORDER),('ALIGN',(0,0),(-1,-1),'CENTER'),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),pad),
        ('BOTTOMPADDING',(0,0),(-1,-1),pad),('LEFTPADDING',(0,0),(-1,-1),pad),
        ('RIGHTPADDING',(0,0),(-1,-1),pad)]))
    return t

def chart_row(frames, gap=0.06, divider=True):
    cells,widths=[],[]
    for i,(frame,w) in enumerate(frames):
        cells.append(frame); widths.append(w)
        if i<len(frames)-1:
            if divider:
                div_cell=Table([['']],colWidths=[gap*inch],rowHeights=[None])
                div_cell.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),RL_BG),
                    ('LEFTPADDING',(0,0),(-1,-1),0),('RIGHTPADDING',(0,0),(-1,-1),0),
                    ('TOPPADDING',(0,0),(-1,-1),0),('BOTTOMPADDING',(0,0),(-1,-1),0),
                    ('LINEBEFORE',(0,0),(0,-1),0.6,RL_BORDER),
                    ('LINEAFTER',(0,0),(0,-1),0.6,RL_BORDER)]))
                cells.append(div_cell)
            else:
                cells.append('')
            widths.append(gap*inch)
    row=Table([cells],colWidths=widths)
    row.setStyle(TableStyle([('ALIGN',(0,0),(-1,-1),'CENTER'),
        ('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(-1,-1),0),
        ('RIGHTPADDING',(0,0),(-1,-1),0),('TOPPADDING',(0,0),(-1,-1),0),
        ('BOTTOMPADDING',(0,0),(-1,-1),0),('BACKGROUND',(0,0),(-1,-1),RL_BG)]))
    return row

def section_label(text):
    return [Paragraph(text,style('sec',fontName='Helvetica-Bold',fontSize=10.5,
                                  textColor=RL_OR,spaceBefore=6,spaceAfter=3)),
            HRFlowable(width='100%',thickness=0.5,color=RL_OR,spaceAfter=4)]

def orange_hr(after=6):
    return HRFlowable(width='100%',thickness=1.5,color=RL_OR,spaceAfter=after)

def footer_line(story):
    story.append(thin_div(BORDER,after=3,before=4))
    story.append(Paragraph(
        f'PitchingWRX    Data Driven Pitching Instruction    '
        f'Generated {datetime.date.today().strftime("%B %d, %Y")}',
        style('ft',fontSize=7,textColor=RL_GR,alignment=TA_CENTER)))

def pill_row(items, col_w=1.18):
    def one(k,v):
        inner=Table([[
            Paragraph(f'<b>{v}</b>',style('pv',fontName='Helvetica-Bold',fontSize=13,
                      textColor=RL_OR,alignment=TA_CENTER)),
            Paragraph(k,style('pk',fontSize=7,textColor=RL_GR,alignment=TA_CENTER)),
        ]],colWidths=[col_w*inch],style=TableStyle([
            ('BACKGROUND',(0,0),(-1,-1),RL_BG2),('ALIGN',(0,0),(-1,-1),'CENTER'),
            ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),6),
            ('BOTTOMPADDING',(0,0),(-1,-1),6),('LEFTPADDING',(0,0),(-1,-1),2),
            ('RIGHTPADDING',(0,0),(-1,-1),2),('LINEBELOW',(0,0),(-1,0),0.8,RL_OR)]))
        return inner
    row=Table([[one(k,v) for k,v in items]],colWidths=[(col_w+0.08)*inch]*len(items))
    row.setStyle(TableStyle([('ALIGN',(0,0),(-1,-1),'CENTER'),
        ('LEFTPADDING',(0,0),(-1,-1),2),('RIGHTPADDING',(0,0),(-1,-1),2)]))
    return row

#  MAIN BUILD 
def build_report(data_path, logo_path, output_path,
                 headshot_override=None, team_logo_override=None,
                 db_path=None, game_date=None, player_name=None):
    df = load_data(data_path)
    # Filter to selected player if provided
    if player_name:
        for col in ['fullName', 'pitcher', 'pitcherName']:
            if col in df.columns:
                df = df[df[col] == player_name].copy()
                break
    # Filter to selected game date if provided
    if game_date and 'gameDate' in df.columns:
        df['gameDate'] = pd.to_datetime(df['gameDate'])
        target = pd.to_datetime(game_date).date()
        df = df[df['gameDate'].dt.date == target].copy()
        if df.empty:
            raise ValueError(f'No pitches found for {player_name} on {game_date}')

    szn_df=pd.DataFrame(); game_log_df=pd.DataFrame()
    use_db = os.environ.get('DATABASE_URL') or db_path
    if use_db:
        try:
            ingest_xlsx(data_path, verbose=False)
            game_date_str=game_date if game_date else pd.to_datetime(df['gameDate'].iloc[0]).strftime('%Y-%m-%d')
            pitcher_id=int(df['pitcherId'].iloc[0]) if 'pitcherId' in df.columns else None
            pitcher_name=df['fullName'].iloc[0]
            szn_df=season_averages(pitcher_id=pitcher_id,
                                   pitcher_name=pitcher_name if not pitcher_id else None,
                                   before_date=game_date_str)
            game_log_df=get_game_log(pitcher_id=pitcher_id,
                                     pitcher_name=pitcher_name if not pitcher_id else None)
        except Exception as e:
            print(f" DB query failed: {e}")

    name=df['fullName'].iloc[0] if 'fullName' in df.columns else 'Pitcher'
    date_str=pd.to_datetime(df['gameDate'].iloc[0]).strftime('%B %d, %Y')
    opponent=df.get('opponent',pd.Series([''])).iloc[0]
    team=df.get('team',pd.Series([''])).iloc[0]
    level=df.get('level',pd.Series([''])).iloc[0]
    total_p=len(df)
    innings=df['inn'].nunique() if 'inn' in df.columns else '--'
    ks=df[df['pitchResult']=='Strikeout (Swinging)']['abNumInGame'].nunique()
    bbs=df[df['pitchResult']=='Walk']['abNumInGame'].nunique()
    str_pct=df['is_strike'].sum()/total_p*100
    swings=df['is_swing'].sum(); whiffs=df['is_whiff'].sum()
    whiff_pct=whiffs/swings*100 if swings else 0

    doc=SimpleDocTemplate(output_path, pagesize=letter,
        rightMargin=0.25*inch, leftMargin=0.25*inch,
        topMargin=0.25*inch, bottomMargin=0.25*inch)
    story=[]

    #  Header 
    player_id=df['pitcherId'].iloc[0] if 'pitcherId' in df.columns else None
    team_id=df['pitchingTeamId'].iloc[0] if 'pitchingTeamId' in df.columns else None
    headshot_path=headshot_override or (fetch_player_headshot(player_id,level) if player_id else None)
    teamlogo_path=team_logo_override or (fetch_team_logo(team_id) if team_id else None)

    # Remove white background from logo
    try:
        from PIL import Image as PILImage
        import numpy as _np
        _logo_img = PILImage.open(logo_path).convert('RGBA')
        _data = _np.array(_logo_img)
        r,g,b,a = _data[:,:,0],_data[:,:,1],_data[:,:,2],_data[:,:,3]
        white_mask = (r>220)&(g>220)&(b>220)
        _data[white_mask,3] = 0
        _logo_clean = PILImage.fromarray(_data)
        _logo_tmp = tempfile.NamedTemporaryFile(delete=False,suffix='.png')
        _logo_clean.save(_logo_tmp.name,'PNG')
        _logo_tmp.close()
        logo=RLImage(_logo_tmp.name,width=2.0*inch,height=0.68*inch)
    except Exception:
        logo=RLImage(logo_path,width=2.0*inch,height=0.68*inch)
    name_para=Paragraph(name,style('nm',fontName='Helvetica-Bold',fontSize=18,
                                    textColor=RL_OWHT,spaceAfter=3,leading=22))
    date_para=Paragraph(f'Game Outing Report    {date_str}',
                        style('sub',fontSize=9,textColor=RL_GR,spaceAfter=2,leading=13))
    match_para=Paragraph(f'{team}  vs  {opponent}    {level}',
                         style('sub2',fontSize=9,textColor=RL_GR,leading=13))
    info_col=Table([[name_para],[date_para],[match_para]],colWidths=[4.28*inch])
    info_col.setStyle(TableStyle([('LEFTPADDING',(0,0),(-1,-1),10),
        ('RIGHTPADDING',(0,0),(-1,-1),0),('TOPPADDING',(0,0),(-1,-1),1),
        ('BOTTOMPADDING',(0,0),(-1,-1),1),('BACKGROUND',(0,0),(-1,-1),RL_BG),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE')]))

    def _sized_image(path, max_w, max_h):
        from PIL import Image as PILImage
        img=PILImage.open(path); iw,ih=img.size; ratio=iw/ih
        if ratio>=1.0: w=min(max_w,max_h*ratio); h=w/ratio
        else: h=min(max_h,max_w/ratio); w=h*ratio
        return RLImage(path,width=w,height=h)

    headshot_cell=_sized_image(headshot_path,1.05*inch,1.05*inch) if headshot_path else Paragraph('',style('empty',fontSize=6))
    team_cell=(_sized_image(teamlogo_path,1.05*inch,0.90*inch) if teamlogo_path
               else Paragraph(team,style('teamfb',fontName='Helvetica-Bold',fontSize=14,textColor=RL_OR)))

    hdr=Table([[logo,info_col,headshot_cell,team_cell]],
              colWidths=[2.10*inch,3.70*inch,1.10*inch,1.10*inch],rowHeights=[1.18*inch])
    hdr.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'MIDDLE'),('ALIGN',(2,0),(3,0),'CENTER'),
        ('LEFTPADDING',(0,0),(-1,-1),0),('RIGHTPADDING',(0,0),(-1,-1),0),
        ('TOPPADDING',(0,0),(-1,-1),0),('BOTTOMPADDING',(0,0),(-1,-1),0),
        ('BACKGROUND',(0,0),(-1,-1),RL_BG)]))
    story.append(hdr); story.append(orange_hr(6))

    # Arsenal table
    if not szn_df.empty:
        story.extend(section_label('Pitch Arsenal Summary    This Game vs Season'))
        cmp_tbl=arsenal_comparison_table(df,szn_df,game_log_df)
        story.append(cmp_tbl)
    else:
        story.extend(section_label('Pitch Arsenal Summary'))
        sdf=pitch_stats(df)
        cw=[1.439*inch,.426*inch,.576*inch,.608*inch,.608*inch,
            .554*inch,.661*inch,.533*inch,.533*inch,.576*inch,.576*inch,.512*inch]
        tbl_data=[list(sdf.columns)]+sdf.values.tolist()
        tbl=dark_table(tbl_data,cw)
        for i,row in enumerate(sdf.itertuples(),1):
            tbl.setStyle(TableStyle([('TEXTCOLOR',(0,i),(0,i),rl_color(pc(row.Pitch))),
                                     ('FONTNAME',(0,i),(0,i),'Helvetica-Bold')]))
        story.append(tbl)
    story.append(Spacer(1,6))

    # Charts row 1
    story.extend(section_label('Pitch Visualizations'))
    fig_loc=chart_location(df,'All Pitches',show_avg=True)
    fig_mov=chart_movement_radial(df); fig_rel=chart_release(df)
    cw_s=2.58*inch; cw_m=2.62*inch; ch3=3.05*inch
    f_loc=framed_chart(RLImage(buf(fig_loc),width=cw_s-0.10*inch,height=ch3-0.10*inch),cw_s,ch3)
    f_mov=framed_chart(RLImage(buf(fig_mov),width=cw_m-0.10*inch,height=ch3-0.10*inch),cw_m,ch3)
    f_rel=framed_chart(RLImage(buf(fig_rel),width=cw_s-0.10*inch,height=ch3-0.10*inch),cw_s,ch3)
    story.append(chart_row([(f_loc,cw_s),(f_mov,cw_m),(f_rel,cw_s)]))
    story.append(Spacer(1,4))

    cw_vs=3.91*inch; ch_vs=3.0*inch
    fig_vel=chart_dot_range_single(df,'Vel','Velocity','mph')
    fig_spn=chart_dot_range_single(df,'Spin','Spin Rate','rpm')
    f_vel=framed_chart(RLImage(buf(fig_vel,140),width=cw_vs-0.10*inch,height=ch_vs-0.10*inch),cw_vs,ch_vs)
    f_spn=framed_chart(RLImage(buf(fig_spn,140),width=cw_vs-0.10*inch,height=ch_vs-0.10*inch),cw_vs,ch_vs)
    story.append(chart_row([(f_vel,cw_vs),(f_spn,cw_vs)]))
    story.append(Spacer(1,4))

    fig_cnt=chart_usage_count(df)
    f_cnt=framed_chart(RLImage(buf(fig_cnt,140),width=7.88*inch,height=2.68*inch),8.0*inch,2.80*inch)
    story.append(f_cnt); story.append(Spacer(1,4))

    cw_cnt_h=3.91*inch; ch_cnt_h=3.0*inch
    fig_cnt_rh=chart_usage_count_single(df,'R'); fig_cnt_lh=chart_usage_count_single(df,'L')
    f_cnt_rh=framed_chart(RLImage(buf(fig_cnt_rh,140),width=cw_cnt_h-0.10*inch,height=ch_cnt_h-0.10*inch),cw_cnt_h,ch_cnt_h)
    f_cnt_lh=framed_chart(RLImage(buf(fig_cnt_lh,140),width=cw_cnt_h-0.10*inch,height=ch_cnt_h-0.10*inch),cw_cnt_h,ch_cnt_h)
    story.append(chart_row([(f_cnt_rh,cw_cnt_h),(f_cnt_lh,cw_cnt_h)]))
    footer_line(story)

    #  PAGE 2: L/R Splits 
    story.append(PageBreak())
    pg2_hdr=Paragraph(f'{name}    L/R Splits    {date_str}',
        style('ph2',fontName='Helvetica-Bold',fontSize=13,textColor=RL_OWHT,spaceAfter=4))
    cw_loc_h=3.91*inch; ch_loc_h=3.85*inch
    fig_rh_loc=chart_location_single_hand(df,'R'); fig_lh_loc=chart_location_single_hand(df,'L')
    f_rh_loc=framed_chart(RLImage(buf(fig_rh_loc,140),width=cw_loc_h-0.10*inch,height=ch_loc_h-0.10*inch),cw_loc_h,ch_loc_h)
    f_lh_loc=framed_chart(RLImage(buf(fig_lh_loc,140),width=cw_loc_h-0.10*inch,height=ch_loc_h-0.10*inch),cw_loc_h,ch_loc_h)
    story.append(KeepTogether([pg2_hdr,orange_hr(6),chart_row([(f_rh_loc,cw_loc_h),(f_lh_loc,cw_loc_h)])]))
    story.append(Spacer(1,4))

    cw2h=3.91*inch; ch2h=2.7*inch
    fig_rh=chart_usage_by_hand_single(df,'R'); fig_lh=chart_usage_by_hand_single(df,'L')
    f_rh=framed_chart(RLImage(buf(fig_rh,140),width=cw2h-0.10*inch,height=ch2h-0.10*inch),cw2h,ch2h)
    f_lh=framed_chart(RLImage(buf(fig_lh,140),width=cw2h-0.10*inch,height=ch2h-0.10*inch),cw2h,ch2h)
    story.append(chart_row([(f_rh,cw2h),(f_lh,cw2h)])); story.append(Spacer(1,4))

    fig_lr_whiff=chart_lr_bar_single(df,'Whiff%','Whiff %')
    fig_lr_csw=chart_lr_bar_single(df,'CSW%','CSW %')
    f_lrw=framed_chart(RLImage(buf(fig_lr_whiff,140),width=cw2h-0.10*inch,height=ch2h-0.10*inch),cw2h,ch2h)
    f_lrc=framed_chart(RLImage(buf(fig_lr_csw,140),width=cw2h-0.10*inch,height=ch2h-0.10*inch),cw2h,ch2h)
    story.append(chart_row([(f_lrw,cw2h),(f_lrc,cw2h)])); story.append(Spacer(1,6))

    spdf=splits_stats(df)
    szn_spdf=splits_stats_season(db_path=db_path,
        pitcher_id=int(df['pitcherId'].iloc[0]) if 'pitcherId' in df.columns else None,
        pitcher_name=df['fullName'].iloc[0],
        before_date=pd.to_datetime(df['gameDate'].iloc[0]).strftime('%Y-%m-%d')
    ) if db_path else pd.DataFrame()

    def render_splits_block(spdf_in, label, label_color):
        if spdf_in.empty: return []
        block=[]
        block.append(Paragraph(label,style(f'sl_{label}',fontName='Helvetica-Bold',fontSize=10,
                                            textColor=rl_color(label_color),spaceBefore=6,spaceAfter=3)))
        cw2=[1.50*inch,0.55*inch,0.65*inch,0.65*inch,0.65*inch,0.80*inch,0.60*inch,0.60*inch,0.70*inch]
        for hand,hdr_clr in [('vs RHH',rl_color('#2980B9')),('vs LHH',rl_color('#E03434'))]:
            sub=spdf_in[spdf_in['Hand']==hand].drop(columns='Hand')
            if sub.empty: continue
            block.append(Paragraph(f'  {hand}',style(f'hh_{hand}_{label}',fontName='Helvetica-Bold',
                                                       fontSize=8,textColor=RL_OR,spaceBefore=3,spaceAfter=2)))
            td=[list(sub.columns)]+sub.values.tolist()
            t2=dark_table(td,cw2,hdr_color=hdr_clr)
            for i,row in enumerate(sub.itertuples(),1):
                t2.setStyle(TableStyle([('TEXTCOLOR',(0,i),(0,i),rl_color(pc(row.Pitch))),
                                        ('FONTNAME',(0,i),(0,i),'Helvetica-Bold')]))
            block.append(t2); block.append(thin_div(BORDER,after=3,before=3))
        return block

    if not spdf.empty:
        splits_block=[]
        splits_block+=section_label('Splits by Batter Handedness')
        splits_block+=render_splits_block(spdf,' This Game',OR)
        if not szn_spdf.empty:
            splits_block.append(Spacer(1,6))
            splits_block+=render_splits_block(szn_spdf,' Season',GR)
        story.append(KeepTogether(splits_block))
    footer_line(story)

    #  PAGE 3: Per-pitch breakdown 
    story.append(PageBreak())
    story.append(Paragraph(f'{name}    Pitch Breakdown    {date_str}',
        style('ph3',fontName='Helvetica-Bold',fontSize=13,textColor=RL_OWHT,spaceAfter=4)))
    story.append(orange_hr(6))

    for pt in df['pitchTypeFull'].unique():
        sub=df[df['pitchTypeFull']==pt]; n=len(sub); c=pc(pt)
        sw=sub['is_swing'].sum(); wh=sub['is_whiff'].sum()
        ev=sub['ExitVel_num'].dropna(); csw=(sub['is_called_strike'].sum()+wh)/n*100
        title_bar=Table([[
            Table([['']],colWidths=[0.08*inch],style=TableStyle([
                ('BACKGROUND',(0,0),(-1,-1),rl_color(c)),
                ('LEFTPADDING',(0,0),(-1,-1),0),('RIGHTPADDING',(0,0),(-1,-1),0),
                ('TOPPADDING',(0,0),(-1,-1),0),('BOTTOMPADDING',(0,0),(-1,-1),0)])),
            Paragraph(f'<b>{pt}</b>',style('ptname',fontName='Helvetica-Bold',fontSize=12,
                                            textColor=rl_color(c),leading=14)),
            Paragraph(f'{n} Pitches    {n/total_p*100:.1f}% Usage',
                      style('ptbadge',fontName='Helvetica',fontSize=8.5,textColor=RL_GR,leading=12)),
        ]],colWidths=[0.12*inch,2.4*inch,5.48*inch])
        title_bar.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),RL_BG2),
            ('BOX',(0,0),(-1,-1),0.5,RL_BORDER),('VALIGN',(0,0),(-1,-1),'MIDDLE'),
            ('LEFTPADDING',(0,0),(-1,-1),0),('RIGHTPADDING',(0,0),(-1,-1),8),
            ('TOPPADDING',(0,0),(-1,-1),7),('BOTTOMPADDING',(0,0),(-1,-1),7),
            ('LEFTPADDING',(1,0),(1,0),10)]))
        stat_labels=['Avg Velo','Max Velo','Avg Spin','Spin Eff%','IVB','Horiz Brk',
                     'Whiff%','CSW%','Avg EV','Extension']
        stat_values=[
            f"{sub['Vel'].mean():.1f} mph", f"{sub['Vel'].max():.1f} mph",
            f"{sub['Spin'].mean():.0f} rpm",
            f"{sub['SpinEff'].mean()*100:.1f}%" if pd.notna(sub['SpinEff'].mean()) else '',
            f"{sub['IndVertBrk'].mean():.1f}\"", f"{sub['HorzBrk'].mean():.1f}\"",
            f'{wh/sw*100:.1f}%' if sw else '', f'{csw:.1f}%',
            f"{ev.mean():.1f}" if len(ev) else '', f"{sub['Extension'].mean():.2f} ft",
        ]
        stat_cw=[0.8*inch]*10
        def val_color(label,val_str):
            try:
                v=float(val_str.split()[0].replace('%','').replace('"',''))
                if label=='Whiff%' and v>=25: return OR
                if label=='CSW%' and v>=30: return OR
                if label=='Avg Velo' and v>=93: return OR
            except: pass
            return OFF_WHT
        lbl_cells=[Paragraph(f'<b>{l}</b>',style(f'sl{i}',fontName='Helvetica-Bold',fontSize=7,
                              textColor=RL_GR,alignment=TA_CENTER)) for i,l in enumerate(stat_labels)]
        val_cells=[Paragraph(f'<b>{v}</b>',style(f'sv{i}',fontName='Helvetica-Bold',fontSize=9.5,
                              textColor=rl_color(val_color(stat_labels[i],v)),alignment=TA_CENTER))
                   for i,v in enumerate(stat_values)]
        stat_tbl=Table([lbl_cells,val_cells],colWidths=stat_cw)
        stat_tbl.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),RL_BG2),
            ('BOX',(0,0),(-1,-1),0.5,RL_BORDER),('GRID',(0,0),(-1,-1),0.3,RL_BG3),
            ('ALIGN',(0,0),(-1,-1),'CENTER'),('VALIGN',(0,0),(-1,-1),'MIDDLE'),
            ('TOPPADDING',(0,0),(-1,0),4),('BOTTOMPADDING',(0,0),(-1,0),3),
            ('TOPPADDING',(0,1),(-1,1),5),('BOTTOMPADDING',(0,1),(-1,1),6),
            ('LEFTPADDING',(0,0),(-1,-1),2),('RIGHTPADDING',(0,0),(-1,-1),2),
            ('LINEBELOW',(0,0),(-1,0),0.4,RL_BORDER)]))
        fig_pt=chart_location(sub,f'{pt}  Location',w=2.6,h=3.0,show_avg=True)
        fig_seq=chart_velo_seq(sub,pt,w=5.2,h=2.4)
        cw_loc=2.62*inch; cw_seq=5.26*inch; ch_loc=3.05*inch; ch_seq=2.55*inch
        f_pt=framed_chart(RLImage(buf(fig_pt),width=cw_loc-0.12*inch,height=ch_loc-0.12*inch),cw_loc,ch_loc)
        f_seq=framed_chart(RLImage(buf(fig_seq),width=cw_seq-0.12*inch,height=ch_seq-0.12*inch),cw_seq,ch_seq)
        story.append(KeepTogether([title_bar,Spacer(1,4),stat_tbl,Spacer(1,4),
                                   chart_row([(f_pt,cw_loc),(f_seq,cw_seq)])]))
        story.append(thin_div(BORDER,after=4,before=3))
    footer_line(story)

    #  PAGE 4: Pitch log 
    story.append(PageBreak())
    story.append(Paragraph(f'{name}    Pitch Log    {date_str}',
        style('ph4',fontName='Helvetica-Bold',fontSize=13,textColor=RL_OWHT,spaceAfter=4)))
    story.append(orange_hr(6))
    log_cols=['pitchNumInGame','inn','outs','count_fixed','batterAbbrevName',
              'batterHand','pitchTypeFull','Vel','Spin','IndVertBrk','HorzBrk','pitchResult','ExitVel']
    log_labels=['#','Inn','Outs','Count','Batter','Hand','Pitch Type','Velo','Spin','IVB','HB','Result','EV']
    log_df=df[log_cols].copy().reset_index(drop=True)
    log_df['Vel']=log_df['Vel'].apply(lambda x: f'{x:.1f}' if pd.notna(x) else '')
    log_df['Spin']=log_df['Spin'].apply(lambda x: f'{int(x)}' if pd.notna(x) else '')
    log_df['IndVertBrk']=log_df['IndVertBrk'].apply(lambda x: f'{x:.1f}"' if pd.notna(x) else '')
    log_df['HorzBrk']=log_df['HorzBrk'].apply(lambda x: f'{x:.1f}"' if pd.notna(x) else '')
    log_df['pitchNumInGame']=range(1,len(log_df)+1)
    counts_before=[]; balls=0; strikes=0; cur_ab=None
    for _,row in df.iterrows():
        ab=row.get('abNumInGame')
        if ab!=cur_ab: balls=0; strikes=0; cur_ab=ab
        counts_before.append(f'{balls}-{strikes}')
        result=row['pitchResult']
        if result in ('Ball','Ball In Dirt','Intentional Ball','Hit By Pitch'): balls=min(balls+1,3)
        elif result in ('Strike Looking','Strike Swinging','Foul','Foul Tip','Missed Bunt'):
            if strikes<2: strikes+=1
        elif result in ('Strikeout (Looking)','Strikeout (Swinging)','Walk','Hit By Pitch',
                        'Single','Double','Triple','Home Run','Ground Out','Fly Out','Line Out',
                        'Pop Out','Sac Fly','Sac Bunt','Field Error','Fielders Choice',
                        'Double Play','Triple Play'):
            balls=0; strikes=0
    log_df['count_fixed']=counts_before
    log_cw=[0.373*inch,0.560*inch,0.408*inch,0.467*inch,0.839*inch,0.408*inch,0.991*inch,
            0.489*inch,0.489*inch,0.443*inch,0.443*inch,1.224*inch,0.467*inch]
    log_data=[log_labels]
    for _,row in log_df.iterrows():
        log_data.append([str(row[c]) if pd.notna(row[c]) else '' for c in log_cols])
    log_tbl=Table(log_data,colWidths=log_cw,repeatRows=1)
    ls=[('BACKGROUND',(0,0),(-1,0),RL_OR),('TEXTCOLOR',(0,0),(-1,0),RL_BG),
        ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),('FONTSIZE',(0,0),(-1,-1),6.8),
        ('FONTNAME',(0,1),(-1,-1),'Helvetica'),('TEXTCOLOR',(0,1),(-1,-1),RL_OWHT),
        ('ALIGN',(0,0),(-1,-1),'CENTER'),('ALIGN',(4,1),(4,-1),'LEFT'),
        ('ALIGN',(11,1),(11,-1),'LEFT'),('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3),
        ('LEFTPADDING',(0,0),(-1,-1),3),('RIGHTPADDING',(0,0),(-1,-1),3),
        ('GRID',(0,0),(-1,-1),0.25,RL_BORDER)]
    for i,row in enumerate(log_df.itertuples(),1):
        bg=RL_BG2 if i%2==1 else RL_BG3
        ls.append(('BACKGROUND',(0,i),(-1,i),bg))
        ls.append(('TEXTCOLOR',(6,i),(6,i),rl_color(pc(row.pitchTypeFull))))
        ls.append(('FONTNAME',(6,i),(6,i),'Helvetica-Bold'))
        result=str(row.pitchResult)
        if 'Swinging' in result or result=='Strike Swinging':
            ls.append(('TEXTCOLOR',(11,i),(11,i),rl_color('#FF4C4C')))
            ls.append(('FONTNAME',(11,i),(11,i),'Helvetica-Bold'))
        elif result=='Strike Looking':
            ls.append(('TEXTCOLOR',(11,i),(11,i),rl_color('#4DA6FF')))
        elif result=='Ball':
            ls.append(('TEXTCOLOR',(11,i),(11,i),RL_GR))
    log_tbl.setStyle(TableStyle(ls))
    story.append(log_tbl)
    story.append(thin_div(BORDER,after=3,before=4))
    story.append(Paragraph(
        '<b>Result Key:</b>  '
        f'<font color="#FF4C4C"> Swinging Strike / K</font>  '
        f'<font color="#4DA6FF"> Called Strike</font>  '
        f'<font color="{GR}"> Ball</font>  Black = In Play / Other',
        style('leg',fontSize=7.5,textColor=RL_GR,spaceAfter=4)))
    footer_line(story)

    #  PAGE 5: Season Trends 
    if not game_log_df.empty:
        story.append(PageBreak())
        story.append(Paragraph(f'{name}    Season Trends    {date_str}',
            style('ph5',fontName='Helvetica-Bold',fontSize=13,textColor=RL_OWHT,spaceAfter=4)))
        story.append(orange_hr(6))
        story.extend(section_label('Season Trends    Per Pitch Type'))
        story.append(Paragraph(
            f'<font color="{OR}"></font>  = This game    '
            f'<font color="{GR}">- -</font>  = Season avg    '
            f'<font color="{OR}"></font> MLB  '
            f'<font color="#4DA6FF"></font> AAA  '
            f'<font color="#A0A0A0"></font> EXB/SP',
            style('leg5',fontSize=7.5,textColor=RL_GR,spaceAfter=6)))

        pitch_types_ordered=df['pitchTypeFull'].value_counts().index.tolist()
        current_date_str=pd.to_datetime(df['gameDate'].iloc[0]).strftime('%Y-%m-%d')
        trend_charts=chart_trends(game_log_df,current_date_str,pitch_types_ordered)

        for pt, chart_buf in trend_charts:
            pt_color=pc(pt)
            img=RLImage(chart_buf,width=7.78*inch,height=2.18*inch)
            framed=framed_chart(img,8.0*inch,2.30*inch)
            story.append(KeepTogether([
                Paragraph(f'<b>{pt}</b>',style(f'pt_{pt}',fontName='Helvetica-Bold',
                           fontSize=8,textColor=rl_color(pt_color),spaceBefore=6,spaceAfter=2)),
                framed, Spacer(1,4)]))
        footer_line(story)

    doc.build(story, onFirstPage=_dark_bg, onLaterPages=_dark_bg)
    print(f' Saved  {output_path}')


def build_report_from_db(player_name, game_date, logo_path, output_path,
                         headshot_override=None, team_logo_override=None):
    """
    Generate a report entirely from Supabase -- no file upload needed.
    Fetches the game's pitch data directly from the DB and passes it
    to build_report as an in-memory DataFrame.
    """
    from pwrx_db import get_conn
    import tempfile

    conn = get_conn()
    df = pd.read_sql(
        "SELECT * FROM pitches WHERE pitcher_name = %s AND game_date = %s",
        conn, params=[player_name, game_date]
    )
    conn.close()

    if df.empty:
        raise ValueError(f"No pitches found for {player_name} on {game_date}")

    # Remap DB column names back to Trackman names expected by build_report
    col_map = {
        'pitcher_id':       'pitcherId',
        'pitcher_name':     'fullName',
        'game_date':        'gameDate',
        'game_id':          'gameId',
        'pitch_num':        'pitchNumInGame',
        'pitch_type':       'pitchType',
        'pitch_type_full':  'pitchTypeFull',
        'batter_hand':      'batterHand',
        'count_str':        'count',
        'inning':           'inn',
        'pitch_result':     'pitchResult',
        'pitch_outcome':    'pitchOutcome',
        'vel':              'Vel',
        'spin':             'Spin',
        'spin_eff':         'SpinEff',
        'ivb':              'IndVertBrk',
        'hb':               'HorzBrk',
        'rel_x':            'RelX',
        'rel_z':            'RelZ',
        'extension':        'Extension',
        'vert_appr_angle':  'VertApprAngle',
        'horz_appr_angle':  'HorzApprAngle',
        'exit_vel':         'ExitVel',
        'launch_ang':       'LaunchAng',
    }
    df = df.rename(columns=col_map)

    # Restore x/y from px/pz (px was stored as -x for catcher view)
    # build_report's load_data will re-apply the flip, so we reverse it here
    if 'px' in df.columns:
        df['x'] = -df['px']
    if 'pz' in df.columns:
        df['y'] = df['pz'] - 2.5

    # Write to a temp XLSX so build_report can use load_data as normal
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
    df.to_excel(tmp.name, index=False)
    tmp.close()

    try:
        build_report(
            data_path=tmp.name,
            logo_path=logo_path,
            output_path=output_path,
            headshot_override=headshot_override,
            team_logo_override=team_logo_override,
            db_path=None,
            game_date=game_date,
            player_name=player_name
        )
    finally:
        if os.path.exists(tmp.name):
            os.unlink(tmp.name)


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'season':
        # Full season file  run report for most recent game
        build_report(
            data_path = '/home/claude/houser_7-3-25.xlsx',
            logo_path = '/home/claude/pwrx_logo.png',
            output_path = '/home/claude/houser_7-3-25_report.pdf',
            db_path   = '/home/claude/pitchingwrx.db',
        )
    else:
        build_report(
            data_path = '/home/claude/houser_7-3-25.xlsx',
            logo_path = '/home/claude/pwrx_logo.png',
            output_path = '/home/claude/houser_7-3-25_report.pdf',
            db_path   = '/home/claude/pitchingwrx.db',
        )
