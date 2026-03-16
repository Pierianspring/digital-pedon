"""
experiment4_heat_flux_3d.py  —  transparent background version
"""

import csv
import math
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib import colors
from mpl_toolkits.mplot3d import Axes3D   # noqa: F401

from digital_pedon import build_pedon

# =============================================================================
#  STYLE BLOCK
# =============================================================================
STYLE = {

    # ── Colours ───────────────────────────────────────────────────────────────
    "color_bg":          "none",      # "none" = transparent background
    "color_title":       "#1F4E79",
    "color_label":       "#1F4E79",
    "color_3d_face":     "none",      # 3D axes background also transparent
    "color_hz_line":     "white",
    "color_hz_label":    "black",
    "color_contour":     "black",
    "color_spine":       "#BBBBBB",
    "color_annot_bg":    "#EEF3FB",
    "color_annot_edge":  "#1F4E79",
    "color_annot_text":  "#2E4057",

    # ── Colormap ──────────────────────────────────────────────────────────────
    "cmap":  "RdYlBu_r",

    # ── Figure dimensions ─────────────────────────────────────────────────────
    "fig_width":    16,
    "fig_height":    8,
    "fig_dpi":       500,
    "out_file":     "experiment4_heat_3d.png",

    # ── Layout ────────────────────────────────────────────────────────────────
    "gs_width_ratios": [1.4, 1.0],
    "gs_wspace":        0.1,
    "gs_left":          0.02,
    "gs_right":         0.97,
    "gs_top":           0.90,
    "gs_bottom":        0.08,

    # ── Fonts ─────────────────────────────────────────────────────────────────
    "font_family":           "DejaVu Sans",
    "font_size_suptitle":    10,
    "font_size_panel_title": 16,
    "font_size_axis_label":  14,
    "font_size_tick":        14,
    "font_size_hz_label":    14,
    "font_size_contour":     14,
    "font_size_annot":       14,
    "font_size_colorbar":    14,
    "font_size_colorbar_tick": 14,
    "font_weight_title":    "bold",

    # ── 3D surface ────────────────────────────────────────────────────────────
    "surf_alpha":   0.92,
    "surf_rcount":  80,
    "surf_ccount":  80,
    "view_elev":    28,
    "view_azim":   -50,

    # ── Horizon boundary lines ────────────────────────────────────────────────
    "hz_lw":        1.0,
    "hz_alpha":     0.70,
    "hz_linestyle": "--",

    # ── 2D heatmap contours ────────────────────────────────────────────────────
    "contour_interval":  2,
    "contour_lw":        0.45,
    "contour_alpha":     0.55,
    "contour_label_fmt": "%.0f°C",

    # ── Colorbars ─────────────────────────────────────────────────────────────
    "cbar3d_shrink":  0.45,
    "cbar3d_aspect":  12,
    "cbar3d_pad":     0.04,
    "cbar2d_shrink":  0.85,
    "cbar2d_aspect":  18,
    "cbar2d_pad":     0.02,

    # ── Annotation box ────────────────────────────────────────────────────────
    "annot_x":  0.845,
    "annot_y":  0.50,

    # ── Spine ─────────────────────────────────────────────────────────────────
    "spine_lw": 0.8,

    # ── Tick rotation ─────────────────────────────────────────────────────────
    "xtick_rotation_3d": 15,
    "xtick_rotation_2d": 45,
}
# =============================================================================

# -----------------------------------------------------------------------------
# 1.  PROFILE
# -----------------------------------------------------------------------------
pedon = build_pedon({
    "site_name": "E4 — Ghent Reference Profile",
    "latitude":   51.05, "longitude": 3.72,
    "horizons": [
        {"designation": "Ap", "depth_top_cm":   0, "depth_bottom_cm":  28, "soil_type": "loamy_topsoil"},
        {"designation": "Bw", "depth_top_cm":  28, "depth_bottom_cm":  70, "soil_type": "clay_subsoil"},
        {"designation": "BC", "depth_top_cm":  70, "depth_bottom_cm": 110, "soil_type": "sandy_loam"},
        {"designation": "C",  "depth_top_cm": 110, "depth_bottom_cm": 200, "soil_type": "sandy_loam"},
    ],
})
HORIZONS = [
    {"id": "Ap", "z_top":   0, "z_bot":  28},
    {"id": "Bw", "z_top":  28, "z_bot":  70},
    {"id": "BC", "z_top":  70, "z_bot": 110},
    {"id": "C",  "z_top": 110, "z_bot": 200},
]

# -----------------------------------------------------------------------------
# 2.  SEASONAL MOISTURE
# -----------------------------------------------------------------------------
def theta_seasonal(hz_id, t_days):
    params = {"Ap":(0.28,0.08,0.0),"Bw":(0.35,0.05,30.0),"BC":(0.22,0.03,60.0),"C":(0.18,0.015,90.0)}
    mean, amp, lag = params[hz_id]
    return mean + amp * math.cos(2 * math.pi * (t_days - lag) / 365.0)

# -----------------------------------------------------------------------------
# 3.  SURFACE TEMPERATURE
# -----------------------------------------------------------------------------
T_MEAN=10.5; T_AMP=9.5; T_PHASE=196.0
def T_surface(t): return T_MEAN + T_AMP * math.sin(2*math.pi*(t-T_PHASE)/365.0)

# -----------------------------------------------------------------------------
# 4.  GRID
# -----------------------------------------------------------------------------
Z_MAX=200; DZ=5.0; NZ=int(Z_MAX/DZ)+1; DT_SIM=1.0; N_DAYS=730; SAVE_EVERY=5
z_nodes = np.arange(0, NZ) * DZ
def horizon_for_depth(z):
    for h in HORIZONS:
        if h["z_top"] <= z < h["z_bot"]: return h["id"]
    return "C"

# -----------------------------------------------------------------------------
# 5.  SIMULATION
# -----------------------------------------------------------------------------
print("Running simulation...")
def solve_tridiagonal(a,b,c,d):
    n=len(b); c_=np.zeros(n); d_=np.zeros(n); x=np.zeros(n)
    c_[0]=c[0]/b[0]; d_[0]=d[0]/b[0]
    for i in range(1,n):
        m=b[i]-a[i]*c_[i-1]
        c_[i]=c[i]/m if i<n-1 else 0.0
        d_[i]=(d[i]-a[i]*d_[i-1])/m
    x[-1]=d_[-1]
    for i in range(n-2,-1,-1): x[i]=d_[i]-c_[i]*x[i+1]
    return x

T_field=np.ones(NZ)*T_MEAN; output_days=[]; output_T_grid=[]
for step in range(N_DAYS+1):
    t_day=float(step); day_of_year=t_day%365.0
    DT_arr=np.zeros(NZ)
    for iz in range(NZ):
        hz_id=horizon_for_depth(z_nodes[iz])
        theta=theta_seasonal(hz_id,day_of_year)
        res=pedon.update_sync({"horizon_id":hz_id,"volumetric_water_content":theta,"soil_temperature_c":float(T_field[iz])})
        DT_arr[iz]=res["derived"]["thermal_diffusivity_cm2_day"]
    r=DT_arr*DT_SIM/DZ**2
    T_surf_new=T_surface(day_of_year); T_surf_old=T_surface((day_of_year-1)%365.0)
    n_int=NZ-2; a=np.zeros(n_int); b=np.zeros(n_int); c=np.zeros(n_int); d=np.zeros(n_int)
    for i in range(n_int):
        iz=i+1; ri=r[iz]; a[i]=-0.5*ri; b[i]=1.0+ri; c[i]=-0.5*ri
        d[i]=0.5*ri*T_field[iz-1]+(1.0-ri)*T_field[iz]+0.5*ri*T_field[iz+1]
    d[0]+=0.5*r[1]*T_surf_new+0.5*r[1]*T_surf_old; b[-1]+=c[-1]; c[-1]=0.0
    T_int=solve_tridiagonal(a,b,c,d)
    T_new=np.empty(NZ); T_new[0]=T_surf_new; T_new[1:-1]=T_int; T_new[-1]=T_new[-2]; T_field=T_new
    if t_day>=365.0 and step%SAVE_EVERY==0:
        output_days.append(t_day-365.0); output_T_grid.append(T_field.copy())
print(f"  Done: {len(output_days)} snapshots")

# -----------------------------------------------------------------------------
# 6.  ARRAYS
# -----------------------------------------------------------------------------
days_arr=np.array(output_days); T_matrix=np.array(output_T_grid)
depth_plot=z_nodes; T_plot=T_matrix
DAYS_GRID,DEPTH_GRID=np.meshgrid(days_arr,depth_plot,indexing="ij")

# -----------------------------------------------------------------------------
# 7.  CSV
# -----------------------------------------------------------------------------
with open("experiment4_heat_data.csv","w",newline="") as f:
    writer=csv.writer(f)
    writer.writerow(["day_of_year"]+[f"T_depth_{d}cm" for d in depth_plot])
    for i,day in enumerate(days_arr): writer.writerow([round(day,1)]+[round(v,3) for v in T_plot[i]])
print("CSV saved")

# -----------------------------------------------------------------------------
# 8.  RCPARAMS
# -----------------------------------------------------------------------------
matplotlib.rcParams.update({
    "font.family":      STYLE["font_family"],
    "font.size":        STYLE["font_size_axis_label"],
    "axes.titlesize":   STYLE["font_size_panel_title"],
    "axes.titleweight": STYLE["font_weight_title"],
    "axes.labelsize":   STYLE["font_size_axis_label"],
    "figure.facecolor": STYLE["color_bg"],
    "axes.facecolor":   STYLE["color_bg"],
    "xtick.labelsize":  STYLE["font_size_tick"],
    "ytick.labelsize":  STYLE["font_size_tick"],
})

# -----------------------------------------------------------------------------
# 9.  FIGURE
# -----------------------------------------------------------------------------
T_min=T_matrix.min(); T_max=T_matrix.max()
norm=colors.Normalize(vmin=T_min,vmax=T_max)
cmap=plt.get_cmap(STYLE["cmap"])
month_days  =[0,31,59,90,120,151,181,212,243,273,304,334]
month_names =["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

fig=plt.figure(figsize=(STYLE["fig_width"],STYLE["fig_height"]),facecolor=STYLE["color_bg"])
gs=gridspec.GridSpec(1,2,figure=fig,
    width_ratios=STYLE["gs_width_ratios"],wspace=STYLE["gs_wspace"],
    left=STYLE["gs_left"],right=STYLE["gs_right"],top=STYLE["gs_top"],bottom=STYLE["gs_bottom"])

# -----------------------------------------------------------------------------
# 10.  3D PANEL
# -----------------------------------------------------------------------------
ax3d=fig.add_subplot(gs[0,0],projection="3d")
ax3d.set_facecolor(STYLE["color_3d_face"])
# Make the 3D pane walls transparent
ax3d.xaxis.pane.fill=False; ax3d.yaxis.pane.fill=False; ax3d.zaxis.pane.fill=False

surf=ax3d.plot_surface(DAYS_GRID,DEPTH_GRID,T_plot,cmap=cmap,norm=norm,
    alpha=STYLE["surf_alpha"],linewidth=0,antialiased=True,
    rcount=STYLE["surf_rcount"],ccount=STYLE["surf_ccount"])

for hz in HORIZONS[1:]:
    ax3d.plot([days_arr.min(),days_arr.max()],[hz["z_top"],hz["z_top"]],[T_min,T_min],
              color=STYLE["color_hz_line"],linewidth=STYLE["hz_lw"],
              alpha=STYLE["hz_alpha"],linestyle=STYLE["hz_linestyle"],zorder=2)
    ax3d.text(days_arr.max()+5,hz["z_top"],T_min-0.5,hz["id"],
              fontsize=STYLE["font_size_hz_label"],color=STYLE["color_hz_label"],alpha=0.85)

ax3d.set_xticks(month_days)
ax3d.set_xticklabels(month_names,fontsize=STYLE["font_size_tick"],rotation=STYLE["xtick_rotation_3d"])
ax3d.tick_params(axis="both",labelsize=STYLE["font_size_tick"])
ax3d.set_xlabel("Month",fontsize=STYLE["font_size_axis_label"],labelpad=8,color=STYLE["color_label"])
ax3d.set_ylabel("Depth (cm)",fontsize=STYLE["font_size_axis_label"],labelpad=8,color=STYLE["color_label"])
ax3d.set_zlabel("Temperature (°C)",fontsize=STYLE["font_size_axis_label"],labelpad=8,color=STYLE["color_label"])
ax3d.set_title("(a)  3D Temperature Field  T(depth, time)",
               fontsize=STYLE["font_size_panel_title"],fontweight=STYLE["font_weight_title"],
               color=STYLE["color_title"],pad=12)
ax3d.set_ylim(200,0); ax3d.set_xlim(0,365); ax3d.set_zlim(T_min-0.5,T_max+0.5)
ax3d.view_init(elev=STYLE["view_elev"],azim=STYLE["view_azim"])

cbar3d=fig.colorbar(surf,ax=ax3d,shrink=STYLE["cbar3d_shrink"],aspect=STYLE["cbar3d_aspect"],
                    pad=STYLE["cbar3d_pad"],location="right")
cbar3d.set_label("T (°C)",fontsize=STYLE["font_size_colorbar"])
cbar3d.ax.tick_params(labelsize=STYLE["font_size_colorbar_tick"])

# -----------------------------------------------------------------------------
# 11.  2D PANEL
# -----------------------------------------------------------------------------
ax2d=fig.add_subplot(gs[0,1]); ax2d.set_facecolor(STYLE["color_bg"])
im=ax2d.pcolormesh(days_arr,depth_plot,T_plot.T,cmap=cmap,norm=norm,shading="gouraud")

for hz in HORIZONS[1:]:
    ax2d.axhline(hz["z_top"],color=STYLE["color_hz_line"],linewidth=STYLE["hz_lw"],
                 linestyle=STYLE["hz_linestyle"],alpha=STYLE["hz_alpha"])
    ax2d.text(5,hz["z_top"]+3,hz["id"],fontsize=STYLE["font_size_hz_label"],
              color=STYLE["color_hz_label"],fontweight="bold",alpha=0.9)
ax2d.text(5,5,"Ap",fontsize=STYLE["font_size_hz_label"],color=STYLE["color_hz_label"],
          fontweight="bold",alpha=0.9)

contours=ax2d.contour(days_arr,depth_plot,T_plot.T,
    levels=np.arange(math.floor(T_min),math.ceil(T_max)+1,STYLE["contour_interval"]),
    colors=STYLE["color_contour"],linewidths=STYLE["contour_lw"],alpha=STYLE["contour_alpha"])
ax2d.clabel(contours,inline=True,fontsize=STYLE["font_size_contour"],
            fmt=STYLE["contour_label_fmt"],inline_spacing=2)

ax2d.set_xticks(month_days)
ax2d.set_xticklabels(month_names,fontsize=STYLE["font_size_tick"],
                     rotation=STYLE["xtick_rotation_2d"],ha="right")
ax2d.set_xlim(0,365); ax2d.set_ylim(200,0)
ax2d.set_ylabel("Depth (cm)",fontsize=STYLE["font_size_axis_label"],color=STYLE["color_label"])
ax2d.set_xlabel("Month",fontsize=STYLE["font_size_axis_label"],color=STYLE["color_label"])
ax2d.set_title("(b)  2D Depth-Time Heatmap  T(depth, time)",
               fontsize=STYLE["font_size_panel_title"],fontweight=STYLE["font_weight_title"],
               color=STYLE["color_title"],pad=8)
for spine in ax2d.spines.values():
    spine.set_color(STYLE["color_spine"]); spine.set_linewidth(STYLE["spine_lw"])

cbar2d=fig.colorbar(im,ax=ax2d,shrink=STYLE["cbar2d_shrink"],aspect=STYLE["cbar2d_aspect"],
                    pad=STYLE["cbar2d_pad"])
cbar2d.set_label("Temperature (°C)",fontsize=STYLE["font_size_colorbar"])
cbar2d.ax.tick_params(labelsize=STYLE["font_size_colorbar_tick"])

# -----------------------------------------------------------------------------
# 13.  SAVE  — transparent=True removes any residual background colour
# -----------------------------------------------------------------------------
fig.savefig(STYLE["out_file"],dpi=STYLE["fig_dpi"],bbox_inches="tight",
            facecolor=STYLE["color_bg"],transparent=True)
print(f"\nFigure saved: {STYLE['out_file']}  ({STYLE['fig_dpi']} dpi)  [transparent background]")
plt.show()

# -----------------------------------------------------------------------------
# 14.  DIAGNOSTICS
# -----------------------------------------------------------------------------
print(f"\nTemperature range: {T_min:.1f} to {T_max:.1f} °C")
T_surf_series=T_plot[:,0]; T_50cm_series=T_plot[:,10]; T_bot_series=T_plot[:,-1]
print(f"Amplitude at surface: {T_surf_series.max()-T_surf_series.min():.1f} °C")
print(f"Amplitude at 50 cm:   {T_50cm_series.max()-T_50cm_series.min():.1f} °C")
print(f"Phase lag at 50 cm:   {((np.argmax(T_50cm_series)-np.argmax(T_surf_series))*SAVE_EVERY):.0f} days")