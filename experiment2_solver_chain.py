"""
experiment2_solver_chain.py  —  3-panel row version (a, b, c only)
"""

import csv
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.lines import Line2D

from digital_pedon import build_pedon

# =============================================================================
#  STYLE BLOCK
# =============================================================================
STYLE = {
    "color_a":  "#1F4E79",
    "color_b":  "#2E4057",
    "color_c":  "#6B3D14",
    "color_d":  "#1A5C2A",
    "color_e":  "#8B2500",
    "color_f":  "#8B6914",
    "color_sat_line":   "#1F4E79",
    "color_wp_line":    "#8B2500",
    "color_ref_hline":  "#2E4057",
    "color_bg":         "none",    # Changed to 'none' for transparency
    "color_grid":       "#E0DDD8",
    "color_title":      "#1F4E79",
    "color_annot":      "#2E4057",

    # ── Panel selection ───────────────────────────────────────────────────────
    "show_a": True,    # (a) Matric Potential
    "show_b": False,    # (b) Effective Saturation
    "show_c": True,    # (c) Hydraulic Conductivity
    "show_d": False,   # (d) Darcy-Buckingham Flux
    "show_e": True,   # (e) Soil Respiration
    "show_f": False,   # (f) Thermal Diffusivity

    # ── Number of columns — set to 3 for a single row ─────────────────────────
    "n_cols":  3,

    # ── Figure dimensions ─────────────────────────────────────────────────────
    "fig_width":    14,
    "fig_height":    6,
    "fig_dpi":       500,
    "out_file":     "experiment2_results.png",

    # ── Layout ────────────────────────────────────────────────────────────────
    "gs_hspace":    0.45,
    "gs_wspace":    0.38,
    "gs_left":      0.07,
    "gs_right":     0.97,
    "gs_top":       0.88,
    "gs_bottom":    0.19,

    # ── Fonts ─────────────────────────────────────────────────────────────────
    "font_family":          "DejaVu Sans",
    "font_size_suptitle":   11,
    "font_size_panel_title":16,
    "font_size_axis_label":  14,
    "font_size_tick":        14,
    "font_size_legend":      14,
    "font_size_annotation":  14,
    "font_weight_title":    "bold",

    # ── Line widths ───────────────────────────────────────────────────────────
    "lw_main":       2.2,
    "lw_refline":    2.2,
    "lw_vline":      2.0,
    "lw_legend":     1.2,

    # ── Fill / shading ────────────────────────────────────────────────────────
    "fill_alpha":      0.2,
    "fill_alpha_zone": 0.2,

    # ── Markers ───────────────────────────────────────────────────────────────
    "marker_size":   70,
    "marker_lw":     1.5,

    # ── Thresholds ────────────────────────────────────────────────────────────
    "theta_sat":     0.44,
    "theta_wp":      0.09,
    "theta_s_loam":  0.48,
    "Ks_ref":        15.0,

    # ── Ticks / spines ────────────────────────────────────────────────────────
    "tick_direction":   "in",
    "tick_length":       3,
    "hide_top_spine":   True,
    "hide_right_spine": True,

    # ── Grid ──────────────────────────────────────────────────────────────────
    "grid_linewidth": 0.6,
    "grid_alpha":     0.80,

    # ── Legend ────────────────────────────────────────────────────────────────
    "legend_ncol":       2,
    "legend_framealpha": 0.85,
    "legend_y":          0.01,
}
# =============================================================================

# -----------------------------------------------------------------------------
# 1.  BUILD PEDON
# -----------------------------------------------------------------------------
pedon = build_pedon({
    "site_name": "Experiment 2 — Loamy Profile",
    "latitude":   51.05,
    "longitude":   3.72,
    "thresholds": {
        "theta_saturation": STYLE["theta_sat"],
        "theta_wilting_pt":  STYLE["theta_wp"],
    },
    "horizons": [
        {"designation": "Ap", "depth_top_cm": 0,  "depth_bottom_cm": 28, "soil_type": "loamy_topsoil"},
        {"designation": "Bw", "depth_top_cm": 28, "depth_bottom_cm": 70, "soil_type": "clay_subsoil"},
    ],
})

# -----------------------------------------------------------------------------
# 2.  RUN SWEEP
# -----------------------------------------------------------------------------
theta_values = [round(0.08 + i * 0.005, 3) for i in range(72)]
soil_temp    = 15.0

records = []
for theta in theta_values:
    pedon.update_sync({"horizon_id": "Bw", "volumetric_water_content": 0.32, "soil_temperature_c": 12.0})
    result = pedon.update_sync({"horizon_id": "Ap", "volumetric_water_content": theta, "soil_temperature_c": soil_temp})
    d = result["derived"]
    records.append({
        "theta":          theta,
        "psi_cm":         d.get("matric_potential_cm",           float("nan")),
        "Se":             d.get("effective_saturation",          float("nan")),
        "K_cm_day":       d.get("hydraulic_conductivity_cm_day", float("nan")),
        "flux_cm_day":    result.get("flux_down_cm_day",         float("nan")),
        "co2_mg_cm3_day": d.get("soil_respiration_mg_C_cm3_day", float("nan")),
        "thermal_diff":   d.get("thermal_diffusivity_cm2_day",   float("nan")),
    })

# -----------------------------------------------------------------------------
# 3.  CSV
# -----------------------------------------------------------------------------
with open("experiment2_results.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=records[0].keys())
    writer.writeheader()
    writer.writerows(records)
print("CSV saved: experiment2_results.csv")

# -----------------------------------------------------------------------------
# 4.  ARRAYS
# -----------------------------------------------------------------------------
theta   = [r["theta"]          for r in records]
psi_abs = [abs(r["psi_cm"])    for r in records]
Se      = [r["Se"]             for r in records]
K       = [r["K_cm_day"]       for r in records]
flux    = [r["flux_cm_day"]    for r in records]
co2     = [r["co2_mg_cm3_day"] for r in records]
DT      = [r["thermal_diff"]   for r in records]

# -----------------------------------------------------------------------------
# 5.  RCPARAMS
# -----------------------------------------------------------------------------
matplotlib.rcParams.update({
    "font.family":       STYLE["font_family"],
    "font.size":         STYLE["font_size_axis_label"],
    "axes.titlesize":    STYLE["font_size_panel_title"],
    "axes.titleweight":  STYLE["font_weight_title"],
    "axes.labelsize":    STYLE["font_size_axis_label"],
    "axes.spines.top":   not STYLE["hide_top_spine"],
    "axes.spines.right": not STYLE["hide_right_spine"],
    "axes.grid":         True,
    "grid.color":        STYLE["color_grid"],
    "grid.linewidth":    STYLE["grid_linewidth"],
    "grid.alpha":        STYLE["grid_alpha"],
    "figure.facecolor":  "none",     # Set for transparency
    "axes.facecolor":    "none",     # Set for transparency
    "xtick.labelsize":   STYLE["font_size_tick"],
    "ytick.labelsize":   STYLE["font_size_tick"],
    "lines.linewidth":   STYLE["lw_main"],
    "legend.fontsize":   STYLE["font_size_legend"],
    "legend.framealpha": STYLE["legend_framealpha"],
})

# -----------------------------------------------------------------------------
# 6.  FIGURE & GRID
# -----------------------------------------------------------------------------
ALL_PANELS = ["a", "b", "c", "d", "e", "f"]
active     = [p for p in ALL_PANELS if STYLE.get(f"show_{p}", True)]
n_cols     = STYLE["n_cols"]
n_rows     = (len(active) + n_cols - 1) // n_cols
panel_pos  = {p: divmod(i, n_cols) for i, p in enumerate(active)}
_labels    = "abcdefghijklmnopqrstuvwxyz"
label_map  = {p: _labels[i] for i, p in enumerate(active)}

fig = plt.figure(figsize=(STYLE["fig_width"], STYLE["fig_height"]), facecolor="none")
gs  = gridspec.GridSpec(
    n_rows, n_cols, figure=fig,
    hspace=STYLE["gs_hspace"], wspace=STYLE["gs_wspace"],
    left=STYLE["gs_left"],    right=STYLE["gs_right"],
    top=STYLE["gs_top"],      bottom=STYLE["gs_bottom"],
)

# -----------------------------------------------------------------------------
# 7.  PANEL HELPER
# -----------------------------------------------------------------------------
def make_panel(panel_letter, title, xlabel, ylabel, color, ys, xs=None, yscale="linear"):
    row, col = panel_pos[panel_letter]
    display_label = label_map[panel_letter]
    title = title.replace(f"({panel_letter})", f"({display_label})")
    ax  = fig.add_subplot(gs[row, col])
    xs_ = xs if xs is not None else theta
    ax.plot(xs_, ys, color=color, linewidth=STYLE["lw_main"], zorder=3)
    if yscale == "log":
        ax.set_yscale("log")
    ax.set_title(title, pad=6, color=STYLE["color_title"],
                 fontsize=STYLE["font_size_panel_title"], fontweight=STYLE["font_weight_title"])
    ax.set_xlabel(xlabel, labelpad=4, fontsize=STYLE["font_size_axis_label"])
    ax.set_ylabel(ylabel, labelpad=4, fontsize=STYLE["font_size_axis_label"])
    ax.tick_params(axis="both", which="both", direction=STYLE["tick_direction"],
                   length=STYLE["tick_length"], labelsize=STYLE["font_size_tick"])
    if xs is None:
        ax.axvline(STYLE["theta_sat"], color=STYLE["color_sat_line"], linestyle=":",
                   linewidth=STYLE["lw_vline"], alpha=0.6, zorder=2)
        ax.axvline(STYLE["theta_wp"],  color=STYLE["color_wp_line"],  linestyle=":",
                   linewidth=STYLE["lw_vline"], alpha=0.6, zorder=2)
    return ax

# -----------------------------------------------------------------------------
# 8.  PANELS
# -----------------------------------------------------------------------------
if STYLE["show_a"]:
    ax1 = make_panel("a",
        title  = r"(a)  Matric Potential  |$\psi$|",
        xlabel = r"Volumetric water content  $\theta$  (cm$^3$ cm$^{-3}$)",
        ylabel = r"|$\psi$|  (cm H$_2$O)  — log scale",
        color  = STYLE["color_a"], ys=psi_abs, yscale="log")
    ax1.fill_between(theta, psi_abs, alpha=STYLE["fill_alpha"], color=STYLE["color_a"])
    ax1.annotate("Wilting\npoint", xy=(0.09, psi_abs[3]), xytext=(0.12, psi_abs[8]),
                 fontsize=STYLE["font_size_annotation"], color=STYLE["color_wp_line"],
                 arrowprops=dict(arrowstyle="->", color=STYLE["color_wp_line"], lw=0.9))
    ax1.annotate(r"$\theta_{sat}$", xy=(STYLE["theta_sat"], psi_abs[-1] * 1.5),
                 fontsize=STYLE["font_size_annotation"], color=STYLE["color_sat_line"])

if STYLE["show_b"]:
    ax2 = make_panel("b",
        title  = r"(b)  Effective Saturation  $S_e$",
        xlabel = r"Volumetric water content  $\theta$  (cm$^3$ cm$^{-3}$)",
        ylabel = r"$S_e$  (dimensionless)",
        color  = STYLE["color_b"], ys=Se)
    ax2.fill_between(theta, Se, alpha=STYLE["fill_alpha"], color=STYLE["color_b"])
    ax2.set_ylim(0, 1.05)
    ax2.axhspan(0.95, 1.05, color=STYLE["color_sat_line"], alpha=STYLE["fill_alpha_zone"],
                label="Near-saturated zone")
    ax2.legend(loc="upper left", fontsize=STYLE["font_size_legend"],
               framealpha=STYLE["legend_framealpha"])

if STYLE["show_c"]:
    ax3 = make_panel("c",
        title  = r"(c)  Unsaturated Conductivity  $K(\theta)$",
        xlabel = r"Volumetric water content  $\theta$  (cm$^3$ cm$^{-3}$)",
        ylabel = r"$K(\theta)$  (cm day$^{-1}$)  — log scale",
        color  = STYLE["color_c"], ys=K, yscale="log")
    ax3.fill_between(theta, K, 1e-8, alpha=STYLE["fill_alpha"], color=STYLE["color_c"])
    ax3.axhline(STYLE["Ks_ref"], color=STYLE["color_c"], linestyle="--",
                linewidth=STYLE["lw_refline"], alpha=0.55, label=f"Ks = {STYLE['Ks_ref']} cm/d")
    ax3.legend(loc="upper left", fontsize=STYLE["font_size_legend"],
               framealpha=STYLE["legend_framealpha"])

if STYLE["show_d"]:
    ax4 = make_panel("d",
        title  = r"(d)  Darcy-Buckingham Vertical Flux  $q$",
        xlabel = r"Volumetric water content  $\theta$  (cm$^3$ cm$^{-3}$)",
        ylabel = r"$q$  (cm day$^{-1}$)   [+ = downward]",
        color  = STYLE["color_d"], ys=flux)
    ax4.fill_between(theta, flux, 0, where=[f >= 0 for f in flux],
                     color=STYLE["color_d"], alpha=STYLE["fill_alpha"]+0.04, label="Drainage")
    ax4.fill_between(theta, flux, 0, where=[f < 0 for f in flux],
                     color=STYLE["color_wp_line"], alpha=STYLE["fill_alpha"]+0.04, label="Upward")
    ax4.axhline(0, color=STYLE["color_ref_hline"], linewidth=STYLE["lw_refline"],
                linestyle="-", alpha=0.45)
    ax4.legend(loc="upper left", fontsize=STYLE["font_size_legend"],
               framealpha=STYLE["legend_framealpha"])

if STYLE["show_e"]:
    ax5 = make_panel("e",
        title  = r"(e)  Soil Respiration  $R(T) \times f(\theta)$",
        xlabel = r"Volumetric water content  $\theta$  (cm$^3$ cm$^{-3}$)",
        ylabel = r"CO$_2$ efflux  (mg C cm$^{-3}$ day$^{-1}$)",
        color  = STYLE["color_e"], ys=co2)
    ax5.fill_between(theta, co2, alpha=STYLE["fill_alpha"], color=STYLE["color_e"])
    theta_opt = 0.55 * STYLE["theta_s_loam"]
    idx_opt   = min(range(len(theta)), key=lambda i: abs(theta[i] - theta_opt))
    ax5.scatter([theta[idx_opt]], [co2[idx_opt]], color=STYLE["color_e"],
                s=STYLE["marker_size"], zorder=5, edgecolors="white", linewidths=STYLE["marker_lw"])
    ax5.annotate(r"$\theta_{opt}$" + f" = {theta_opt:.2f}",
                 xy=(theta[idx_opt], co2[idx_opt]),
                 xytext=(theta[idx_opt]+0.025, co2[idx_opt]/2+0.003),
                 fontsize=STYLE["font_size_annotation"], color=STYLE["color_annot"],
                 arrowprops=dict(arrowstyle="->", color=STYLE["color_e"], lw=0.9))

if STYLE["show_f"]:
    ax6 = make_panel("f",
        title  = r"(f)  Apparent Thermal Diffusivity  $D_T$",
        xlabel = r"Volumetric water content  $\theta$  (cm$^3$ cm$^{-3}$)",
        ylabel = r"$D_T$  (cm$^2$ day$^{-1}$)",
        color  = STYLE["color_f"], ys=DT)
    ax6.fill_between(theta, DT, alpha=STYLE["fill_alpha"], color=STYLE["color_f"])

# -----------------------------------------------------------------------------
# 9.  SHARED LEGEND
# -----------------------------------------------------------------------------
legend_handles = [
    Line2D([0],[0], color=STYLE["color_sat_line"], linestyle=":", linewidth=STYLE["lw_legend"],
           label=fr"$\theta_{{sat}}$ = {STYLE['theta_sat']} cm$^3$ cm$^{{-3}}$  (saturation threshold)"),
    Line2D([0],[0], color=STYLE["color_wp_line"],  linestyle=":", linewidth=STYLE["lw_legend"],
           label=fr"$\theta_{{wp}}$  = {STYLE['theta_wp']} cm$^3$ cm$^{{-3}}$  (wilting point threshold)"),
]
fig.legend(handles=legend_handles, loc="lower center", ncol=STYLE["legend_ncol"],
           fontsize=STYLE["font_size_legend"], framealpha=STYLE["legend_framealpha"],
           bbox_to_anchor=(0.5, STYLE["legend_y"]))

# -----------------------------------------------------------------------------
# 10.  SAVE
# -----------------------------------------------------------------------------
# Added transparent=True to preserve transparency in the output file
fig.savefig(STYLE["out_file"], dpi=STYLE["fig_dpi"], bbox_inches="tight", 
            transparent=True, facecolor="none")
print(f"Figure saved: {STYLE['out_file']}  ({STYLE['fig_dpi']} dpi)")
plt.show()