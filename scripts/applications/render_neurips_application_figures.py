"""Render the three application-summary panels as standalone NeurIPS-style figures.

The script only restyles released analysis outputs; it does not recompute or
alter any statistical result.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap, Normalize
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.metrics import normalized_mutual_info_score


ROOT = Path(__file__).resolve().parents[2]
FIG = ROOT / "figures"
OUT = ROOT / "analysis_outputs/applications/neurips_figures"

NAVY = "#17233C"
MUTED = "#5D6678"
GRID = "#DCE2EA"
BLUE = "#0072B2"
ORANGE = "#D55E00"
GOLD = "#E69F00"

BENCHMARK_CMAP = LinearSegmentedColormap.from_list(
    "oata_blue_green",
    ["#F5F8FB", "#DCEEF0", "#A8D8CF", "#61B7AF", "#287D92", "#173B5E"],
)
DOMAIN_CMAP = LinearSegmentedColormap.from_list(
    "semantic_indigo",
    ["#F7F8FC", "#E0E4F2", "#B7C0DF", "#8493CB", "#5369AD", "#283E7B"],
)


def _style() -> None:
    sns.set_theme(style="white")
    plt.rcParams.update({
        "font.family": "DejaVu Sans",
        "font.size": 9,
        "axes.titlesize": 11,
        "axes.labelsize": 9,
        "axes.labelcolor": NAVY,
        "axes.edgecolor": NAVY,
        "axes.linewidth": 0.8,
        "xtick.labelsize": 8,
        "ytick.labelsize": 8,
        "xtick.color": NAVY,
        "ytick.color": NAVY,
        "text.color": NAVY,
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
        "savefig.facecolor": "white",
    })


def _save(fig: plt.Figure, stem: str) -> None:
    FIG.mkdir(parents=True, exist_ok=True)
    for ext in ("pdf", "png"):
        fig.savefig(
            FIG / f"{stem}.{ext}",
            dpi=600 if ext == "png" else None,
            bbox_inches="tight",
            facecolor="white",
        )
    plt.close(fig)


def render_model_benchmark() -> None:
    source = ROOT / "analysis_outputs/applications/oata/fig_oata_model_benchmark.csv"
    frame = pd.read_csv(source).query("version == 'full'").copy()
    model_order = [
        "gower_pam", "optimal_matching", "soft_dtw",
        "hsmm_mixture", "multiview_nmf", "sequence_transformer",
    ]
    track_order = ["reward", "penalty", "adjudication"]
    labels = {
        "gower_pam": "Gower + PAM",
        "optimal_matching": "Opt. matching",
        "soft_dtw": "Soft-DTW",
        "hsmm_mixture": "HSMM",
        "multiview_nmf": "MV-NMF",
        "sequence_transformer": "Seq. encoder",
    }
    matrix = (
        frame.pivot(index="model", columns="track", values="silhouette")
        .reindex(index=model_order, columns=track_order)
    )

    # Model-specific protocol NMI is a post-fit diagnostic; protocol identity
    # is never an input to any trajectory model.
    protocol_nmi = pd.DataFrame(index=model_order, columns=track_order, dtype=float)
    for model in model_order:
        for track in track_order:
            weights = pd.read_parquet(
                ROOT / f"data/applications/oata/archetype_weights/{track}_full_{model}.parquet",
                columns=["protocol", "dominant_component"],
            )
            protocol_nmi.loc[model, track] = normalized_mutual_info_score(
                weights.protocol, weights.dominant_component,
            )
    stable = frame.set_index(["model", "track"]).apply(
        lambda row: (
            (pd.notna(row.bootstrap_ari) and row.bootstrap_ari >= 0.90)
            or (
                pd.notna(row.archetype_loading_stability)
                and row.archetype_loading_stability >= 0.90
            )
        ),
        axis=1,
    )

    fig, ax = plt.subplots(figsize=(2.60, 2.22))
    sns.heatmap(
        matrix, ax=ax, cmap=BENCHMARK_CMAP, vmin=0.58, vmax=0.92,
        annot=False, linewidths=0.7, linecolor="white", cbar=False,
    )
    ax.set_yticklabels([labels[x] for x in model_order], rotation=0)
    ax.set_xticklabels(["Reward", "Penalty", "Adjud."], rotation=0)
    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.tick_params(axis="both", length=0, labelsize=7.2)
    # Values are single deterministic full-lifecycle fits. Bold black outlines
    # mark the best internal separation within each track.
    for column, track in enumerate(track_order):
        best_row = int(np.nanargmax(matrix[track].to_numpy()))
        ax.add_patch(Rectangle(
            (column, best_row), 1, 1, fill=False, edgecolor="#111111",
            linewidth=1.35, joinstyle="miter",
        ))
        for row, model in enumerate(model_order):
            value = matrix.loc[model, track]
            text_color = "white" if value >= 0.80 else NAVY
            ax.text(
                column + 0.5, row + 0.53, f"{value:.2f}",
                ha="center", va="center", fontsize=7.2,
                color=text_color,
                fontweight="bold" if row == best_row else "normal",
            )
            if bool(stable.get((model, track), False)):
                ax.text(
                    column + 0.12, row + 0.23, "●",
                    ha="center", va="center", fontsize=4.8, color=text_color,
                )
            if protocol_nmi.loc[model, track] >= 0.70:
                ax.text(
                    column + 0.87, row + 0.23, "△",
                    ha="center", va="center", fontsize=5.0, color=text_color,
                )
    ax.text(
        0.0, -0.19, "● stable   △ protocol NMI ≥ .70",
        transform=ax.transAxes, ha="left", va="top",
        fontsize=6.0, color=MUTED,
    )
    fig.subplots_adjust(left=0.39, right=0.99, top=0.99, bottom=0.18)
    _save(fig, "fig_full_lifecycle_model_benchmark")

    released = frame[[
        "model", "track", "version", "n", "k", "silhouette",
        "davies_bouldin", "calinski_harabasz", "bootstrap_ari",
        "archetype_loading_stability",
    ]].sort_values(["model", "track"])
    released["protocol_nmi"] = [
        protocol_nmi.loc[row.model, row.track] for row in released.itertuples()
    ]
    released["stable_marker"] = released.apply(
        lambda row: bool(stable.get((row.model, row.track), False)), axis=1,
    )
    released["high_protocol_nmi_marker"] = released.protocol_nmi >= 0.70
    released.to_csv(OUT / "fig_full_lifecycle_model_benchmark.csv", index=False)


def _median_duration(curve: pd.DataFrame) -> float:
    reached = curve.loc[curve["survival"] <= 0.5, "duration_days"]
    return float(reached.iloc[0]) if len(reached) else float("nan")


def _km_greenwood(grouped: pd.DataFrame) -> pd.DataFrame:
    """Kaplan--Meier curve and pointwise Greenwood normal intervals."""
    grouped = grouped.sort_values("duration_seconds").reset_index(drop=True)
    total = int(grouped["observations"].sum())
    prior = grouped["observations"].cumsum().shift(fill_value=0).to_numpy()
    at_risk = total - prior
    events = grouped["events"].to_numpy(dtype=float)
    denominator = at_risk - events
    factors = np.where(at_risk > 0, 1 - events / at_risk, 1.0)
    survival = np.cumprod(factors)
    greenwood_increment = np.divide(
        events,
        at_risk * denominator,
        out=np.zeros_like(events, dtype=float),
        where=(at_risk > 0) & (denominator > 0),
    )
    variance = survival**2 * np.cumsum(greenwood_increment)
    standard_error = np.sqrt(np.maximum(variance, 0))
    result = grouped.assign(
        at_risk=at_risk,
        survival=survival,
        lower_95=np.clip(survival - 1.96 * standard_error, 0, 1),
        upper_95=np.clip(survival + 1.96 * standard_error, 0, 1),
    )
    start = pd.DataFrame([{
        "duration_seconds": 0, "observations": 0, "events": 0,
        "censored": 0, "at_risk": total, "survival": 1.0,
        "lower_95": 1.0, "upper_95": 1.0,
    }])
    return pd.concat([start, result], ignore_index=True)


def render_survival() -> None:
    latency = ROOT / "data/applications/accountability_economics/accountability_latency.parquet"
    con = duckdb.connect()
    curves = []
    summaries = []
    for protocol in ("UMA", "Flare_FTSOv2"):
        grouped = con.execute(f"""
            SELECT observed_duration_seconds AS duration_seconds,
              count(*) AS observations,
              count(*) FILTER (WHERE completed) AS events,
              count(*) FILTER (WHERE right_censored) AS censored
            FROM read_parquet('{latency}')
            WHERE protocol='{protocol}'
            GROUP BY 1 ORDER BY 1
        """).df()
        curve = _km_greenwood(grouped)
        curve["protocol"] = protocol
        curve["duration_days"] = curve.duration_seconds / 86_400
        n = int(grouped.observations.sum())
        censored = int(grouped.censored.sum())
        median = _median_duration(curve)
        summaries.append({
            "protocol": protocol, "n": n, "censored": censored,
            "median_duration_days": median,
        })
        if len(curve) > 3500:
            keep = np.unique(np.r_[
                np.linspace(0, len(curve) - 1, 3500, dtype=int),
                len(curve) - 1,
            ])
            curve = curve.iloc[keep].reset_index(drop=True)
        curves.append(curve)
    con.close()
    frame = pd.concat(curves, ignore_index=True)
    summary = pd.DataFrame(summaries).set_index("protocol")
    colors = {"UMA": BLUE, "Flare_FTSOv2": ORANGE}
    names = {"UMA": "UMA settlement", "Flare_FTSOv2": "Flare claim"}
    styles = {"UMA": "-", "Flare_FTSOv2": (0, (5, 2))}

    fig, ax = plt.subplots(figsize=(2.86, 2.22))
    for protocol in ("UMA", "Flare_FTSOv2"):
        curve = frame[frame.protocol == protocol].sort_values("duration_days")
        ax.fill_between(
            curve.duration_days, curve.lower_95, curve.upper_95,
            step="post", color=colors[protocol], alpha=0.13, linewidth=0,
        )
        ax.step(
            curve.duration_days, curve.survival, where="post",
            color=colors[protocol], linewidth=1.45, linestyle=styles[protocol],
            label=(
                f"{names[protocol]}\n"
                f"n={summary.loc[protocol, 'n']:,}; "
                f"cens.={summary.loc[protocol, 'censored']:,}"
            ),
        )

    ax.set_xscale("log")
    ax.set_xlim(max(frame.loc[frame.duration_days > 0, "duration_days"].min() * 0.8, 0.04),
                frame.duration_days.max() * 1.25)
    ax.set_ylim(0, 1.02)
    ax.set_xlabel("Time to realization (days, log scale)", labelpad=3, fontsize=7.5)
    ax.set_ylabel(
        "Survival probability\n(not yet settled or claimed)",
        labelpad=3, fontsize=7.5,
    )
    ax.text(
        0.98, 0.50,
        (
            f"Median: UMA {summary.loc['UMA', 'median_duration_days']:.1f} d\n"
            f"Flare {summary.loc['Flare_FTSOv2', 'median_duration_days']:.1f} d"
        ),
        transform=ax.transAxes, ha="right", va="center",
        fontsize=6.2, color=NAVY,
        bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.82, "pad": 1.5},
    )
    ax.grid(axis="both", which="major", color=GRID, linewidth=0.65)
    ax.grid(axis="x", which="minor", color=GRID, linewidth=0.35, alpha=0.55)
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(
        loc="upper right", frameon=False, fontsize=6.1,
        handlelength=2.2, labelspacing=0.45, borderaxespad=0.2,
    )
    ax.tick_params(axis="both", labelsize=7.0)
    fig.subplots_adjust(left=0.24, right=0.99, top=0.98, bottom=0.20)
    _save(fig, "fig_accountability_survival")
    frame.to_csv(OUT / "fig_accountability_survival.csv", index=False)
    summary.reset_index().to_csv(
        OUT / "fig_accountability_survival_summary.csv", index=False,
    )


def render_semantic_coverage() -> None:
    source = ROOT / "data/applications/geographic_semantic/semantic_domain_labels.parquet"
    con = duckdb.connect()
    frame = con.execute(f"""
        SELECT protocol,semantic_domain,coverage_status,
          count(DISTINCT source_record_id) AS distinct_native_objects
        FROM read_parquet('{source}')
        GROUP BY 1,2,3
    """).df()
    con.close()
    protocol_order = [
        "Chainlink", "Flare_FTSOv2", "Pyth", "Tellor",
        "UMA", "Chronicle", "RedStone",
    ]
    domain_order = [
        "crypto_price", "fiat_fx", "commodity", "equity_rwa",
        "macroeconomic_indicator", "politics_election", "sports",
        "weather_climate", "insurance", "corporate_event",
        "legal_regulatory_event", "unresolved",
    ]
    domain_labels = {
        "crypto_price": "Crypto",
        "fiat_fx": "Fiat / FX",
        "commodity": "Commodity",
        "equity_rwa": "Equity / RWA",
        "macroeconomic_indicator": "Macro",
        "politics_election": "Politics",
        "sports": "Sports",
        "weather_climate": "Weather",
        "insurance": "Insurance",
        "corporate_event": "Corporate",
        "legal_regulatory_event": "Legal / reg.",
        "unresolved": "Unresolved",
    }
    evidence_map = {
        "event_level": "event-level",
        "service_window": "event-level",
        "provider_epoch": "event-level",
        "query_type_aggregate": "aggregate",
        "registry_layer": "registry-only",
    }
    frame["evidence_depth"] = frame.coverage_status.map(evidence_map)
    frame["plot_domain"] = frame.semantic_domain.replace({"unknown": "unresolved"})
    frame.loc[frame.plot_domain == "unresolved", "evidence_depth"] = "unresolved"
    frame = frame.groupby(
        ["protocol", "plot_domain", "evidence_depth"], as_index=False,
    ).distinct_native_objects.sum()

    shapes = {
        "event-level": "o",
        "aggregate": "s",
        "registry-only": "o",
        "unresolved": "x",
    }
    colors = {
        "event-level": BLUE,
        "aggregate": ORANGE,
        "registry-only": NAVY,
        "unresolved": "#777777",
    }
    x_lookup = {domain: index for index, domain in enumerate(domain_order)}
    y_lookup = {protocol: index for index, protocol in enumerate(protocol_order)}
    frame = frame[
        frame.protocol.isin(protocol_order) & frame.plot_domain.isin(domain_order)
    ].copy()
    frame["x"] = frame.plot_domain.map(x_lookup)
    frame["y"] = frame.protocol.map(y_lookup)
    frame["marker_area"] = 12 + 27 * np.log10(frame.distinct_native_objects + 1)

    fig, ax = plt.subplots(figsize=(2.64, 2.22))
    ax.axvspan(
        len(domain_order) - 1.5, len(domain_order) - 0.5,
        color="#F0F1F3", zorder=0,
    )
    ax.axvline(len(domain_order) - 1.5, color="#777777", linewidth=0.7, zorder=1)
    for evidence, group in frame.groupby("evidence_depth"):
        kwargs = {
            "marker": shapes[evidence],
            "s": group.marker_area,
            "linewidth": 0.85,
            "zorder": 3,
        }
        if evidence == "registry-only":
            kwargs.update(facecolors="white", edgecolors=colors[evidence])
        elif evidence == "unresolved":
            kwargs.update(c=colors[evidence])
        else:
            kwargs.update(c=colors[evidence], edgecolors="white")
        ax.scatter(group.x, group.y, **kwargs)
        if evidence == "unresolved":
            for row in group.itertuples():
                count = int(row.distinct_native_objects)
                label = (
                    f"{count / 1000:.0f}k" if count >= 10_000 else str(count)
                )
                ax.annotate(
                    label, (row.x, row.y), xytext=(3, -4),
                    textcoords="offset points", fontsize=4.7,
                    color=MUTED, fontweight="semibold",
                )
    ax.set_xlim(-0.55, len(domain_order) - 0.45)
    ax.set_ylim(len(protocol_order) - 0.45, -0.55)
    ax.set_xticks(range(len(domain_order)))
    ax.set_yticks(range(len(protocol_order)))
    ax.set_yticklabels([
        protocol.replace("Flare_FTSOv2", "Flare")
        for protocol in protocol_order
    ], rotation=0)
    ax.set_xticklabels(
        [domain_labels[x] for x in domain_order],
        rotation=58, ha="right", rotation_mode="anchor",
    )
    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.tick_params(axis="both", length=0, labelsize=6.2)
    ax.grid(color=GRID, linewidth=0.4, zorder=0)
    ax.set_axisbelow(True)
    ax.spines[:].set_visible(False)
    handles = [
        Line2D([0], [0], marker="o", linestyle="", markersize=4.2,
               markerfacecolor=BLUE, markeredgecolor="white", label="event"),
        Line2D([0], [0], marker="s", linestyle="", markersize=4.0,
               markerfacecolor=ORANGE, markeredgecolor="white", label="aggregate"),
        Line2D([0], [0], marker="o", linestyle="", markersize=4.2,
               markerfacecolor="white", markeredgecolor=NAVY, label="registry"),
        Line2D([0], [0], marker="x", linestyle="", markersize=4.2,
               color="#777777", label="unresolved"),
    ]
    ax.legend(
        handles=handles, loc="lower left", bbox_to_anchor=(-0.04, 1.015),
        frameon=False, fontsize=4.9, ncol=4, columnspacing=0.48,
        handletextpad=0.25, borderaxespad=0,
    )
    ax.text(
        1.0, 1.005, "bubble area ∝ log distinct native objects",
        transform=ax.transAxes, ha="right", va="top",
        fontsize=4.6, color=MUTED,
    )
    fig.subplots_adjust(left=0.24, right=0.99, top=0.84, bottom=0.32)
    _save(fig, "fig_protocol_domain_heatmap")

    frame[[
        "protocol", "plot_domain", "evidence_depth",
        "distinct_native_objects", "marker_area",
    ]].rename(columns={"plot_domain": "semantic_domain"}).to_csv(
        OUT / "fig_protocol_domain_heatmap.csv", index=False,
    )


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    _style()
    render_model_benchmark()
    render_survival()
    render_semantic_coverage()


if __name__ == "__main__":
    main()
