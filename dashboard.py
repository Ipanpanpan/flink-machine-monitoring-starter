"""Baseline Flink dashboard with a small usability/alerting extension.

Run on Windows: python -m streamlit run dashboard.py
Read-only: consumes FINISHED CSV part files written by Flink.
"""

from pathlib import Path

import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent
TUMBLING_FOLDER = BASE_DIR / "output" / "tumbling"
SLIDING_FOLDER = BASE_DIR / "output" / "sliding"
COLUMNS = ["machine_id", "window_start", "window_end", "avg_temperature"]

st.set_page_config(page_title="Live Machine Monitoring", page_icon="🌡️", layout="wide")


def read_flink_files(folder: Path) -> pd.DataFrame:
    """Read only completed Flink part files, not in-progress or pending files."""
    if not folder.is_dir():
        return pd.DataFrame(columns=COLUMNS)

    frames = []
    for path in sorted(folder.rglob("part-*")):
        if not path.is_file() or path.stat().st_size == 0:
            continue
        if ".inprogress" in path.name or ".pending" in path.name:
            continue
        try:
            frame = pd.read_csv(path, header=None, names=COLUMNS)
            if not frame.empty:
                frames.append(frame)
        except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError):
            # A corrupt file should not crash the demo dashboard.
            continue

    if not frames:
        return pd.DataFrame(columns=COLUMNS)

    data = pd.concat(frames, ignore_index=True)
    data["machine_id"] = pd.to_numeric(data["machine_id"], errors="coerce")
    data["avg_temperature"] = pd.to_numeric(data["avg_temperature"], errors="coerce")
    for column in ("window_start", "window_end"):
        data[column] = pd.to_datetime(data[column], errors="coerce")
    data = data.dropna(subset=COLUMNS)
    if data.empty:
        return pd.DataFrame(columns=COLUMNS)
    data["machine_id"] = data["machine_id"].astype(int)
    return data.drop_duplicates(subset=COLUMNS).sort_values("window_end")


def classify_temperature(avg_temp: float, warning: int, high: int) -> str:
    """Status is based on the sliding-window mean, NOT individual spikes."""
    if avg_temp >= high:
        return "High"
    if avg_temp >= warning:
        return "Warning"
    return "Normal"


st.title("Real-Time Machine Temperature Monitoring")
st.caption("Apache Flink DataGen → Tumbling / Hopping windows → CSV → Streamlit")

st.sidebar.header("Monitoring controls")
warning_limit = st.sidebar.slider("Warning threshold (°C)", 60, 95, 80)
high_limit = st.sidebar.slider(
    "High threshold (°C)",
    min_value=warning_limit + 1,
    max_value=105,
    value=max(90, warning_limit + 1),
)
selected_machines = st.sidebar.multiselect("Show machines", options=[1, 2, 3], default=[1, 2, 3])
st.sidebar.caption("Statuses use the latest 10-second sliding-window average.")


@st.fragment(run_every="5s")
def live_dashboard() -> None:
    tumbling = read_flink_files(TUMBLING_FOLDER)
    sliding = read_flink_files(SLIDING_FOLDER)

    if tumbling.empty or sliding.empty:
        st.warning(
            "Waiting for completed CSV results. Confirm the Flink job is RUNNING "
            "and at least one checkpoint has completed."
        )
        return

    latest_tumbling = tumbling.groupby("machine_id", group_keys=False).tail(1)
    latest_sliding = sliding.groupby("machine_id", group_keys=False).tail(1)

    comparison = latest_tumbling[["machine_id", "avg_temperature"]].merge(
        latest_sliding[["machine_id", "window_end", "avg_temperature"]],
        on="machine_id",
        how="inner",
        suffixes=("_tumbling", "_sliding"),
    )
    if comparison.empty:
        st.info("The two window outputs do not yet have matching machine IDs.")
        return

    comparison["status"] = comparison["avg_temperature_sliding"].apply(
        lambda value: classify_temperature(value, warning_limit, high_limit)
    )
    comparison["delta"] = (
        comparison["avg_temperature_sliding"] - comparison["avg_temperature_tumbling"]
    )

    display = comparison.loc[comparison["machine_id"].isin(selected_machines)].copy()
    if display.empty:
        st.info("Select at least one machine from the sidebar.")
        return

    counts = display["status"].value_counts()
    metrics = st.columns(4)
    metrics[0].metric("Visible machines", len(display))
    metrics[1].metric("High", int(counts.get("High", 0)))
    metrics[2].metric("Warning", int(counts.get("Warning", 0)))
    metrics[3].metric("Normal", int(counts.get("Normal", 0)))

    n_high = int(counts.get("High", 0))
    n_warning = int(counts.get("Warning", 0))
    if n_high:
        high_ids = ", ".join(
            f"Machine {mid}" for mid in display.loc[display["status"] == "High", "machine_id"]
        )
        st.error(f"HIGH TEMPERATURE ALERT: {high_ids}. Check the latest sliding average.")
    elif n_warning:
        st.warning(f"{n_warning} machine(s) are in Warning status.")
    else:
        st.success("All visible machines have Normal average temperatures.")

    st.subheader("Latest window comparison")
    show_table = display[[
        "machine_id", "avg_temperature_tumbling", "avg_temperature_sliding",
        "delta", "status", "window_end"
    ]].rename(columns={
        "machine_id": "Machine",
        "avg_temperature_tumbling": "Tumbling Avg (°C)",
        "avg_temperature_sliding": "Sliding Avg (°C)",
        "delta": "Sliding − Tumbling (°C)",
        "status": "Status",
        "window_end": "Latest Sliding Window End",
    }).sort_values("Machine")
    st.dataframe(
        show_table.style.format({
            "Tumbling Avg (°C)": "{:.1f}",
            "Sliding Avg (°C)": "{:.1f}",
            "Sliding − Tumbling (°C)": "{:+.1f}",
        }),
        hide_index=True,
        use_container_width=True,
    )
    st.caption("The two latest windows are not necessarily identical in timing; their difference is descriptive.")

    st.subheader("Temperature trend (sliding window)")
    trend = sliding.loc[sliding["machine_id"].isin(selected_machines)].copy()
    trend["Machine"] = "Machine " + trend["machine_id"].astype(str)
    chart_data = trend.pivot_table(
        index="window_end", columns="Machine", values="avg_temperature", aggfunc="mean"
    ).sort_index().tail(100)
    st.line_chart(chart_data, y_label="Average temperature (°C)")

    st.subheader("Export the processed data")
    export = sliding.loc[sliding["machine_id"].isin(selected_machines)].sort_values(
        ["window_end", "machine_id"]
    )
    st.download_button(
        label="Download sliding-window CSV",
        data=export.to_csv(index=False).encode("utf-8"),
        file_name="machine_sliding_averages.csv",
        mime="text/csv",
        key="download_sliding_csv",
    )
    st.caption(
        f"Latest sliding window end in output: {sliding['window_end'].max()} "
        "(Flink session time). Refreshes this dashboard every 5 seconds."
    )


live_dashboard()
