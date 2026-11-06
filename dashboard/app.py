"""BikeFlow dashboard (serving layer): a live page and a history page.

    docker compose up -d dashboard        then open http://localhost:8501

Data access lives in queries.py; this file only lays out the pages.
"""

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

import queries

st.set_page_config(page_title="BikeFlow", page_icon=":material/pedal_bike:", layout="wide")

# Colors from the team's chart palette: one blue for single series, a blue ramp for
# magnitude, and fixed status colors that always come with a text label.
BLUE = "#2a78d6"
# Magnitude ramp: low values stay close to the page background, high values stand out.
BLUE_RAMP_LIGHT = ["#cde2fb", "#6da7ec", "#2a78d6", "#1c5cab", "#0d366b"]
BLUE_RAMP_DARK = ["#104281", "#1c5cab", "#2a78d6", "#6da7ec", "#cde2fb"]
STATUS_COLORS = {
    "Empty": "#d03b3b",
    "Full": "#fab219",
    "OK": "#0ca30c",
    "Out of service": "#898781",
}
STATUS_ORDER = ["Out of service", "OK", "Full", "Empty"]  # drawn last = on top


def _rgb(hex_color: str) -> list[int]:
    return [int(hex_color[i:i + 2], 16) for i in (1, 3, 5)]


def status_legend() -> None:
    items = "".join(
        f'<span style="margin-right:1.2rem;white-space:nowrap">'
        f'<span style="display:inline-block;width:10px;height:10px;border-radius:50%;'
        f'background:{STATUS_COLORS[s]};margin-right:6px"></span>{s}</span>'
        for s in ["Empty", "Full", "OK", "Out of service"]
    )
    st.markdown(f'<div style="font-size:0.9rem">{items}</div>', unsafe_allow_html=True)


# ---------------------------------------------------------------- Live page


@st.fragment(run_every="60s")
def live_page() -> None:
    df = queries.live_stations()
    if df is None:
        st.info("No live data yet. Start the producer and the lake sink: `docker compose up -d`, "
                "then wait about two minutes for the first write.")
        return

    as_of = df["event_time"].max().tz_convert(queries.NY_TZ)
    age_min = (pd.Timestamp.now(tz="UTC") - df["event_time"].max()).total_seconds() / 60
    counts = df["status"].value_counts()
    in_service = df[df["status"] != "Out of service"]

    c = st.columns(5)
    c[0].metric("Stations in service", f"{len(in_service):,}")
    c[1].metric("Empty now", f"{counts.get('Empty', 0):,}", help="No bike to rent")
    c[2].metric("Full now", f"{counts.get('Full', 0):,}", help="No free dock to return a bike")
    c[3].metric("Bikes available", f"{int(in_service['num_bikes_available'].sum()):,}")
    c[4].metric("Data as of (New York)", as_of.strftime("%H:%M"), help=f"{age_min:.0f} min ago")

    m = df.assign(
        color=df["status"].map(lambda s: _rgb(STATUS_COLORS[s])),
        order=df["status"].map(STATUS_ORDER.index),
        changed=df["last_change_ny"].dt.strftime("%H:%M"),
    ).sort_values("order")
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=m[["lon", "lat", "color", "name", "status", "num_bikes_available",
                "num_ebikes_available", "num_docks_available", "changed"]],
        get_position="[lon, lat]",
        get_fill_color="color",
        get_line_color=[255, 255, 255],
        stroked=True,
        line_width_min_pixels=1,
        get_radius=45,
        radius_min_pixels=3,
        radius_max_pixels=9,
        pickable=True,
    )
    tooltip = {
        "html": "<b>{name}</b><br/>{status}<br/>Bikes: {num_bikes_available} "
                "(e-bikes: {num_ebikes_available})<br/>Free docks: {num_docks_available}"
                "<br/>Last change: {changed}",
    }
    view = pdk.ViewState(latitude=40.735, longitude=-73.97, zoom=11.2)
    status_legend()
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view, tooltip=tooltip), height=560)

    cols = {"name": "Station", "num_bikes_available": "Bikes", "num_ebikes_available": "E-bikes",
            "num_docks_available": "Free docks", "num_bikes_disabled": "Disabled bikes",
            "changed": "Last change"}
    left, right = st.columns(2)
    for col, label in ((left, "Empty"), (right, "Full")):
        rows = m[m["status"] == label].sort_values("name")
        col.subheader(f"{label} stations ({len(rows)})")
        col.dataframe(rows[list(cols)].rename(columns=cols), hide_index=True, height=300)


# ---------------------------------------------------------------- History page


@st.cache_data(show_spinner="Reading trips…")
def history_data(months: tuple[str, ...]):
    # `months` is only the cache key: a new month in the lake recomputes everything.
    return (queries.hour_weekday(), queries.monthly(), queries.busiest_stations(),
            queries.daily_weather())


def history_page() -> None:
    months = queries.months_loaded()
    if not months:
        st.info("No trips in the lake yet. Load at least one month with the batch scripts "
                "(see README_batch.md).")
        return
    hw, mo, top, wd = history_data(tuple(months))
    span = months[0] if len(months) == 1 else f"{months[0]} to {months[-1]}"
    st.caption(f"Trips loaded: {span} ({len(months)} month{'s' if len(months) > 1 else ''}).")

    total = int(mo["trips"].sum())
    c = st.columns(3)
    c[0].metric("Trips", f"{total:,}")
    c[1].metric("Members", f"{(mo['member_pct'] * mo['trips']).sum() / total:.0f}%")
    c[2].metric("E-bikes", f"{(mo['ebike_pct'] * mo['trips']).sum() / total:.0f}%")

    st.subheader("When do people ride?")
    st.caption("Average trips started per hour (New York time).")
    dark = getattr(st.context, "theme", None) is not None and st.context.theme.type == "dark"
    ramp = BLUE_RAMP_DARK if dark else BLUE_RAMP_LIGHT
    heat = alt.Chart(hw).mark_rect(cornerRadius=2).encode(
        x=alt.X("hour:O", title="Hour of day", axis=alt.Axis(labelAngle=0)),
        y=alt.Y("weekday:N", title=None, sort=queries.WEEKDAYS),
        color=alt.Color("avg_trips:Q", title="Trips / hour", scale=alt.Scale(range=ramp)),
        tooltip=[alt.Tooltip("weekday:N", title="Day"), alt.Tooltip("hour:O", title="Hour"),
                 alt.Tooltip("avg_trips:Q", title="Trips / hour", format=",.0f")],
    ).properties(height=260)
    st.altair_chart(heat, width="stretch")

    left, right = st.columns(2)
    with left:
        st.subheader("Trips per month")
        bars = alt.Chart(mo).mark_bar(color=BLUE, cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=alt.X("month_label:O", title=None, axis=alt.Axis(labelAngle=0)),
            y=alt.Y("trips:Q", title="Trips"),
            tooltip=[alt.Tooltip("month_label:O", title="Month"),
                     alt.Tooltip("trips:Q", title="Trips", format=","),
                     alt.Tooltip("member_pct:Q", title="Members %", format=".1f"),
                     alt.Tooltip("ebike_pct:Q", title="E-bikes %", format=".1f")],
        ).properties(height=420)
        st.altair_chart(bars, width="stretch")
    with right:
        st.subheader("Busiest stations")
        hbar = alt.Chart(top).mark_bar(color=BLUE, cornerRadiusTopRight=4, cornerRadiusBottomRight=4).encode(
            x=alt.X("trips:Q", title="Trips started"),
            y=alt.Y("station:N", title=None, sort="-x", axis=alt.Axis(labelLimit=220)),
            tooltip=[alt.Tooltip("station:N", title="Station"),
                     alt.Tooltip("trips:Q", title="Trips started", format=",")],
        ).properties(height=420)
        st.altair_chart(hbar, width="stretch")

    st.subheader("Weather")
    st.caption("One point per day. A wet day has at least 1 mm of precipitation, a heavy-rain day at "
               "least 10 mm. Weekends ride differently, so compare like with like.")
    left, right = st.columns(2)
    with left:
        by_rain = (wd.groupby("rain", observed=True)
                     .agg(avg_trips=("trips", "mean"), days=("trips", "size")).reset_index())
        rain = alt.Chart(by_rain).mark_bar(color=BLUE, cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=alt.X("rain:N", title=None, sort=list(by_rain["rain"]), axis=alt.Axis(labelAngle=0, labelLimit=200)),
            y=alt.Y("avg_trips:Q", title="Average trips per day"),
            tooltip=[alt.Tooltip("rain:N", title="Day type"),
                     alt.Tooltip("avg_trips:Q", title="Trips / day", format=",.0f"),
                     alt.Tooltip("days:Q", title="Days")],
        ).properties(height=300)
        st.altair_chart(rain, width="stretch")
    with right:
        temp = alt.Chart(wd).mark_circle(size=80, color=BLUE, opacity=0.85).encode(
            x=alt.X("temp_c:Q", title="Mean temperature (°C)", scale=alt.Scale(zero=False)),
            y=alt.Y("trips:Q", title="Trips per day", scale=alt.Scale(zero=False)),
            tooltip=[alt.Tooltip("date:T", title="Date"), alt.Tooltip("weekday:N", title="Day"),
                     alt.Tooltip("trips:Q", title="Trips", format=","),
                     alt.Tooltip("temp_c:Q", title="°C", format=".1f"),
                     alt.Tooltip("precip_mm:Q", title="Precipitation (mm)", format=".1f")],
        ).properties(height=300)
        st.altair_chart(temp, width="stretch")

    with st.expander("Daily table"):
        st.dataframe(wd.sort_values("date"), hide_index=True)


# ---------------------------------------------------------------- Layout

st.title("BikeFlow")
live_tab, history_tab = st.tabs(["Live", "History"])
with live_tab:
    live_page()
with history_tab:
    history_page()
