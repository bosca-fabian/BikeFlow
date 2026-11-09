"""BikeFlow dashboard (serving layer): a live page and a history page.

    docker compose up -d dashboard        then open http://localhost:8501

The live page reads the speed layer (Redis, written by the `stream` service); the
history page reads Delta tables.

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


@st.fragment(run_every="30s")
def live_page() -> None:
    live = queries.live_state()
    if live is None:
        st.info("No live data in Redis yet. Start the speed layer with `docker compose up -d stream` "
                "(it needs Kafka and the producer running); stations appear within a minute.")
        return
    stats, df, alerts, warnings = live["stats"], live["stations"], live["alerts"], live["warnings"]
    now = df["event_time"].max()  # the stream's clock: newest event written to Redis
    lag_s = (pd.Timestamp.now(tz="UTC") - now).total_seconds()
    in_service = df[df["status"] != "Out of service"]

    c = st.columns(6)
    c[0].metric("Stations in service", f"{int(stats['total']) - int(stats['out_of_service']):,}")
    c[1].metric("Empty now", f"{int(stats['empty']):,}", help="No bike to rent")
    c[2].metric("Full now", f"{int(stats['full']):,}", help="No free dock to return a bike")
    c[3].metric("Open alerts", f"{int(stats['alerts_open']):,}",
                help="Empty or full for at least 2 minutes; critical after 10 minutes")
    c[4].metric("Bikes available", f"{int(in_service['bikes'].sum()):,}")
    c[5].metric("As of (NY time)", now.tz_convert(queries.NY_TZ).strftime("%H:%M"),
                help=f"Newest station update in Redis, {lag_s:.0f} s ago")

    minutes = ((now - df["condition_since"]).dt.total_seconds() // 60).astype("Int64")
    problem = df["status"].isin(["Empty", "Full"]) & minutes.notna()
    severity = df["alert_severity"].fillna("").str.capitalize()
    note = pd.Series("", index=df.index)
    note[problem] = df["status"] + " for " + minutes.astype(str) + " min"
    note[problem & (severity != "")] = severity + " alert: " + note
    m = df[df["lat"].notna()].assign(
        color=lambda d: d["status"].map(lambda s: _rgb(STATUS_COLORS[s])),
        order=lambda d: d["status"].map(STATUS_ORDER.index),
        note=note,
        bikes_txt=lambda d: d["bikes"].fillna(0).astype(int).astype(str),
        ebikes_txt=lambda d: d["ebikes"].fillna(0).astype(int).astype(str),
        docks_txt=lambda d: d["docks"].fillna(0).astype(int).astype(str),
    ).sort_values("order")
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=m[["lon", "lat", "color", "name", "status", "bikes_txt", "ebikes_txt", "docks_txt", "note"]],
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
        "html": "<b>{name}</b><br/>{status}<br/>Bikes: {bikes_txt} (e-bikes: {ebikes_txt})"
                "<br/>Free docks: {docks_txt}<br/>{note}",
    }
    view = pdk.ViewState(latitude=40.735, longitude=-73.97, zoom=11.2)
    status_legend()
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view, tooltip=tooltip), height=560)

    st.subheader(f"Open alerts ({len(alerts)})")
    st.caption("A station gets an alert once it has been empty or full for 2 minutes. The alert turns "
               "critical after 10 minutes and closes when the station recovers.")
    if alerts.empty:
        st.write("No open alerts.")
    else:
        t = alerts.assign(
            Station=alerts["name"].fillna(alerts["station_id"]),
            Problem=alerts["kind"].map({"empty": "Empty", "full": "Full"}),
            Severity=alerts["severity"].str.capitalize(),
            minutes=((now - alerts["condition_since"]).dt.total_seconds() // 60).astype("Int64"),
            Opened=alerts["opened_at"].dt.tz_convert(queries.NY_TZ).dt.strftime("%H:%M"),
        ).sort_values(["Severity", "minutes"], ascending=[True, False])  # critical first
        cols = {"Station": "Station", "Problem": "Problem", "Severity": "Severity", "minutes": "For (min)",
                "bikes": "Bikes", "docks": "Free docks", "Opened": "Opened"}
        st.dataframe(t[list(cols)].rename(columns=cols), hide_index=True, height=360)

    st.subheader("30-minute warnings")
    if live["profiles"].get("status") != "loaded":
        st.caption("Not available yet: the warnings need the demand profiles from batch processing, "
                   "which the speed layer reports as missing.")
    elif warnings.empty:
        st.caption("No station is expected to empty or fill in the next 30 minutes.")
    else:
        w = warnings.assign(
            Station=warnings["name"].fillna(warnings["station_id"]),
            Expected=warnings["kind"].map({"likely_empty": "Likely empty", "likely_full": "Likely full"}),
        ).sort_values("eta_minutes")
        cols = {"Station": "Station", "Expected": "Expected", "eta_minutes": "In (min)", "bikes": "Bikes",
                "docks": "Free docks", "confidence": "Confidence"}
        st.dataframe(w[list(cols)].rename(columns=cols), hide_index=True, height=300)


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
