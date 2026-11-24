"""Live and historical views for the single-partition NASA log demo."""

import os
from collections import Counter

import pandas as pd
import plotly.express as px
from cassandra.cluster import Cluster
from dash import Dash, Input, Output, dcc, html


app = Dash(__name__)
app.title = "NASA Log Analytics"
app.layout = html.Main(
    [
        html.H1("NASA Log Analytics"),
        html.P("Live requests to nasa.gov and historical NASA access logs • All times UTC • Refreshes every 10 seconds"),
        html.P(html.A("Make a live NASA request", href="http://localhost:8081/", target="_blank")),
        dcc.RadioItems(
            id="view", value="live", inline=True,
            options=[{"label": "Live (last 15 minutes)", "value": "live"},
                     {"label": "Historical: hourly", "value": "hourly"},
                     {"label": "Historical: daily", "value": "daily"}],
        ),
        html.P(id="summary"),
        html.P(id="last-refresh"),
        dcc.Interval(id="refresh", interval=10_000, n_intervals=0),
        dcc.Graph(id="traffic"),
        dcc.Graph(id="statuses"),
        dcc.Graph(id="paths"),
    ],
    style={"fontFamily": "Arial, sans-serif", "maxWidth": "1100px", "margin": "auto", "padding": "20px"},
)


def empty_charts(message):
    return tuple(px.scatter(title=message) for _ in range(3))


@app.callback(
    Output("summary", "children"), Output("last-refresh", "children"),
    Output("traffic", "figure"), Output("statuses", "figure"), Output("paths", "figure"),
    Input("refresh", "n_intervals"), Input("view", "value"),
)
def refresh_dashboard(_, view):
    now = pd.Timestamp.now(tz="UTC")
    refreshed = f"Last refreshed: {now:%Y-%m-%d %H:%M:%S} UTC"
    cluster = None
    try:
        cluster = Cluster([os.environ.get("CASSANDRA_HOST", "cassandra")], connect_timeout=5)
        session = cluster.connect("log_analysis")
        rows = list(session.execute(
            "SELECT event_time, ingested_at, status, path FROM events_by_partition WHERE partition = 0"
        ))
        if not rows:
            return ("No stored events yet.", refreshed, *empty_charts("Waiting for data"))
        df = pd.DataFrame(rows, columns=["event_time", "ingested_at", "status", "path"])
        df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
        df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)
        total = len(df)
        if view == "live":
            df = df[(df["ingested_at"] >= now - pd.Timedelta(minutes=15)) & (df["ingested_at"] <= now)]
            time_column, frequency, title = "ingested_at", "min", "Events received by Kafka per minute (UTC)"
            description = "events received in the last 15 minutes"
        else:
            time_column = "event_time"
            frequency = "h" if view == "hourly" else "D"
            title = f"Requests by original event {'hour' if view == 'hourly' else 'day'} (UTC)"
            description = "historical events"
        if df.empty:
            return (f"0 {description} | {total:,} total stored events", refreshed,
                    *empty_charts("No events in this view"))
        traffic = df.groupby(df[time_column].dt.floor(frequency)).size().reset_index(name="requests")
        statuses = df.groupby("status").size().reset_index(name="requests")
        statuses["status"] = statuses["status"].astype(str)
        paths = pd.DataFrame(Counter(df["path"]).most_common(10), columns=["path", "requests"])
        return (
            f"{len(df):,} {description} | {total:,} total stored events", refreshed,
            px.bar(traffic, x=time_column, y="requests", title=title),
            px.bar(statuses, x="status", y="requests", title="HTTP status codes in selected view"),
            px.bar(paths, x="requests", y="path", orientation="h", title="Top 10 paths in selected view"),
        )
    except Exception as exc:
        return (f"Unable to load events: {exc}", refreshed, *empty_charts("Waiting for data"))
    finally:
        if cluster is not None:
            cluster.shutdown()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
