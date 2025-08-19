import numpy as np
import plotly.graph_objects as go
import plotly.io as pio
import os
import argparse
import re
from rich import print


def parse_folder_name(folder_name: str):
    pattern = re.compile(
        r"^(?P<dataset>[a-zA-Z0-9]+)"
        r"_l(?P<l>[0-9]*\.?[0-9]+)"
        r"_w(?P<w>\d+)"
        r"_g(?P<g>\d+)"
        r"_p(?P<p>\d+)"
        r"_sm(?P<sm>True|False)$"
    )
    match = pattern.match(folder_name)
    if not match:
        raise ValueError(f"Folder name '{folder_name}' does not match expected format.")

    groups = match.groupdict()
    return {
        "dataset": groups["dataset"],
        "l": float(groups["l"]),
        "w": int(groups["w"]),
        "g": int(groups["g"]),
        "p": int(groups["p"]),
        "sm": groups["sm"] == "True",
    }


def moving_average(data, window_size=5):
    return np.convolve(data, np.ones(window_size) / window_size, mode="valid")


def plot_resources_total(details, resources, output_path):
    args = parse_folder_name(os.path.basename(output_path))

    # Memory and CPU usage
    timestamps = (resources["timestamp"] - resources["timestamp"][0]) / 60.0
    start_main = (details["main_start_timestamp"] - resources["timestamp"][0]) / 60.0
    mr_iterations = (details["update_edges_timestamp"] - resources["timestamp"][0]) / 60
    sm_iterations = (details["sm_timestamp"] - resources["timestamp"][0]) / 60

    main_python_mem = resources["main_python_mem"]
    main_python_cpu = resources["main_python_cpu"]
    java_mem = resources["java_mem"]
    java_cpu = resources["java_cpu"]
    workers_mem = resources["workers_mem"]
    workers_cpu = resources["workers_cpu"]

    cpu_raw = main_python_cpu + (java_cpu + workers_cpu if not args["sm"] else 0)
    cpu_smooth = moving_average(cpu_raw, window_size=5)

    # Since smoothing shortens the array (mode='valid'), adjust timestamps:
    timestamps_smooth = timestamps[len(timestamps) - len(cpu_smooth) :]

    # Create a plotly figure with bars for memory and CPU
    fig = go.Figure()

    memory_color = "rgba(214, 39, 40, 1)"
    memory_fillcolor = "rgba(214, 39, 40, 0.2)"

    cpu_color = "rgba(31, 119, 180, 1)"

    fig.add_trace(
        go.Scatter(
            x=timestamps,
            y=(main_python_mem + (java_mem + workers_mem if not args["sm"] else 0))
            / (1024**3),
            name="Memory usage (GB)",
            mode="lines",
            line=dict(color=memory_color, width=1),
            # fill="tozeroy",
            # fillcolor=memory_fillcolor,
        )
    )

    fig.add_trace(
        go.Scatter(
            x=timestamps_smooth,
            y=cpu_smooth,
            name="CPU Usage (%)",
            mode="lines",
            line=dict(
                color=cpu_color,
                width=1,
            ),
            yaxis="y2",
        )
    )

    end = 0
    for i in range(len(mr_iterations)):
        start = mr_iterations[i - 1] if i > 0 else start_main
        end = mr_iterations[i]
        fill_opacity = 0.07 if i % 2 == 0 else 0.03
        fillcolor = f"rgba(255, 0, 0, {fill_opacity})"

        fig.add_vrect(
            x0=start,
            x1=end,
            fillcolor=fillcolor,
            line_width=0,
            annotation_text=f"MR{i + 1}",
            annotation={
                "x": (start + end) / 2,
                "y": 1,
                "showarrow": False,
                "font": dict(size=16, color="black"),
                "align": "center",
                "textangle": 320,
            },
            annotation_position="top",
        )

    for i in range(len(sm_iterations)):
        start = sm_iterations[i - 1] if i > 0 else end
        end = sm_iterations[i]
        fill_opacity = 0.07 if i % 2 == 0 else 0.03
        fillcolor = f"rgba(0, 255, 0, {fill_opacity})"

        fig.add_vrect(
            x0=start,
            x1=end,
            fillcolor=fillcolor,
            line_width=0,
        )

    fig.update_layout(
        title="Resource Utilization Over Time",
        xaxis_title="Time elapsed (minutes)",
        margin=dict(l=80, r=80, t=100, b=80),
        xaxis=dict(
            title=dict(
                font=dict(size=20), standoff=15
            ),  # space between axis title and ticks
            tickfont=dict(size=16),
            ticks="outside",
            ticklen=8,
            tickwidth=1,
            showline=True,
            linecolor="black",
            mirror=True,
        ),
        yaxis=dict(
            title="Memory (GB)",
            side="left",
            showgrid=True,
            gridcolor="rgba(200,200,200,0.3)",
            tickfont=dict(size=16),
            ticks="outside",
            ticklen=8,
            tickwidth=1,
            showline=True,
            linecolor="black",
            mirror=True,
        ),
        yaxis2=dict(
            title="CPU Usage (%)",
            overlaying="y",
            range=[0, 100],
            side="right",
            showgrid=False,
            tickfont=dict(size=16),
            ticks="outside",
            ticklen=8,
            tickwidth=1,
            showline=True,
            linecolor="black",
            mirror=True,
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.08, xanchor="right", x=1),
        hovermode="x unified",
        plot_bgcolor="rgba(245,245,245,1)",
        template="plotly_white",
    )

    pio.write_image(
        fig,
        os.path.join(output_path, "total_resource_usage.png"),
        format="png",
        width=1200,
        height=800,
    )


def plot_running_time_total_first(datasets, data, output_path):
    folder_of_int_template = {
        "MR only": "_l0.5_w20_g0_p8_smFalse",
        "gamma = 5000": "_l0.5_w20_g5000_p8_smFalse",
        "Single process only": "_l0.5_w20_g5000_p8_smTrue",
    }
    
    folder_of_first_template = {
        "MR": "_l0.5_w20_g0_p8_smFalse",
        "Single process": "_l0.5_w20_g5000_p8_smTrue",
    }

    running_times = {label: [] for label in folder_of_int_template.keys()}
    first_iterations = {label: [] for label in folder_of_first_template.keys()}

    for dataset in datasets:
        for label, suffix in folder_of_int_template.items():
            f = f"{dataset}{suffix}"
            output = data[f]["output"]
            running_time = int(
                (output["main_end_timestamp"] - output["main_start_timestamp"]) / 60.0
            )
            running_times[label].append(running_time)

        for label, suffix in folder_of_first_template.items():
            f = f"{dataset}{suffix}"
            output = data[f]["output"]
            first = (
                output["update_edges_timestamp"][0] - output["partitions_computed"]
                if label != "Single process"
                else output["sm_timestamp"][0] - output["main_start_timestamp"]
            )

            first_iteration = int((first) / 60.0)
            first_iterations[label].append(first_iteration)

    fig_running = go.Figure()
    for label, values in running_times.items():
        fig_running.add_trace(go.Bar(x=datasets, y=values, name=label))

    fig_running.update_layout(
        title="Running Time (minutes)",
        barmode="group",
        xaxis_title="Dataset",
        yaxis_title="Running Time (minutes)",
    )

    print(values)

    # First iteration bar chart
    fig_first = go.Figure()
    for label, values in first_iterations.items():
        fig_first.add_trace(go.Bar(x=datasets, y=values, name=label))

    fig_first.update_layout(
        title="First Iteration Time (minutes)",
        barmode="group",
        xaxis_title="Dataset",
        yaxis_title="First Iteration Time (minutes)",
    )

    pio.write_image(
        fig_running,
        os.path.join(output_path, "datasets_running_time.png"),
        format="png",
        width=1200,
        height=800,
    )

    pio.write_image(
        fig_first,
        os.path.join(output_path, "datasets_first_it_running_time.png"),
        format="png",
        width=1200,
        height=800,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MRAttractor plotter")
    parser.add_argument("-f", required=True, help="Folder to data")
    args = parser.parse_args()

    subfolders = [f.path for f in os.scandir(args.f) if f.is_dir()]

    data_to_analyze = {}

    for folder in subfolders:
        _, sub = folder.split("/")
        resources_file = os.path.join(folder, "resources.npz")
        output_file = os.path.join(folder, "details.npz")
        try:
            data_to_analyze[sub] = {
                "path": folder,
                "resources": np.load(resources_file),
                "output": np.load(output_file, allow_pickle=True),
            }
        except Exception as e:
            print(f"Error loading [red] {folder} [/red]: {e}")

    # for folder, data in data_to_analyze.items():
    #     print(f"Analyzing [green] {folder} [/green]")
    #     plot_resources_total(data["output"], data["resources"], data["path"])

    datasets = ["amazon", "dblp"]
    plot_running_time_total_first(datasets, data_to_analyze, args.f)
