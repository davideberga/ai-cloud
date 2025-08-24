import numpy as np
import plotly.graph_objects as go
import plotly.io as pio
import os
import argparse
import re
from rich import print
from mdutils import MdUtils

from check_communities import (
    average_degree,
    compare_communities_sets,
    compute_nmi_ari,
    compute_purity,
    load_communities,
    load_ground_truth,
)


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

    # Timestamps
    timestamps = (resources["timestamp"] - resources["timestamp"][0]) / 60.0
    start_main = (details["main_start_timestamp"] - resources["timestamp"][0]) / 60.0
    computed_partitions = (details["partitions_computed"] - resources["timestamp"][0]) / 60.0
    mr_iterations = (details["update_edges_timestamp"] - resources["timestamp"][0]) / 60
    sm_iterations = (details["sm_timestamp"] - resources["timestamp"][0]) / 60

    # Resource signals
    main_python_mem = resources["main_python_mem"]
    main_python_cpu = resources["main_python_cpu"]
    java_mem = resources["java_mem"]
    java_cpu = resources["java_cpu"]
    workers_mem = resources["workers_mem"]
    workers_cpu = resources["workers_cpu"]

    # CPU (smoothed)
    cpu_raw = main_python_cpu + (java_cpu + workers_cpu if not args["sm"] else 0)
    cpu_smooth = moving_average(cpu_raw, window_size=5)
    timestamps_smooth = timestamps[len(timestamps) - len(cpu_smooth) :]

    fig_mem = go.Figure()

    fig_mem.add_trace(
        go.Scatter(
            x=timestamps,
            y=(main_python_mem + (java_mem + workers_mem if not args["sm"] else 0))
            / (1024**3),
            name="Memory usage (GB)",
            mode="lines",
            line=dict(color="rgba(214, 39, 40, 1)", width=1),
        )
    )
    fig_mem.add_vrect(x0=0, x1=computed_partitions, fillcolor="rgba(226, 153, 75, 0.2)", line_width=0)

    # Highlight MR iterations
    end = 0
    for i in range(len(mr_iterations)):
        start = mr_iterations[i - 1] if i > 0 else computed_partitions
        end = mr_iterations[i]
        fill_opacity = 0.07 if i % 2 == 0 else 0.03
        fillcolor = f"rgba(255, 0, 0, {fill_opacity})"

        fig_mem.add_vrect(x0=start, x1=end, fillcolor=fillcolor, line_width=0)

    # Highlight SM iterations
    for i in range(len(sm_iterations)):
        start = sm_iterations[i - 1] if i > 0 else end
        end = sm_iterations[i]
        fill_opacity = 0.07 if i % 2 == 0 else 0.03
        fillcolor = f"rgba(0, 255, 0, {fill_opacity})"
        fig_mem.add_vrect(x0=start, x1=end, fillcolor=fillcolor, line_width=0)

    fig_mem.update_layout(
        title="Memory Usage Over Time",
        xaxis_title="Time elapsed (minutes)",
        yaxis_title="Memory (GB)",
        margin=dict(l=80, r=80, t=100, b=80),
        plot_bgcolor="rgba(245,245,245,1)",
        template="plotly_white",
    )

    pio.write_image(
        fig_mem,
        os.path.join(output_path, "memory_usage.png"),
        format="png",
        width=1200,
        height=800,
    )

    # =============== CPU PLOT ===============
    fig_cpu = go.Figure()

    fig_cpu.add_trace(
        go.Scatter(
            x=timestamps_smooth,
            y=cpu_smooth,
            name="CPU Usage (%)",
            mode="lines",
            line=dict(color="rgba(31, 119, 180, 1)", width=1),
        )
    )
    
    fig_cpu.add_vrect(x0=0, x1=computed_partitions, fillcolor="rgba(226, 153, 75, 0.2)", line_width=0)

    # Highlight MR iterations
    end = 0
    for i in range(len(mr_iterations)):
        start = mr_iterations[i - 1] if i > 0 else computed_partitions
        end = mr_iterations[i]
        fill_opacity = 0.07 if i % 2 == 0 else 0.03
        fillcolor = f"rgba(255, 0, 0, {fill_opacity})"
        fig_cpu.add_vrect(x0=start, x1=end, fillcolor=fillcolor, line_width=0)

    # Highlight SM iterations
    for i in range(len(sm_iterations)):
        start = sm_iterations[i - 1] if i > 0 else end
        end = sm_iterations[i]
        fill_opacity = 0.07 if i % 2 == 0 else 0.03
        fillcolor = f"rgba(0, 255, 0, {fill_opacity})"
        fig_cpu.add_vrect(x0=start, x1=end, fillcolor=fillcolor, line_width=0)

    fig_cpu.update_layout(
        title="CPU Usage Over Time",
        xaxis_title="Time elapsed (minutes)",
        yaxis_title="CPU Usage (%)",
        margin=dict(l=80, r=80, t=100, b=80),
        yaxis=dict(range=[0, 100]),
        plot_bgcolor="rgba(245,245,245,1)",
        template="plotly_white",
    )

    pio.write_image(
        fig_cpu,
        os.path.join(output_path, "cpu_usage.png"),
        format="png",
        width=1200,
        height=800,
    )


def plot_running_time_total_first(datasets, data, output_path):
    folder_of_int_template = {
        "MR only": "_l0.5_w15_g0_p8_smFalse",
        "gamma = 5000": "_l0.5_w15_g5000_p8_smFalse",
        "Single process only": "_l0.5_w15_g5000_p8_smTrue",
    }

    folder_of_first_template = {
        "MR": "_l0.5_w15_g0_p8_smFalse",
        "Single process": "_l0.5_w15_g5000_p8_smTrue",
    }

    running_times = {label: [] for label in folder_of_int_template.keys()}
    first_iterations = {label: [] for label in folder_of_first_template.keys()}

    for dataset in datasets:
        for label, suffix in folder_of_int_template.items():
            f = f"{dataset}{suffix}"
            output = data[f]["output"]
            running_time = (
                output["main_end_timestamp"] - output["main_start_timestamp"]
            ) / 60.0

            running_times[label].append(running_time)

        for label, suffix in folder_of_first_template.items():
            f = f"{dataset}{suffix}"
            output = data[f]["output"]
            first = (
                output["update_edges_timestamp"][0] - output["partitions_computed"]
                if label != "Single process"
                else output["sm_timestamp"][0] - output["main_start_timestamp"]
            )

            first_iteration = (first) / 60.0
            first_iterations[label].append(first_iteration)

    fig_running = go.Figure()

    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b"]

    for i, (label, values) in enumerate(running_times.items()):
        fig_running.add_trace(
            go.Bar(
                x=datasets,
                y=values,
                name=label,
                marker=dict(
                    color=colors[i % len(colors)], line=dict(width=1.5, color="black")
                ),
                text=[f"{v:.2f}" for v in values],
                textposition="outside",
            )
        )

    fig_running.update_layout(
        title=dict(text="Running Time per Dataset", font=dict(size=24)),
        barmode="group",
        xaxis=dict(title="Dataset", tickangle=-45, tickfont=dict(size=14)),
        yaxis=dict(
            title="Running Time (minutes)",
            tickfont=dict(size=14),
            gridcolor="rgba(200,200,200,0.3)",
        ),
        plot_bgcolor="rgba(245,245,245,1)",
        paper_bgcolor="rgba(245,245,245,1)",
        legend=dict(title="Algorithm", font=dict(size=14)),
        margin=dict(l=80, r=80, t=100, b=120),
    )

    pio.write_image(
        fig_running,
        os.path.join(output_path, "datasets_running_time.png"),
        format="png",
        width=800,
        height=550,
    )


    fig_first = go.Figure()

    for i, (label, values) in enumerate(first_iterations.items()):
        fig_first.add_trace(
            go.Bar(
                x=datasets,
                y=values,
                name=label,
                width=0.3,
                marker=dict(
                    color=colors[i % len(colors)], line=dict(width=1.5, color="black")
                ),
                text=[f"{v:.2f}" for v in values],
                textposition="outside",
            )
        )

    fig_first.update_layout(
        title=dict(text="First Iteration Time per Dataset", font=dict(size=24)),
        barmode="group",
        xaxis=dict(title="Dataset", tickangle=-45, tickfont=dict(size=14)),
        yaxis=dict(
            title="First Iteration Time (minutes)",
            tickfont=dict(size=14),
            gridcolor="rgba(200,200,200,0.3)",
        ),
        plot_bgcolor="rgba(245,245,245,1)",
        paper_bgcolor="rgba(245,245,245,1)",
        legend=dict(title="Algorithm", font=dict(size=14)),
        margin=dict(l=80, r=80, t=100, b=120),
    )

    pio.write_image(
        fig_first,
        os.path.join(output_path, "datasets_first_it_running_time.png"),
        format="png",
        width=800,
        height=550,
    )


def compute_metrics(data_to_analyze, output_path):
    table = ["Test", "Running time (m)", "Mean mem (GB)", "Mean cpu (%)", "Predicted", "Accuracy", "Purity", "NMI", "ARI", "Avg Degree"]
    cols = len(table)
    rows = 1

    for label, data in data_to_analyze.items():
        args = parse_folder_name(label)

        title_final = "single process" if args["sm"] else f"MR gamma={args['g']}"
        title = f"{str(args['dataset']).capitalize()}, {title_final} l={args['l']}"

        resources = data["resources"]
        output = data["output"]

        predicted_comms, degree = load_communities(output['communities'], output['degree'])
        ground_truth_comms = load_ground_truth(f"testgraphs/{args['dataset']}_top5000.txt")
    

        (exact_correct, exact_accuracy, wrong_exact), (inclusion_correct, inclusion_accuracy, wrong_inclusion), total = compare_communities_sets(predicted_comms, ground_truth_comms)

        purity_score = compute_purity(predicted_comms, ground_truth_comms)
        nmi, ari = compute_nmi_ari(predicted_comms, ground_truth_comms)
        avg_degree = average_degree(degree)

        main_time = int(
            (output["main_end_timestamp"] - output["main_start_timestamp"]) / 60.0
        )
        main_python_mem = resources["main_python_mem"]
        main_python_cpu = resources["main_python_cpu"]
        java_mem = resources["java_mem"]
        java_cpu = resources["java_cpu"]
        workers_mem = resources["workers_mem"]
        workers_cpu = resources["workers_cpu"]

        mean_mem = np.round(
            np.mean(np.array(main_python_mem + java_mem + workers_mem) / (1024**3)), 2
        )
        mean_cpu = np.round(
            np.mean(np.array(main_python_cpu + java_cpu + workers_cpu)), 2
        )

        table.extend([title, main_time, mean_mem, mean_cpu, len(predicted_comms), inclusion_accuracy, round(purity_score, 4), round(nmi, 4), round(ari, 4), round(avg_degree, 4)])
        rows += 1

    mdFile = MdUtils(file_name=os.path.join(output_path, "metrics.md"))
    mdFile.new_table(columns=cols, rows=rows, text=table, text_align="center")
    mdFile.create_md_file()


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
    compute_metrics(data_to_analyze, args.f)
