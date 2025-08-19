import argparse


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Run MrAttractor with specified parameters"
    )

    parser.add_argument(
        "-g", "--graph-file", required=True, help="Path to the input graph file"
    )
    parser.add_argument(
        "-o", "--output-folder", required=True, help="Path to the output folder"
    )
    parser.add_argument(
        "-l",
        "--lambda",
        type=float,
        dest="lambda_",
        required=True,
        help="Lambda value for similarity computation",
    )
    parser.add_argument(
        "-w",
        "--window-size",
        type=int,
        required=True,
        help="Size of the sliding window",
    )
    parser.add_argument(
        "-t",
        "--tau",
        type=float,
        required=True,
        help="Threshold for edge convergence in sliding window",
    )
    parser.add_argument(
        "-G",
        "--gamma",
        type=int,
        required=True,
        help="Edge threshold to switch to single-machine mode",
    )
    parser.add_argument(
        "-p",
        "--num-partitions",
        type=int,
        required=True,
        help="Partitions for dynamic interaction phase",
    )

    parser.add_argument(
        "-sm",
        "--single-machine",
        action="store_true",
        required=False,
        default=False,
        help="Run all in single-machine mode",
    )

    return parser.parse_args()


def parse_arguments_monitor():
    parser = argparse.ArgumentParser(description="MRAttractor monitor")

    parser.add_argument("-g", required=True, help="Path to the input graph file")
    parser.add_argument("-o", required=True, help="Path to the output folder")
    parser.add_argument(
        "-l",
        type=float,
        required=True,
        help="Lambda value for similarity computation",
    )
    parser.add_argument(
        "-w", type=int, required=True, help="Size of the sliding window"
    )
    parser.add_argument(
        "-t",
        type=float,
        required=True,
        help="Threshold for edge convergence in sliding window",
    )
    parser.add_argument(
        "-G",
        type=int,
        required=True,
        help="Edge threshold to switch to single-machine mode",
    )
    parser.add_argument(
        "-p", type=int, required=True, help="Partitions for dynamic interaction phase"
    )

    parser.add_argument(
        "-sm", required=False, default=False, help="Run all in single-machine mode",
        action="store_true",
    )

    return parser.parse_args()


def stringify_args(args):
    parts = []
    for k, v in vars(args).items():
        if isinstance(v, bool):
            if v:
                parts.append(f"-{k}")
        else:
            parts.append(f"-{k}")
            parts.append(f"{v}")
    return parts
