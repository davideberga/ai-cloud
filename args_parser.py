import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Run MrAttractor with specified parameters"
    )

    parser.add_argument("--graph-file", required=True, help="Path to the input graph file")
    parser.add_argument("--output-folder", required=True, help="Path to the output folder")
    parser.add_argument("--lambda-value", type=float, required=True, help="Lambda value for similarity computation")
    parser.add_argument("--mb-per-reducer", type=int, required=True, help="Memory per reducer in MB")
    parser.add_argument("--num-vertices", type=int, required=True, help="Number of vertices")
    parser.add_argument("--num-edges", type=int, required=True, help="Number of edges")
    parser.add_argument("--window-size", type=int, required=True, help="Size of the sliding window")
    parser.add_argument("--miu", type=float, required=True, help="Threshold for edge convergence in sliding window")
    parser.add_argument("--threshold-used-edges", type=int, required=True, help="Edge threshold to switch to single-machine mode")
    parser.add_argument("--num-partitions-dynamic", type=int, required=True, help="Partitions for dynamic interaction phase")

    return parser.parse_args()