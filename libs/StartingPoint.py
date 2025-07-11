import sys
import CommunityDetection_v2


def main():
    try:
        # Parse command line arguments
        graphfile = sys.argv[1]
        no_vertices = int(sys.argv[2])
        no_edges = int(sys.argv[3])
        lambda_ = float(sys.argv[4])
        
        # Create and execute community detection
        single_attractor = CommunityDetection_v2(graphfile, no_vertices, no_edges, lambda_)
        single_attractor.execute()
        
    except IndexError:
        print("Error: Not enough command line arguments provided")
        print("Usage: python starting_point.py <graphfile> <no_vertices> <no_edges> <lambda>")
        sys.exit(1)
    except ValueError as e:
        print(f"Error: Invalid argument format - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()