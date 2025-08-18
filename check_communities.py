import numpy as np
import sys
from sklearn.metrics import normalized_mutual_info_score, adjusted_rand_score

def load_npz_communities(npz_path):
    data = np.load(npz_path, allow_pickle=True)
    if 'communities' not in data:
        raise KeyError("There is no 'communities' key in the .npz file")
    node_to_comm = data['communities'].item()
    comm_to_nodes = {}
    for node, comm in node_to_comm.items():
        comm = int(comm)
        node = int(node)
        comm_to_nodes.setdefault(comm, set()).add(node)

    if 'degree' not in data:
        raise KeyError("There is no 'degree' key in the .npz file")
    degree = data['degree'].item()

    return comm_to_nodes, degree

def load_ground_truth(txt_path):
    comm_to_nodes = {}
    with open(txt_path, 'r') as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) < 2:
                continue
            comm_id = int(parts[0])
            nodes = set(map(int, parts[1:]))
            comm_to_nodes[comm_id] = nodes
    return comm_to_nodes

def compare_communities_sets(predicted, ground_truth):
    predicted_fsets = {frozenset(nodes) for nodes in predicted.values()}
    predicted_sets = list(predicted.values())  # for inclusion

    exact_correct = 0
    inclusion_correct = 0
    total = len(ground_truth)
    wrong_exact = []
    wrong_inclusion = []

    for gt_comm_id, gt_nodes in ground_truth.items():
        f_gt_nodes = frozenset(gt_nodes)

        if f_gt_nodes in predicted_fsets:
            exact_correct += 1
        else:
            wrong_exact.append(gt_comm_id)

        if any(gt_nodes.issubset(pred_set) for pred_set in predicted_sets):
            inclusion_correct += 1
        else:
            wrong_inclusion.append(gt_comm_id)

    exact_accuracy = exact_correct / total * 100 if total else 0
    inclusion_accuracy = inclusion_correct / total * 100 if total else 0

    return (exact_correct, exact_accuracy, wrong_exact), (inclusion_correct, inclusion_accuracy, wrong_inclusion), total

def compute_purity(predicted, ground_truth):
    node_to_gt = {}
    for new_label, nodes in enumerate(ground_truth.values()):
        for node in nodes:
            node_to_gt[node] = new_label

    # Consider only the node with a ground truth
    common_nodes = set(node_to_gt.keys())
    total_nodes = len(common_nodes)
    if total_nodes == 0:
        return 0.0

    correct = 0
    for pred_nodes in predicted.values():
        label_counts = {}
        for node in pred_nodes:
            if node in node_to_gt:  # Consider only nodes with ground truth
                gt_label = node_to_gt[node]
                label_counts[gt_label] = label_counts.get(gt_label, 0) + 1
        if label_counts:
            correct += max(label_counts.values())

    return correct / total_nodes


def compute_nmi_ari(predicted, ground_truth):
    # Associating new ID for avoid dependencies with gt labels
    node_to_gt = {}
    for new_label, nodes in enumerate(ground_truth.values()):
        for node in nodes:
            node_to_gt[node] = new_label

    node_to_pred = {}
    for new_label, nodes in enumerate(predicted.values()):
        for node in nodes:
            node_to_pred[node] = new_label

    # Only nodes present in both
    common_nodes = sorted(set(node_to_gt.keys()) & set(node_to_pred.keys()))
    gt_labels = [node_to_gt[n] for n in common_nodes]
    pred_labels = [node_to_pred[n] for n in common_nodes]

    nmi = normalized_mutual_info_score(gt_labels, pred_labels)
    ari = adjusted_rand_score(gt_labels, pred_labels)
    return nmi, ari

def average_degree(degree_dict):
    if not degree_dict:
        return 0.0
    return sum(degree_dict.values()) / len(degree_dict)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python check_communities.py <file.npz> <top5000_remapped.txt>")
        sys.exit(1)

    npz_file = sys.argv[1]
    txt_file = sys.argv[2]

    predicted_comms, degree = load_npz_communities(npz_file)
    ground_truth_comms = load_ground_truth(txt_file)

    (exact_correct, exact_accuracy, wrong_exact), (inclusion_correct, inclusion_accuracy, wrong_inclusion), total = compare_communities_sets(predicted_comms, ground_truth_comms)

    purity_score = compute_purity(predicted_comms, ground_truth_comms)
    nmi, ari = compute_nmi_ari(predicted_comms, ground_truth_comms)
    avg_degree = average_degree(degree)

    print(f"Total communities compared: {total}")

    print("\n--- Exact Match ---")
    print(f"Correct communities: {exact_correct}")
    print(f"Wrong communities: {total - exact_correct}")
    print(f"Accuracy: {exact_accuracy:.2f}%")

    print("\n--- Inclusion ---")
    print(f"Correct communities: {inclusion_correct}")
    print(f"Wrong communities: {total - inclusion_correct}")
    print(f"Accuracy: {inclusion_accuracy:.2f}%")

    print("\n--- Clustering Quality ---")
    print(f"Purity: {purity_score:.4f}")
    print(f"NMI: {nmi:.4f}")
    print(f"ARI: {ari:.4f}")
    print(f"AVG: {avg_degree:.4f}")