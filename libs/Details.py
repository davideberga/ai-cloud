import numpy as np
import os
from datetime import datetime

class Details:

    def __init__(self, output_path: str):
        self.output_path = output_path

        self.jaccard_timestamp = 0
        self.partitions_timestamp = 0
        self.star_graph_timestamp = []
        self.dynamic_interactions_timestamp = []
        self.update_edges_timestamp = []
        self.sm_timestamp = []
        self.n_community = 0
        self.communities = {}

    def save(self):
        np.savez(
            os.path.join(self.output_path, "details.npz"),
            jaccard_timestamp=np.array(self.jaccard_timestamp),
            partitions_timestamp=np.array(self.partitions_timestamp),
            star_graph_timestamp=np.array(self.star_graph_timestamp),
            dynamic_interactions_timestamp=np.array(self.dynamic_interactions_timestamp),
            update_edges_timestamp=np.array(self.update_edges_timestamp),
            sm_timestamp=np.array(self.sm_timestamp),
            n_community=np.array(self.n_community),
            communities=np.array(self.communities, dtype=object),
        )

    @staticmethod
    def current_timestamp():
        return datetime.now().strftime("%H:%M:%S")