import numpy as np
import os
import time

class Details:

    def __init__(self, output_path: str):
        self.output_path = output_path

        self.main_start_timestamp = 0.0
        self.main_end_timestamp = 0.0
        self.update_edges_timestamp = []
        self.partitions_computed = 0.0
        self.sm_timestamp = []
        self.n_community = 0
        self.communities = {}
        self.degree = {}

    def save(self):
        np.savez(
            os.path.join(self.output_path, "details.npz"),
            main_start_timestamp=self.main_start_timestamp,
            main_end_timestamp=self.main_end_timestamp,
            update_edges_timestamp=np.array(self.update_edges_timestamp),
            partitions_computed=self.partitions_computed,
            sm_timestamp=np.array(self.sm_timestamp),
            n_community=np.array(self.n_community),
            communities=np.array(self.communities, dtype=object),
            degree=np.array(self.degree, dtype=object),
        )

    @staticmethod
    def current_timestamp():
        return time.time()