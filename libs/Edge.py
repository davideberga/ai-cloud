from libs.Settings import Settings
import numpy as np

class Edge:
    
    
    
    def __init__(self,vertex_start, vertex_end, disuv):

        self.deltaWindow = None
        self.i_newest_delta_index = 0
        self.a_distance = [0.0] * 2
        self.sliding_window = None
        
        assert 0 <= disuv <= 1, f"disuv have to be between 0 and 1, received: {disuv}"

        
        self.vertex_start = vertex_start
        self.vertex_end = vertex_end
        
        self.weight = disuv
        
        self.common_n = set()
        self.exclusive_n = [set(), set()]
        self.DEFAULT_SUPPORT_SLIDING_WINDOW = -0.7
    
    def add_new_delta_2_window(self, d_delta, window_size: int):

        if self.deltaWindow is None:
            self.deltaWindow = np.zeros((32), dtype=bool)
        
        window_index = self.i_newest_delta_index % window_size
        
        if d_delta < 0:
            self.deltaWindow[window_index] = False
        else:
            self.deltaWindow[window_index] = True
        
        i_sum_same_sign = 0
        
        if self.i_newest_delta_index >= window_size - 1:
            if self.deltaWindow[window_index]:
                sum_ = sum(self.deltaWindow)
                if sum_ > self.DEFAULT_SUPPORT_SLIDING_WINDOW * window_size:
                    d_delta = 2
            else:
                i_sum_same_sign = window_size - sum(self.deltaWindow)
                if i_sum_same_sign > self.DEFAULT_SUPPORT_SLIDING_WINDOW * window_size:
                    d_delta = -2
        self.i_newest_delta_index += 1
        return d_delta
    
    def set_sliding_window(self, current_loop, status):
        if self.deltaWindow is None:
            self.deltaWindow = self.deltaWindow = np.zeros((32), dtype=bool)
        
        for i in range(current_loop):
            self.deltaWindow[i] = status[i]
        
        self.i_newest_delta_index = current_loop
    
    def init_window_size(self, i_size):
        """
        Inizializza la dimensione della finestra
        
        Args:
            i_size (int): La nuova dimensione della finestra
        """
        Settings.SLIDINNG_WINDOW_SIZE = i_size