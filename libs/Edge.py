from libs.Settings import Settings
import numpy as np
from bitarray import bitarray

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
            self.deltaWindow = bitarray(window_size)
            self.deltaWindow.setall(0)
        
        window_index = self.i_newest_delta_index % window_size
        
        self.deltaWindow[window_index] = int(d_delta >= 0)
        
        i_sum_same_sign = 0
        
        if self.i_newest_delta_index >= window_size - 1:
            if self.deltaWindow[window_index]:
                sum_ = self.deltaWindow.count(1)
                if sum_ > self.DEFAULT_SUPPORT_SLIDING_WINDOW * window_size:
                    d_delta = 2
            else:
                i_sum_same_sign = window_size - self.deltaWindow.count(0)
                if i_sum_same_sign > self.DEFAULT_SUPPORT_SLIDING_WINDOW * window_size:
                    d_delta = -2
        self.i_newest_delta_index += 1
        return d_delta
    
    def set_sliding_window(self, current_loop, status):
        if self.deltaWindow is None:
            self.deltaWindow = bitarray(len(status))
            self.deltaWindow.setall(0)
        
        for i in range(current_loop):
            #self.deltaWindow[i] = int(status[i])
            self.deltaWindow = bitarray(status)
        
        self.i_newest_delta_index = current_loop