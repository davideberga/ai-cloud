from single_attractor.Settings import Settings

class EdgeValue:
    def __init__(self, disuv):
        # Inizializza array delle distanze
        self.a_distance = [0.0] * Settings.STEP_LENGTH
        
        # Validazione del valore di distanza
        assert 0 <= disuv <= 1, "disuv deve essere compreso tra 0 e 1"
        self.weight = disuv
        
        # TreeSet equivale a set ordinato in Python
        # Per mantenere l'ordinamento si può usare una lista ordinata o set normale
        self.p_common_neighbours = set()
        
        # Lista di due set per i vicini esclusivi
        self.p_exclusive_neighbours = [set(), set()]
        
        # Variabili per la sliding window
        self.b_delta_window = None
        self.i_newest_delta_index = 0
    
    def add_new_delta_2_window(self, d_delta):
        """Checking sliding windows"""
        if self.b_delta_window is None:
            # BitSet equivale a un set di indici in Python
            self.b_delta_window = set()
        
        # Gestione della sliding window
        window_index = self.i_newest_delta_index % Settings.SLIDINNG_WINDOW_SIZE
        
        if d_delta < 0:
            # Clear bit (rimuovi dall'insieme)
            self.b_delta_window.discard(window_index)
        else:
            # Set bit (aggiungi all'insieme)
            self.b_delta_window.add(window_index)
        
        i_sum_same_sign = 0
        if self.i_newest_delta_index >= Settings.SLIDINNG_WINDOW_SIZE - 1:
            if window_index in self.b_delta_window:
                # Conta i bit settati (cardinality)
                i_sum_same_sign = len(self.b_delta_window)
                if i_sum_same_sign > Settings.DEFAULT_SUPPORT_SLIDING_WINDOW * Settings.SLIDINNG_WINDOW_SIZE:
                    d_delta = 2
            else:
                i_sum_same_sign = Settings.SLIDINNG_WINDOW_SIZE - len(self.b_delta_window)
                if i_sum_same_sign > Settings.DEFAULT_SUPPORT_SLIDING_WINDOW * Settings.SLIDINNG_WINDOW_SIZE:
                    d_delta = -2
        
        self.i_newest_delta_index += 1
        return d_delta
    
    def set_sliding_window(self, current_loop, status):
        """
        Updating the slidingWindow from previous MR version. 
        This is used to guarantee the correctness of this algorithm.
        
        Args:
            current_loop: the number of loops of MR version
            status: The sliding window status from iteration 0 to current iteration
        """
        if self.b_delta_window is None:
            self.b_delta_window = set()
        
        assert current_loop == len(status), "current_loop deve essere uguale alla lunghezza di status"
        assert len(status) <= Settings.SLIDINNG_WINDOW_SIZE, "status troppo lungo per la sliding window"
        
        for i in range(len(status)):
            assert status[i] in [0, 1], "status[i] deve essere 0 o 1"
            if status[i] == 1:
                self.b_delta_window.add(i)
        
        self.i_newest_delta_index = len(status)
    
    def init_window_size(self, i_size):
        """Inizializza la dimensione della finestra"""
        Settings.SLIDINNG_WINDOW_SIZE = i_size