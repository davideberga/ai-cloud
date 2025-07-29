from libs.Settings import Settings

class EdgeInfo:
    def __init__(self, disuv):

        self.b_delta_window = None
        self.i_newest_delta_index = 0
        self.a_distance = [0.0] * Settings.STEP_LENGTH
        
        # Assertion per verificare che disuv sia nell'intervallo corretto
        assert 0 <= disuv <= 1, f"disuv deve essere tra 0 e 1, ricevuto: {disuv}"
        
        self.weight = disuv
        
        # TODO
        # TreeSet è implementato come set ordinato in Python
        # In Python, set() è efficiente ma non mantiene l'ordine
        # Per mantenere l'ordine come TreeSet, si può usare una lista ordinata
        # o importare from sortedcontainers import SortedSet
        self.pCommonNeighbours = set()
        
        # Lista di due set per i vicini esclusivi
        self.pExclusiveNeighbours = [set(), set()]
    
    def add_new_delta_2_window(self, d_delta):
        """
        Aggiunge un nuovo delta alla finestra scorrevole
        
        Args:
            d_delta (float): Il valore delta da aggiungere
            
        Returns:
            float: Il valore delta modificato se necessario
        """
        if self.b_delta_window is None:
            # In Python, possiamo usare una lista di bool o un set per simulare BitSet
            self.b_delta_window = [False] * 32
        
        window_index = self.i_newest_delta_index % Settings.SLIDINNG_WINDOW_SIZE
        
        if d_delta < 0:
            self.b_delta_window[window_index] = False
        else:
            self.b_delta_window[window_index] = True
        
        i_sum_same_sign = 0
        
        if self.i_newest_delta_index >= Settings.SLIDINNG_WINDOW_SIZE - 1:
            if self.b_delta_window[window_index]:
                # Conta il numero di bit settati (True) nella finestra
                i_sum_same_sign = sum(
                    1 for i in range(Settings.SLIDINNG_WINDOW_SIZE) 
                    if i < len(self.b_delta_window) and self.b_delta_window[i]
                )
                
                if i_sum_same_sign > Settings.DEFAULT_SUPPORT_SLIDING_WINDOW * Settings.SLIDINNG_WINDOW_SIZE:
                    d_delta = 2
            else:
                # Conta il numero di bit non settati (False) nella finestra
                i_sum_same_sign = Settings.SLIDINNG_WINDOW_SIZE - sum(
                    1 for i in range(Settings.SLIDINNG_WINDOW_SIZE) 
                    if i < len(self.b_delta_window) and self.b_delta_window[i]
                )
                
                if i_sum_same_sign > Settings.DEFAULT_SUPPORT_SLIDING_WINDOW * Settings.SLIDINNG_WINDOW_SIZE:
                    d_delta = -2
        
        self.i_newest_delta_index += 1
        return d_delta
    
    def set_sliding_window(self, current_loop, status):
        """
        Aggiorna la finestra scorrevole dalla versione MR precedente
        
        Args:
            current_loop (int): Il numero di loop della versione MR
            status (list): Lo stato della finestra scorrevole dall'iterazione 0 a quella corrente
        """
        if self.b_delta_window is None:
            self.b_delta_window = [False] * 32
        
        assert current_loop == len(status), f"current_loop ({current_loop}) deve essere uguale alla lunghezza di status ({len(status)})"
        assert len(status) <= Settings.SLIDINNG_WINDOW_SIZE, f"La lunghezza di status ({len(status)}) deve essere <= SLIDINNG_WINDOW_SIZE ({Settings.SLIDINNG_WINDOW_SIZE})"
        
        for i in range(len(status)):
            assert status[i] == 0 or status[i] == 1, f"status[{i}] deve essere 0 o 1, ricevuto: {status[i]}"
            if status[i] == 1:
                self.b_delta_window[i] = True
        
        self.i_newest_delta_index = len(status)
    
    def init_window_size(self, i_size):
        """
        Inizializza la dimensione della finestra
        
        Args:
            i_size (int): La nuova dimensione della finestra
        """
        Settings.SLIDINNG_WINDOW_SIZE = i_size