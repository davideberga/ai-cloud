from single_attractor.Settings import Settings


class VertexValue:
    """
    Classe che rappresenta il valore di un vertice nel grafo
    """
    
    def __init__(self):
        """
        Costruttore della classe VertexValue
        
        Nota: Utilizza un set ordinato (sorted set simulato con set normale) 
        per mantenere i vicini ordinati e migliorare l'efficienza della memoria.
        Le iterazioni su strutture linkate sono più lente degli array ma 
        necessarie per questa implementazione.
        """
        # In Python, set() è già efficiente. Per un comportamento simile a TreeSet
        # potremmo usare una lista ordinata, ma set() è generalmente preferibile
        self.p_neighbours = set()
        
        # Array per la somma dei pesi per ogni step
        self.a_weight_sum = [0.0] * Settings.STEP_LENGTH
        self.a_weight_sum[0] = 0.0
        self.a_weight_sum[1] = 0.0