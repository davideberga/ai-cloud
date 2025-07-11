class EdgeKey_v2:
    def __init__(self, i_begin, i_end):
        # Assumendo che Assert.assertTrue sia implementato in un altro file
        # e importato come necessario
        assert i_begin > i_end, "iBegin deve essere maggiore di iEnd"
        
        self.i_begin = i_begin
        self.i_end = i_end
    
    def __str__(self):
        return f"{self.i_begin} {self.i_end}"
    
    def __hash__(self):
        return hash(str(self))
    
    def __eq__(self, other):
        # Aggiunto per completezza quando si implementa __hash__
        if not isinstance(other, EdgeKey_v2):
            return False
        return self.i_begin == other.i_begin and self.i_end == other.i_end