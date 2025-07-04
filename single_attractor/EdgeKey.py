class EdgeKey:
    def __init__(self, i_begin, i_end):
        # Assumendo che Assert.assertTrue sia implementato altrove
        assert i_begin > i_end, "i_begin deve essere maggiore di i_end"
        self.i_begin = i_begin
        self.i_end = i_end
    
    def __str__(self):
        return f"{self.i_begin} {self.i_end}"
    
    def __hash__(self):
        return hash(str(self))
    
    def __eq__(self, other):
        if not isinstance(other, EdgeKey):
            return False
        return self.i_begin == other.i_begin and self.i_end == other.i_end