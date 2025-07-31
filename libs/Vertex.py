class Vertex:
    def __init__(self, step_length=100):
        
        self.neighbours = set()
        self.aWeightSum = [0.0] * step_length
        
        if step_length > 1:
            self.aWeightSum[0] = 0.0
            self.aWeightSum[1] = 0.0