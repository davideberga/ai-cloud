class VertexValue_v2:
    def __init__(self, step_length=100):
        # `step_length` è il numero di passi, da sostituire con Settings.STEP_LENGTH se definito altrove
        self.pNeighbours = []
        self.aWeightSum = [0.0] * step_length
        if step_length > 1:
            self.aWeightSum[0] = 0.0
            self.aWeightSum[1] = 0.0