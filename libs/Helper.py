class Helper:
    
    @staticmethod
    def compute_next_step(i_current_step):
        return 1 if i_current_step == 0 else 0
    
    @staticmethod
    def update_step(i_current_step):
        i_current_step = Helper.compute_next_step(i_current_step)
        return i_current_step
    
    @staticmethod
    def next_step(i_current_step):
        return Helper.compute_next_step(i_current_step)