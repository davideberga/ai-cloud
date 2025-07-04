class Helper:
    """
    Classe di utilità per la gestione degli step
    """
    
    @staticmethod
    def compute_next_step(i_current_step):
        """
        Calcola il prossimo step alternando tra 0 e 1
        
        Args:
            i_current_step (int): Step corrente
            
        Returns:
            int: Prossimo step (0 se corrente è 1, 1 se corrente è 0)
        """
        return 1 if i_current_step == 0 else 0
    
    @staticmethod
    def update_step(i_current_step):
        """
        Aggiorna lo step corrente al prossimo
        
        Args:
            i_current_step (int): Step corrente
            
        Returns:
            int: Nuovo step aggiornato
        """
        i_current_step = Helper.compute_next_step(i_current_step)
        return i_current_step
    
    @staticmethod
    def next_step(i_current_step):
        """
        Restituisce il prossimo step senza modificare il parametro
        
        Args:
            i_current_step (int): Step corrente
            
        Returns:
            int: Prossimo step
        """
        return Helper.compute_next_step(i_current_step)