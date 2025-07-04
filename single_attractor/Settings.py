class Settings:
    """
    Classe che contiene le impostazioni e costanti globali del sistema
    """
    
    # Costanti per la gestione degli step
    STEP_LENGTH = 2
    PRECISE = 0.0000001
    
    # Costanti per la gestione degli archi
    EDGE_ENDPOINT_NUMBER = 2
    BEGIN_POINT = 0
    END_POINT = 1
    
    # Parametro lambda per algoritmi
    lambda_param = 0.5  # Rinominato da 'lambda' (parola riservata in Python)
    
    # Impostazioni per sliding window
    SLIDING_WINDOW_SIZE = -1  # Corretto il typo da SLIDINNG_WINDOW_SIZE
    LIMIT_SIZE_DICT_VIRTUAL_EDGES = -1
    DEFAULT_SUPPORT_SLIDING_WINDOW = -0.7