from enum import Enum

class EdgeTypeEnum(Enum):
    G = 'G'
    S = 'S'

class Settings:
    
    DEBUG = True
    
    STEP_LENGTH = 2
    PRECISE = 0.0000001
    EDGE_ENDPOINT_NUMBER = 2
    BEGIN_POINT = 0
    END_POINT = 1
    lambda_ = 0.5  # Note: 'lambda' is a Python keyword, so using 'lambda_'
    
    SLIDINNG_WINDOW_SIZE = -1  # Kept original typo: SLIDINNG instead of SLIDING
    LIMIT_SIZE_DICT_VIRTUAL_EDGES = -1
    DEFAULT_SUPPORT_SLIDING_WINDOW = -0.7
    log_each_iteration = None  # Python equivalent of null