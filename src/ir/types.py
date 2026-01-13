from enum import Enum

class DataType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    DATE = "date"
    UNKNOWN = "unknown"  # Critical for "Ghost Columns"

class OpType(str, Enum):
    # IO
    LOAD_CSV = "load_csv"
    SAVE_BINARY = "save_binary"
    
    # Logic
    COMPUTE = "compute_columns"
    FILTER = "filter_rows"
    AGGREGATE = "aggregate"
    JOIN = "join"
    SORT = "sort" # <--- 🟢 NEW!
    
    # Structural / Legacy
    GENERIC = "generic_transform"
    MATERIALIZE = "materialize"
    
    # Optimizations
    BATCH_COMPUTE = "batch_compute"    