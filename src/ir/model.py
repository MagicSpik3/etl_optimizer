from typing import List, Dict, Optional, Any, Union
from pydantic import BaseModel, Field, validator
from src.ir.types import OpType, DataType

class Column(BaseModel):
    name: str
    type: DataType

class Dataset(BaseModel):
    id: str
    source: str
    columns: List[Column] = Field(default_factory=list)

class Operation(BaseModel):
    id: str
    type: OpType
    # Edge Case: Inputs/Outputs must be lists. 
    # If the YAML has a single string, we could coerce it, 
    # but strict typing prevents hidden bugs.
    inputs: List[str] = Field(default_factory=list)
    outputs: List[str] = Field(default_factory=list)
    
    # Edge Case: Parameters might be missing in YAML (e.g. for Materialize)
    # We default to empty dict so code doesn't crash on op.params['x']
    parameters: Dict[str, Any] = Field(default_factory=dict)

class Pipeline(BaseModel):
    metadata: Dict[str, str] = Field(default_factory=dict)
    datasets: List[Dataset]
    operations: List[Operation]