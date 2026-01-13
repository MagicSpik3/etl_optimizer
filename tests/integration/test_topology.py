import pytest
from src.ir.model import Pipeline, Operation, Dataset
from src.ir.types import OpType
from src.optimizer.validator import SecurityValidator

class TestGraphTopology:
    
    def test_detects_disconnected_components(self):
        """
        Scenario: The 'Island'.
        Load A -> Compute B.
        Load X -> Compute Y.
        
        The pipeline has two disconnected sub-graphs. 
        While valid in some contexts, strictly speaking, 
        Compute Y might be 'Dead Code' if it never reaches a SAVE.
        """
        ds_a = Dataset(id="ds_a", source="file", columns=[])
        ds_b = Dataset(id="ds_b", source="derived", columns=[])
        ds_x = Dataset(id="ds_x", source="file", columns=[])
        ds_y = Dataset(id="ds_y", source="derived", columns=[])
        
        ops = [
            # Graph 1
            Operation(id="op_1", type=OpType.LOAD_CSV, inputs=[], outputs=["ds_a"]),
            Operation(id="op_2", type=OpType.COMPUTE, inputs=["ds_a"], outputs=["ds_b"]),
            
            # Graph 2 (The Island - No Save, No Connection to Graph 1)
            Operation(id="op_3", type=OpType.LOAD_CSV, inputs=[], outputs=["ds_x"]),
            Operation(id="op_4", type=OpType.COMPUTE, inputs=["ds_x"], outputs=["ds_y"])
        ]
        
        pipeline = Pipeline(datasets=[ds_a, ds_b, ds_x, ds_y], operations=ops)
        validator = SecurityValidator(pipeline)
        
        # We expect a warning about Dead Code or Disconnected Graph
        errors = validator.validate_topology()
        assert any("Disconnected component" in e for e in errors)

    def test_detects_cycles(self):
        """
        Scenario: The 'Ouroboros'.
        A -> B -> A.
        This is a fatal error in DAG-based ETL.
        """
        ds_a = Dataset(id="ds_a", source="file", columns=[])
        ds_b = Dataset(id="ds_b", source="derived", columns=[])
        
        ops = [
            # A -> B
            Operation(id="op_1", type=OpType.COMPUTE, inputs=["ds_a"], outputs=["ds_b"]),
            # B -> A (Cycle!)
            Operation(id="op_2", type=OpType.COMPUTE, inputs=["ds_b"], outputs=["ds_a"])
        ]
        
        pipeline = Pipeline(datasets=[ds_a, ds_b], operations=ops)
        validator = SecurityValidator(pipeline)
        
        errors = validator.validate_topology()
        assert any("Cycle detected" in e for e in errors)

    def test_detects_broken_lineage(self):
        """
        Scenario: The 'Broken Bridge'.
        A -> B -> C.
        If we delete B, A cannot reach C.
        """
        ds_a = Dataset(id="ds_a", source="file", columns=[])
        ds_c = Dataset(id="ds_c", source="derived", columns=[])
        
        ops = [
            Operation(id="op_1", type=OpType.LOAD_CSV, inputs=[], outputs=["ds_a"]),
            # Missing op that produces ds_b!
            Operation(id="op_2", type=OpType.COMPUTE, inputs=["ds_b"], outputs=["ds_c"])
        ]
        
        pipeline = Pipeline(datasets=[ds_a, ds_c], operations=ops)
        validator = SecurityValidator(pipeline)
        
        errors = validator.validate_topology()
        assert any("Missing input dataset 'ds_b'" in e for e in errors)