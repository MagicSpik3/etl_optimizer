import pytest
from src.ir.model import Pipeline, Operation, Dataset
from src.ir.types import OpType
from src.optimizer.validator import SecurityValidator

class TestValidatorIslands:
    
    def test_flags_valid_multi_source_as_disconnected_before_join(self):
        """
        CRITICAL TEST: 
        If we have 3 source files that HAVEN'T joined yet, 
        does the validator flag them as islands?
        
        Answer: YES. And this might be why the integration test fails.
        """
        # Three separate chains (like Control, Benefit, Claims)
        ops = [
            Operation(id="load1", type=OpType.LOAD_CSV, inputs=[], outputs=["ds1"]),
            Operation(id="load2", type=OpType.LOAD_CSV, inputs=[], outputs=["ds2"]),
            Operation(id="load3", type=OpType.LOAD_CSV, inputs=[], outputs=["ds3"]),
        ]
        ds = [Dataset(id=x, source="f") for x in ["ds1", "ds2", "ds3"]]
        
        pipeline = Pipeline(datasets=ds, operations=ops)
        validator = SecurityValidator(pipeline)
        
        errors = validator.validate_topology()
        
        # If this asserts TRUE, then our Integration Test failure is actually correct behavior
        # (The pipeline is technically disconnected until the joins happen)
        assert any("Disconnected component" in e for e in errors)
        assert "Found 3 islands" in str(errors)

    def test_passes_fully_connected_graph(self):
        """
        Scenario: Sources eventually join.
        Load A \
                -> Join -> Result
        Load B /
        """
        ops = [
            Operation(id="l1", type=OpType.LOAD_CSV, inputs=[], outputs=["A"]),
            Operation(id="l2", type=OpType.LOAD_CSV, inputs=[], outputs=["B"]),
            Operation(id="join", type=OpType.JOIN, inputs=["A", "B"], outputs=["C"])
        ]
        ds = [Dataset(id=x, source="f") for x in ["A", "B", "C"]]
        
        pipeline = Pipeline(datasets=ds, operations=ops)
        validator = SecurityValidator(pipeline)
        
        errors = validator.validate_topology()
        assert len(errors) == 0