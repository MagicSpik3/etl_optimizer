import pytest
from etl_ir.model import Pipeline, Operation, Dataset, Column
from etl_ir.types import OpType, DataType
# We haven't created this yet
from src.optimizer.validator import SecurityValidator

class TestSecurityValidator:
    
    def test_detects_ghost_column_usage(self):
        """
        Scenario: Compute operation tries to use 'salary' but input only has 'age'.
        Expected: Validation Error.
        """
        # Dataset has 'age'
        ds1 = Dataset(id="ds1", source="file", columns=[Column(name="age", type=DataType.INTEGER)])
        ds2 = Dataset(id="ds2", source="derived", columns=[])
        
        # Operation tries to use 'salary' in expression
        op = Operation(
            id="op1", 
            type=OpType.COMPUTE_COLUMNS, 
            inputs=["ds1"], 
            outputs=["ds2"], 
            # 'salary' is the ghost column here
            parameters={"target": "bonus", "expression": "salary * 0.1"} 
        )
        
        pipeline = Pipeline(datasets=[ds1, ds2], operations=[op])
        
        validator = SecurityValidator(pipeline)
        errors = validator.run()
        
        # We expect a report containing the error
        assert len(errors) == 1
        assert "Ghost Column" in errors[0]
        assert "salary" in errors[0]

    def test_valid_pipeline_returns_no_errors(self):
        """
        Scenario: All columns exist.
        Expected: Empty error list.
        """
        ds1 = Dataset(id="ds1", source="file", columns=[Column(name="age", type=DataType.INTEGER)])
        ds2 = Dataset(id="ds2", source="derived", columns=[])
        
        op = Operation(
            id="op1", 
            type=OpType.COMPUTE_COLUMNS, 
            inputs=["ds1"], 
            outputs=["ds2"], 
            parameters={"target": "age_months", "expression": "age * 12"}
        )
        
        pipeline = Pipeline(datasets=[ds1, ds2], operations=[op])
        validator = SecurityValidator(pipeline)
        assert len(validator.run()) == 0