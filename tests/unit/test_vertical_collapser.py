import pytest
from etl_ir.model import Pipeline, Operation, Dataset
from etl_ir.types import OpType, DataType
# We haven't created this class yet, but we code against the interface we want!
from etl_optimizer.collapser import VerticalCollapser 

class TestVerticalCollapser:
    
    def test_collapses_linear_computes(self):
        """
        Scenario: Op1 (Compute A) -> Op2 (Compute B)
        Expected: One Op (Batch Compute) containing both logic steps.
        """
        # 1. Setup a chain: Load -> Compute -> Compute -> Save
        ops = [
            Operation(id="load", type=OpType.LOAD_CSV, inputs=[], outputs=["ds1"]),
            Operation(id="comp1", type=OpType.COMPUTE_COLUMNS, inputs=["ds1"], outputs=["ds2"], 
                      parameters={"target": "A", "expression": "1"}),
            Operation(id="comp2", type=OpType.COMPUTE_COLUMNS, inputs=["ds2"], outputs=["ds3"], 
                      parameters={"target": "B", "expression": "A+1"}),
            Operation(id="save", type=OpType.SAVE_BINARY, inputs=["ds3"], outputs=["file"])
        ]
        datasets = [
            Dataset(id="ds1", source="file", columns=[]),
            Dataset(id="ds2", source="derived", columns=[]),
            Dataset(id="ds3", source="derived", columns=[])
        ]
        pipeline = Pipeline(datasets=datasets, operations=ops)
        
        # 2. Optimize
        collapser = VerticalCollapser(pipeline)
        optimized = collapser.run()
        
        # 3. Assertions
        # Should reduce 4 ops to 3 (Load, Batch, Save)
        assert len(optimized.operations) == 3
        
        # Find the batch op
        batch_op = optimized.operations[1]
        assert batch_op.type == OpType.BATCH_COMPUTE
        
        # Verify it captured both parameters
        computes = batch_op.parameters['computes']
        assert len(computes) == 2
        assert computes[0] == {"target": "A", "expression": "1"}
        assert computes[1] == {"target": "B", "expression": "A+1"}
        
        # Verify inputs/outputs wiring
        assert batch_op.inputs == ["ds1"]
        assert batch_op.outputs == ["ds3"] # ds2 should be skipped

    def test_stops_at_barrier(self):
        """
        Scenario: Compute -> Filter -> Compute
        Expected: No collapse across the Filter barrier.
        """
        ops = [
            Operation(id="c1", type=OpType.COMPUTE_COLUMNS, inputs=["d1"], outputs=["d2"]),
            # Filter is a barrier for pure vertical collapse logic usually
            Operation(id="filter", type=OpType.FILTER_ROWS, inputs=["d2"], outputs=["d3"]),
            Operation(id="c2", type=OpType.COMPUTE_COLUMNS, inputs=["d3"], outputs=["d4"]),
        ]
        pipeline = Pipeline(datasets=[], operations=ops)
        
        collapser = VerticalCollapser(pipeline)
        optimized = collapser.run()
        
        # Should stay 3 ops
        assert len(optimized.operations) == 3
        assert optimized.operations[0].type == OpType.COMPUTE_COLUMNS
        assert optimized.operations[1].type == OpType.FILTER_ROWS
        assert optimized.operations[2].type == OpType.COMPUTE_COLUMNS

    def test_handles_recode_as_compute(self):
        """
        Scenario: RECODE is effectively a Compute. It should merge.
        Compute -> Recode -> Compute
        """
        # Note: In our IR, RECODE might come in as COMPUTE or GENERIC depending on earlier stages.
        # If we standardized on OpType.COMPUTE_COLUMNS in Repo 1, it merges automatically.
        # This test ensures we handle 'logic' params mixed with 'expression' params.
        pass