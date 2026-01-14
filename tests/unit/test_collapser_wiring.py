import pytest
from etl_ir.model import Pipeline, Operation, Dataset
from etl_ir.types import OpType
from src.optimizer.collapser import VerticalCollapser

class TestCollapserWiring:
    
    def test_wiring_preserves_lineage(self):
        """
        Scenario: 
        Step 1: A -> B
        Step 2: B -> C
        Step 3: C -> D (Barrier/Save)
        
        Optimization: Merge 1 & 2.
        Result should be: A -> Batch(B,C) -> D.
        The Batch output must be 'C' (the last output), not 'B' or 'D'.
        """
        ops = [
            Operation(id="op1", type=OpType.COMPUTE_COLUMNS, inputs=["A"], outputs=["B"]),
            Operation(id="op2", type=OpType.COMPUTE_COLUMNS, inputs=["B"], outputs=["C"]),
            Operation(id="save", type=OpType.SAVE_BINARY, inputs=["C"], outputs=["D"])
        ]
        
        pipeline = Pipeline(datasets=[], operations=ops) # Datasets irrelevant for wiring check
        collapser = VerticalCollapser(pipeline)
        result = collapser.run()
        
        # We expect 2 ops: Batch -> Save
        assert len(result.operations) == 2
        
        batch_op = result.operations[0]
        save_op = result.operations[1]
        
        # Check Batch Wiring
        assert batch_op.type == OpType.BATCH_COMPUTE
        assert batch_op.inputs == ["A"]  # From Op1
        assert batch_op.outputs == ["C"] # From Op2
        
        # Check Save Wiring (Should be unchanged)
        assert save_op.inputs == ["C"]
        assert save_op.outputs == ["D"]