import pytest
from etl_ir.model import Pipeline, Operation, Dataset
from etl_ir.types import OpType
from src.optimizer.collapser import VerticalCollapser

class TestCollapserGC:
    
    def test_removes_orphaned_intermediate_datasets(self):
        """
        Scenario: A -> B -> C.
        Collapse merges Op1 and Op2 into 'A -> Batch -> C'.
        Dataset 'B' is now an orphan (no inputs, no outputs).
        GC should delete 'B'.
        """
        ds_a = Dataset(id="ds_a", source="file")
        ds_b = Dataset(id="ds_b", source="derived") # The victim
        ds_c = Dataset(id="ds_c", source="derived")
        
        ops = [
            # Op1: A -> B
            Operation(id="op1", type=OpType.COMPUTE_COLUMNS, inputs=["ds_a"], outputs=["ds_b"]),
            # Op2: B -> C
            Operation(id="op2", type=OpType.COMPUTE_COLUMNS, inputs=["ds_b"], outputs=["ds_c"])
        ]
        
        pipeline = Pipeline(datasets=[ds_a, ds_b, ds_c], operations=ops)
        
        # Run Collapser
        collapser = VerticalCollapser(pipeline)
        result = collapser.run()
        
        # Verify Logic
        assert len(result.operations) == 1
        assert result.operations[0].type == OpType.BATCH_COMPUTE
        
        # Verify GC
        dataset_ids = [ds.id for ds in result.datasets]
        assert "ds_a" in dataset_ids
        assert "ds_c" in dataset_ids
        assert "ds_b" not in dataset_ids, "GC failed: ds_b should have been deleted"

    def test_keeps_disconnected_but_active_datasets(self):
        """
        Scenario: Pipeline has two separate islands.
        A -> B
        X -> Y
        GC should NOT delete X or Y just because they aren't connected to A.
        """
        ds_a = Dataset(id="ds_a", source="file")
        ds_b = Dataset(id="ds_b", source="derived")
        ds_x = Dataset(id="ds_x", source="file")
        ds_y = Dataset(id="ds_y", source="derived")
        
        ops = [
            Operation(id="op1", type=OpType.COMPUTE_COLUMNS, inputs=["ds_a"], outputs=["ds_b"]),
            Operation(id="op2", type=OpType.COMPUTE_COLUMNS, inputs=["ds_x"], outputs=["ds_y"])
        ]
        
        pipeline = Pipeline(datasets=[ds_a, ds_b, ds_x, ds_y], operations=ops)
        collapser = VerticalCollapser(pipeline)
        result = collapser.run()
        
        # Both ops might stay (if distinct) or batch (if logic allows, but here they are distinct chains)
        # The key is that datasets must persist.
        dataset_ids = [ds.id for ds in result.datasets]
        assert "ds_x" in dataset_ids
        assert "ds_y" in dataset_ids