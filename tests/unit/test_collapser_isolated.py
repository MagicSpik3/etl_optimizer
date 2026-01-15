import pytest
from etl_ir.model import Pipeline, Operation, Dataset
from etl_ir.types import OpType

# Import your actual class
from etl_optimizer.collapser import VerticalCollapser

def test_vertical_collapse_logic():
    """
    Verifies that A -> B -> C (all COMPUTE) are merged into a single BATCH operation.
    """
    # 1. Setup: Create a chain of 3 connected computations
    # Op1: ds_1 -> ds_2 (x = 1)
    op1 = Operation(
        id="op1", type=OpType.COMPUTE_COLUMNS,
        inputs=["ds_1"], outputs=["ds_2"],
        parameters={"target": "x", "expression": "1"}
    )
    # Op2: ds_2 -> ds_3 (y = x + 1)
    op2 = Operation(
        id="op2", type=OpType.COMPUTE_COLUMNS,
        inputs=["ds_2"], outputs=["ds_3"],
        parameters={"target": "y", "expression": "x + 1"}
    )
    # Op3: ds_3 -> ds_4 (z = y * 2)
    op3 = Operation(
        id="op3", type=OpType.COMPUTE_COLUMNS,
        inputs=["ds_3"], outputs=["ds_4"],
        parameters={"target": "z", "expression": "y * 2"}
    )

    # Dummy Datasets (needed for GC check)
    datasets = [
        Dataset(id="ds_1", source="file", columns=[]),
        Dataset(id="ds_2", source="derived", columns=[]), # Should be GC'd
        Dataset(id="ds_3", source="derived", columns=[]), # Should be GC'd
        Dataset(id="ds_4", source="derived", columns=[])
    ]

    pipeline = Pipeline(
        metadata={"id": "test_pipeline"},
        datasets=datasets,
        operations=[op1, op2, op3]
    )

    # 2. Execute
    collapser = VerticalCollapser(pipeline)
    new_pipeline = collapser.run()  # Calling the correct method!

    # 3. Verify Operations
    # Should be collapsed into exactly 1 operation
    assert len(new_pipeline.operations) == 1, "Should have collapsed 3 ops into 1"
    
    batch_op = new_pipeline.operations[0]
    assert batch_op.type == OpType.BATCH_COMPUTE
    assert batch_op.id == "batch_op1"
    
    # Check that parameters were preserved in order
    computes = batch_op.parameters["computes"]
    assert len(computes) == 3
    assert computes[0]["target"] == "x"
    assert computes[2]["target"] == "z"

    # 4. Verify Garbage Collection (GC)
    # ds_2 and ds_3 are now internal to the batch and should be removed.
    remaining_ids = {ds.id for ds in new_pipeline.datasets}
    assert "ds_1" in remaining_ids
    assert "ds_4" in remaining_ids
    assert "ds_2" not in remaining_ids, "ds_2 should have been garbage collected"
    assert "ds_3" not in remaining_ids, "ds_3 should have been garbage collected"
    
    print("\n✅ Vertical Collapser Logic: Verified")