import pytest
from etl_ir.model import Pipeline, Operation, Dataset
from etl_optimizer.coordinator import OptimizationCoordinator

def test_coordinator_runs_full_chain():
    """
    Verifies that the Coordinator successfully initializes and runs
    all three sub-components (Promoter -> Collapser -> Validator).
    """
    # 1. Setup Dummy Pipeline
    pipeline = Pipeline(
        metadata={"id": "test_coord"},
        datasets=[Dataset(id="ds1", source="file")],
        operations=[
            Operation(id="op1", type="compute_columns", inputs=["ds1"], outputs=["ds2"], parameters={"target": "x", "expression": "1"})
        ]
    )

    # 2. Run Coordinator
    coord = OptimizationCoordinator()
    result_pipeline = coord.optimize(pipeline)

    # 3. Verify Result
    assert result_pipeline is not None
    assert result_pipeline.metadata["id"] == "test_coord"
    
    # If the coordinator crashed on .collapse() vs .run(), this test would fail.
    print("\n✅ Coordinator Wiring: Verified")