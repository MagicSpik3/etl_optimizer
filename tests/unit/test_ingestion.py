import pytest
from pydantic import ValidationError
import yaml

# We assume these models will exist in src.ir.model
from src.ir.model import Pipeline, Operation, Dataset
from src.ir.types import OpType, DataType

class TestIngestion:
    
    def test_load_complex_pipeline_fixture(self):
        """
        Happy Path: Can we load the 60-step monster fixture?
        """
        with open("tests/fixtures/raw_trace.yaml", "r") as f:
            data = yaml.safe_load(f)
        
        pipeline = Pipeline(**data)
        
        # Basic sanity checks on the fixture
        assert len(pipeline.operations) >= 60
        assert len(pipeline.datasets) >= 50
        
        # Check a specific deep node (Forensic check)
        op_58 = next(op for op in pipeline.operations if op.id == "op_058_aggregate")
        assert op_58.type == OpType.AGGREGATE
        assert op_58.parameters["break"] == ["benefit_type", "region"]

    def test_fail_on_invalid_op_type(self):
        """
        Edge Case: The YAML contains an operation type we don't understand.
        """
        yaml_str = """
        metadata: {generator: test, source_type: SPSS}
        datasets: []
        operations:
        - id: op_bad
          type: magic_wand_transform  # <--- Invalid Enum
          inputs: []
          outputs: []
          parameters: {}
        """
        data = yaml.safe_load(yaml_str)
        
        with pytest.raises(ValidationError) as excinfo:
            Pipeline(**data)
        
        assert "magic_wand_transform" in str(excinfo.value)

    def test_fail_on_invalid_data_type(self):
        """
        Edge Case: A column definition has a nonsense data type.
        """
        yaml_str = """
        metadata: {generator: test, source_type: SPSS}
        datasets:
        - id: ds_bad
          source: derived
          columns:
          - name: bad_col
            type: quantum_state  # <--- Invalid Enum
        operations: []
        """
        data = yaml.safe_load(yaml_str)
        
        with pytest.raises(ValidationError):
            Pipeline(**data)

    def test_graceful_missing_parameters(self):
        """
        Edge Case: 'parameters' key is missing entirely.
        Should default to empty dict, NOT fail.
        """
        yaml_str = """
        metadata: {generator: test, source_type: SPSS}
        datasets: []
        operations:
        - id: op_minimal
          type: materialize
          inputs: []
          outputs: []
          # parameters is missing
        """
        data = yaml.safe_load(yaml_str)
        pipeline = Pipeline(**data)
        
        op = pipeline.operations[0]
        assert op.parameters == {} # Must be dict, not None

    def test_strict_list_validation(self):
        """
        Edge Case: 'inputs' is a string, not a list.
        Pydantic might try to coerce, but we want to be sure it handles it or fails predictably.
        """
        yaml_str = """
        metadata: {generator: test, source_type: SPSS}
        datasets: []
        operations:
        - id: op_bad_list
          type: compute_columns
          inputs: ds_001  # <--- String, should be list [ds_001]
          outputs: []
          parameters: {}
        """
        data = yaml.safe_load(yaml_str)
        
        # Depending on Pydantic config, this might fail or coerce.
        # For a compiler, we usually prefer to Fail to catch generator bugs.
        # But if we allow coercion, we assert it becomes a list.
        try:
            pipeline = Pipeline(**data)
            assert isinstance(pipeline.operations[0].inputs, list)
            assert pipeline.operations[0].inputs[0] == "ds_001"
        except ValidationError:
            pass # Failing is also an acceptable outcome for strict mode