import pytest
from etl_ir.model import Pipeline, Operation, Dataset
from etl_ir.types import OpType
from src.optimizer.promoter import SemanticPromoter

class TestSemanticPromoter:
    
    def test_promotes_sort_command(self):
        """
        Scenario: A Generic Node with command 'SORT' should become OpType.SORT.
        """
        ops = [
            Operation(
                id="op_sort_raw", 
                type=OpType.GENERIC_TRANSFORM, 
                inputs=["ds1"], 
                outputs=["ds2"], 
                parameters={"command": "SORT", "args": "BY age"} 
            )
        ]
        pipeline = Pipeline(datasets=[], operations=ops)
        
        promoter = SemanticPromoter(pipeline)
        result = promoter.run()
        
        op = result.operations[0]
        assert op.type == OpType.SORT  # New Semantic Type!
        assert op.parameters["keys"] == "BY age"

    def test_promotes_filter_command(self):
        """
        Scenario: A Generic Node with command 'SELECT IF' or 'FILTER' 
        should become OpType.FILTER_ROWS.
        """
        ops = [
            Operation(
                id="op_filter_raw", 
                type=OpType.GENERIC_TRANSFORM, 
                inputs=["ds1"], 
                outputs=["ds2"], 
                parameters={"command": "SELECT IF", "args": "age > 18"}
            )
        ]
        pipeline = Pipeline(datasets=[], operations=ops)
        
        promoter = SemanticPromoter(pipeline)
        result = promoter.run()
        
        op = result.operations[0]
        assert op.type == OpType.FILTER_ROWS
        assert op.parameters["condition"] == "age > 18"

    def test_ignores_unknown_generics(self):
        """
        Scenario: 'DISPLAY' or 'FREQUENCIES' are not structural.
        They should remain Generic or be dropped (depending on policy).
        For now, we keep them as Generic.
        """
        ops = [
            Operation(
                id="op_display", 
                type=OpType.GENERIC_TRANSFORM, 
                inputs=[], 
                outputs=[], 
                parameters={"command": "DISPLAY"}
            )
        ]
        pipeline = Pipeline(datasets=[], operations=ops)
        
        promoter = SemanticPromoter(pipeline)
        result = promoter.run()
        
        assert result.operations[0].type == OpType.GENERIC_TRANSFORM