from typing import List, Dict, Any
from etl_ir.model import Pipeline, Operation
from etl_ir.types import OpType

class SemanticPromoter:
    """
    Optimization Pass: 
    1. Promotes 'Generic' nodes to Semantic Nodes.
    2. Performs Dead Code Elimination (DCE) on Syntax Noise.
    3. REWIRES the graph to heal broken links caused by DCE.
    """
    
    NOISE_COMMANDS = {
        "DO", "END", "FORMATS", "LIST", "STRING", "EXECUTE"
    }

    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline
        self.new_ops: List[Operation] = []
        self.alias_map: Dict[str, str] = {} # Maps deleted_ds -> source_ds

    def run(self) -> Pipeline:
        self.new_ops = []
        self.alias_map = {} 

        for op in self.pipeline.operations:
            # 1. Resolve Inputs (Rewiring)
            # If a previous node was deleted, its output is now an alias for its input.
            # We points the current op to the original source.
            resolved_inputs = [self.alias_map.get(inp, inp) for inp in op.inputs]
            
            # Create a temporary copy with resolved inputs to check/promote
            current_op = Operation(
                id=op.id,
                type=op.type,
                inputs=resolved_inputs,
                outputs=op.outputs,
                parameters=op.parameters
            )

            if current_op.type == OpType.GENERIC_TRANSFORM:
                promoted_op = self._promote_or_drop(current_op)
                
                if promoted_op:
                    self.new_ops.append(promoted_op)
                else:
                    # Dropped! Heal the bridge.
                    # If we drop a node A->B, map B->A.
                    if current_op.inputs and current_op.outputs:
                        source = current_op.inputs[0]
                        target = current_op.outputs[0]
                        self.alias_map[target] = source
            else:
                self.new_ops.append(current_op)
        
        return Pipeline(
            metadata=self.pipeline.metadata,
            datasets=self.pipeline.datasets,
            operations=self.new_ops
        )

    def _promote_or_drop(self, op: Operation) -> Operation | None:
        command = op.parameters.get("command", "").upper().strip()
        args = op.parameters.get("args", "")
        cmd_root = command.split()[0] if command else ""

        # 1. Dead Code Elimination
        if cmd_root in self.NOISE_COMMANDS or command in self.NOISE_COMMANDS:
            return None # Drop and trigger rewiring

        # 2. Promote SORT
        if "SORT" in command:
            return Operation(
                id=op.id, type=OpType.SORT, inputs=op.inputs, outputs=op.outputs,
                parameters={"keys": args or op.parameters.get("raw_content", "unknown")}
            )
            
        # 3. Promote FILTER
        if "SELECT IF" in command or "FILTER" in command or "IF" == command:
            return Operation(
                id=op.id, type=OpType.FILTER_ROWS, inputs=op.inputs, outputs=op.outputs,
                parameters={"condition": args or op.parameters.get("raw_content", "unknown")}
            )

        return op