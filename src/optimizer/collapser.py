from typing import List, Set
from etl_ir.model import Pipeline, Operation, Dataset
from etl_ir.types import OpType

class VerticalCollapser:
    """
    Optimization Pass: 
    1. Merges consecutive COMPUTE operations *if they are connected*.
    2. Performs Garbage Collection on orphaned datasets.
    """
    
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline
        self.buffer: List[Operation] = []
        self.new_ops: List[Operation] = []

    def run(self) -> Pipeline:
        self.new_ops = []
        self.buffer = []

        for op in self.pipeline.operations:
            # 1. Check basic type
            if op.type == OpType.COMPUTE_COLUMNS:
                # 2. Check Lineage Continuity
                if self._is_connected_to_buffer(op):
                    self.buffer.append(op)
                else:
                    # Not connected (different branch) or buffer empty
                    self._flush_buffer()
                    self.buffer.append(op)
            else:
                self._flush_buffer()
                self.new_ops.append(op)
        
        self._flush_buffer() 
        
        clean_datasets = self._gc_datasets(self.new_ops, self.pipeline.datasets)

        return Pipeline(
            metadata=self.pipeline.metadata,
            datasets=clean_datasets,
            operations=self.new_ops
        )

    def _is_connected_to_buffer(self, current_op: Operation) -> bool:
        """
        Returns True if the current operation consumes the output of the 
        last operation in the buffer.
        """
        if not self.buffer:
            return True # Start of a new batch
            
        last_op = self.buffer[-1]
        
        # Intersection Check:
        # Does any input of current_op match any output of last_op?
        # (Using sets for efficient lookup)
        last_outputs = set(last_op.outputs)
        current_inputs = set(current_op.inputs)
        
        return not last_outputs.isdisjoint(current_inputs)

    def _flush_buffer(self):
        if not self.buffer:
            return

        if len(self.buffer) == 1:
            self.new_ops.append(self.buffer[0])
        else:
            self.new_ops.append(self._create_batch_op())
        
        self.buffer.clear()

    def _create_batch_op(self) -> Operation:
        first_op = self.buffer[0]
        last_op = self.buffer[-1]
        
        return Operation(
            id=f"batch_{first_op.id}",
            type=OpType.BATCH_COMPUTE,
            inputs=first_op.inputs,
            outputs=last_op.outputs,
            parameters={
                'computes': [op.parameters for op in self.buffer]
            }
        )
    
    def _gc_datasets(self, ops: List[Operation], datasets: List[Dataset]) -> List[Dataset]:
        active_ids: Set[str] = set()
        for op in ops:
            active_ids.update(op.inputs)
            active_ids.update(op.outputs)
        return [ds for ds in datasets if ds.id in active_ids]