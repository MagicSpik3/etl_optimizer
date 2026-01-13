from src.ir.model import Pipeline
from src.ir.types import OpType

class MermaidExporter:
    """
    Visualizes the Optimized Pipeline.
    Highlights BATCH nodes to show compression.
    """
    
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline

    def generate(self) -> str:
        lines = ["graph TD"]
        
        # Styles
        lines.append("    classDef dataset fill:#e1f5fe,stroke:#01579b,stroke-width:2px,rx:5,ry:5;")
        lines.append("    classDef op fill:#fff9c4,stroke:#fbc02d,stroke-width:2px;")
        lines.append("    classDef batch fill:#c8e6c9,stroke:#2e7d32,stroke-width:4px;") # Thick green border for batches
        lines.append("    classDef barrier fill:#ffccbc,stroke:#d84315,stroke-width:2px;") # Red for Joins/Aggs

        # Nodes
        for ds in self.pipeline.datasets:
            lines.append(f'    {ds.id}[("{ds.id}")]:::dataset')

        for op in self.pipeline.operations:
            style = "op"
            label = f"{op.type.name}<br/>{op.id}"
            
            if op.type == OpType.BATCH_COMPUTE:
                style = "batch"
                count = len(op.parameters.get('computes', []))
                label = f"BATCH COMPUTE<br/>(Merged {count} steps)"
            elif op.type in [OpType.JOIN, OpType.AGGREGATE, OpType.SAVE_BINARY]:
                style = "barrier"
            
            lines.append(f'    {op.id}["{label}"]:::{style}')
            
            # Edges
            for inp in op.inputs:
                lines.append(f"    {inp} --> {op.id}")
            for out in op.outputs:
                lines.append(f"    {op.id} --> {out}")

        return "\n".join(lines)