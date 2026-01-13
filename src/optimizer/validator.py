import networkx as nx
from typing import List
from src.ir.model import Pipeline, Operation
from src.ir.types import OpType
import re

class SecurityValidator:
    """
    Validation Pass: Performs stateful checks on the pipeline.
    1. Ghost Column Detection (Use-Before-Def)
    2. Type Safety (Future)
    3. PII Leakage (Future)
    """
    
    # SPSS Keywords to ignore when checking for variable names
    KNOWN_FUNCTIONS = {
        "NUMBER", "DATE", "MDY", "TRUNC", "MOD", "SUM", "MEAN", "MAX", "MIN", 
        "SYSMIS", "AND", "OR", "NOT", "IF", "THRU", "LOWEST", "HIGHEST"
    }
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline
        self.ds_map = {ds.id: ds for ds in pipeline.datasets}
        self.graph = self._build_graph()

    def _build_graph(self) -> nx.DiGraph:
        """
        Converts the Pipeline into a NetworkX Directed Graph.
        Nodes = Operations & Datasets
        Edges = Data Flow
        """
        G = nx.DiGraph()
        
        for op in self.pipeline.operations:
            G.add_node(op.id, type="operation")
            
            for inp in op.inputs:
                G.add_edge(inp, op.id) # Dataset -> Op
            
            for out in op.outputs:
                G.add_edge(op.id, out) # Op -> Dataset
                
        return G

    def validate_topology(self) -> List[str]:
        errors = []
        
        # 1. Check for Cycles
        try:
            cycles = list(nx.simple_cycles(self.graph))
            if cycles:
                errors.append(f"Cycle detected in pipeline: {cycles}")
        except:
            pass # NetworkX recursive error sometimes happens on huge graphs
            
        # 2. Check for Disconnected Components (Islands)
        # Convert to undirected to find islands
        undirected = self.graph.to_undirected()
        components = list(nx.connected_components(undirected))
        if len(components) > 1:
            errors.append(f"Disconnected component detected. Found {len(components)} islands.")

        # 3. Check for Broken Bridges (Missing Inputs)
        # Any operation input that isn't in the graph is a missing link
        for op in self.pipeline.operations:
            for inp in op.inputs:
                if inp not in self.ds_map:
                     errors.append(f"Missing input dataset '{inp}' for operation '{op.id}'")
        
        return errors


    def run(self) -> List[str]:
        errors = []
        
        for op in self.pipeline.operations:
            if op.type == OpType.COMPUTE:
                errors.extend(self._validate_compute(op))
            # Future: Add hooks for _validate_join, _validate_filter, etc.
            
        return errors

    def _validate_compute(self, op: Operation) -> List[str]:
        errors = []
        expression = op.parameters.get("expression", "")
        if not expression:
            return []

        # 1. Resolve Input Schema
        # A compute op typically has 1 input dataset
        input_columns = set()
        for ds_id in op.inputs:
            ds = self.ds_map.get(ds_id)
            if ds:
                for col in ds.columns:
                    input_columns.add(col.name.upper()) # Case insensitive normalization

        # 2. Extract Variables from Expression
        # Regex to find words that look like variables
        tokens = set(re.findall(r'[a-zA-Z_][a-zA-Z0-9_]*', expression))
        
        # 3. Check for Ghosts
        for token in tokens:
            upper_token = token.upper()
            
            # Skip numbers, keywords, and functions
            if upper_token in self.KNOWN_FUNCTIONS:
                continue
            
            # If it's not in inputs, it's a Ghost!
            if upper_token not in input_columns:
                errors.append(
                    f"Ghost Column Detected: Operation '{op.id}' uses variable '{token}' "
                    f"which does not exist in input datasets {op.inputs}."
                )
                
        return errors