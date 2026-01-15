from .collapser import VerticalCollapser
from .promoter import SemanticPromoter 
from .validator import SecurityValidator

class OptimizationCoordinator:
    def optimize(self, pipeline):
        """
        Orchestrates the optimization passes.
        """
        # 1. Promote Metadata
        promoter = SemanticPromoter(pipeline)
        pipeline = promoter.run()
        
        # 2. Collapse Vertical Logic
        collapser = VerticalCollapser(pipeline)
        pipeline = collapser.run()  
        
        # 3. Validate Security & Topology
        validator = SecurityValidator(pipeline)
        
        # Check A: Structure (Cycles, Islands)
        topo_errors = validator.validate_topology()
        if topo_errors:
            raise ValueError(f"CRITICAL: Topology Violation: {topo_errors}")

        # Check B: Logic (Ghost Columns)
        # 🟢 FIX: Call .run() instead of .validate()
        logic_errors = validator.run()
        if logic_errors:
            raise ValueError(f"CRITICAL: Validation Failed: {logic_errors}")
        
        return pipeline