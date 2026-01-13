import argparse
import yaml
import sys
from pathlib import Path

# Ensure src is in python path
sys.path.append('src')

from src.ir.model import Pipeline
from src.optimizer.promoter import SemanticPromoter
from src.optimizer.collapser import VerticalCollapser
from src.optimizer.validator import SecurityValidator
from src.exporters.mermaid import MermaidExporter

def main():
    parser = argparse.ArgumentParser(description="ETL Optimizer & Visualizer")
    parser.add_argument("input_file", type=str, help="Path to raw SpecGen YAML")
    parser.add_argument("--output", "-o", type=str, help="Output path for Mermaid .md file", default=None)
    parser.add_argument("--patch-islands", action="store_true", help="Apply fix for disconnected SPSS joins (Demo Only)")
    
    args = parser.parse_args()
    
    # 1. Load
    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"❌ Error: File not found: {input_path}")
        sys.exit(1)
        
    print(f"🔄 Loading {input_path}...")
    with open(input_path, "r") as f:
        data = yaml.safe_load(f)
    
    pipeline = Pipeline(**data)
    initial_count = len(pipeline.operations)
    
    # 🩹 Optional Patch for the SpecGen "Island" bug
    if args.patch_islands:
        print("🩹 Applying 'Island Patch' for implicit SPSS joins...")
        # Link Control Values
        join_op_1 = next((op for op in pipeline.operations if op.id == "op_029_join"), None)
        if join_op_1 and "file_control_values.sav" not in join_op_1.inputs:
            join_op_1.inputs.append("file_control_values.sav")
            
        # Link Benefit Rates
        join_op_2 = next((op for op in pipeline.operations if op.id == "op_053_join"), None)
        if join_op_2 and "file_benefit_rates.sav" not in join_op_2.inputs:
            join_op_2.inputs.append("file_benefit_rates.sav")

    # 2. Semantic Promotion (DCE + Rewiring)
    print("🧠 Running Semantic Promotion (DCE & Rewiring)...")
    promoter = SemanticPromoter(pipeline)
    pipeline = promoter.run()
    
    # 3. Vertical Collapse (Merging + GC)
    print("📉 Running Vertical Collapse (Merging & GC)...")
    collapser = VerticalCollapser(pipeline)
    pipeline = collapser.run()
    
    final_count = len(pipeline.operations)
    reduction = ((initial_count - final_count) / initial_count) * 100
    
    print(f"✅ Optimization Complete: {initial_count} ops -> {final_count} ops (-{reduction:.1f}%)")
    
    # 4. Validation
    validator = SecurityValidator(pipeline)
    topo_errors = validator.validate_topology()
    
    if topo_errors:
        print("\n⚠️  Topology Warnings:")
        for err in topo_errors:
            print(f"  - {err}")
    else:
        print("\n✨ Topology: Valid (Fully Connected)")

    # 5. Export
    exporter = MermaidExporter(pipeline)
    diagram = exporter.generate()
    
    if args.output:
        with open(args.output, "w") as f:
            f.write("```mermaid\n")
            f.write(diagram)
            f.write("\n```")
        print(f"\n📄 Diagram saved to: {args.output}")
    else:
        print("\n--- Mermaid Diagram ---")
        print(diagram)
        print("-----------------------")

if __name__ == "__main__":
    main()