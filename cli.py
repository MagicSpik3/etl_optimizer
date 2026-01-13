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
    # Separate the visual output from the data output
    parser.add_argument("--visualize", "-v", type=str, help="Output path for Mermaid .md file", default=None)
    parser.add_argument("--dump-yaml", "-y", type=str, help="Output path for Optimized IR (YAML)", default=None)
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
    
    # 🩹 Optional Patch
    if args.patch_islands:
        print("🩹 Applying 'Island Patch' for implicit SPSS joins...")
        join_op_1 = next((op for op in pipeline.operations if op.id == "op_029_join"), None)
        if join_op_1 and "file_control_values.sav" not in join_op_1.inputs:
            join_op_1.inputs.append("file_control_values.sav")
            
        join_op_2 = next((op for op in pipeline.operations if op.id == "op_053_join"), None)
        if join_op_2 and "file_benefit_rates.sav" not in join_op_2.inputs:
            join_op_2.inputs.append("file_benefit_rates.sav")

    # 2. Optimize
    print("🧠 Running Semantic Promotion...")
    promoter = SemanticPromoter(pipeline)
    pipeline = promoter.run()
    
    print("📉 Running Vertical Collapse...")
    collapser = VerticalCollapser(pipeline)
    pipeline = collapser.run()
    
    final_count = len(pipeline.operations)
    reduction = ((initial_count - final_count) / initial_count) * 100
    print(f"✅ Optimization Complete: {initial_count} ops -> {final_count} ops (-{reduction:.1f}%)")


# 3. Export Data
    if args.dump_yaml:
        print(f"💾 Saving Optimized IR to {args.dump_yaml}...")
        
        # 🔴 THE BUG: This keeps Enums as Python Objects
        # data_dict = pipeline.model_dump(exclude_none=True) 

        # 🟢 THE FIX: Add mode='json' to convert Enums -> Strings
        data_dict = pipeline.model_dump(mode='json', exclude_none=True)
        
        with open(args.dump_yaml, "w") as f:
            yaml.dump(data_dict, f, sort_keys=False)



    # 4. Export Visualization
    if args.visualize:
        print(f"📊 Saving Visualization to {args.visualize}...")
        exporter = MermaidExporter(pipeline)
        diagram = exporter.generate()
        with open(args.visualize, "w") as f:
            f.write("```mermaid\n")
            f.write(diagram)
            f.write("\n```")

if __name__ == "__main__":
    main()