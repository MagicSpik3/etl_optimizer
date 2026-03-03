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
        self.pending_condition: str | None = None
        self._doif_stack: List[str] = []

    def run(self) -> Pipeline:
        self.new_ops = []
        self.alias_map = {} 
        self.pending_condition = None
        self._doif_stack = []

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
                    promoted_op = self._apply_pending_condition(promoted_op)
                    self.new_ops.append(promoted_op)
                else:
                    # Dropped! Heal the bridge.
                    # If we drop a node A->B, map B->A.
                    if current_op.inputs and current_op.outputs:
                        source = current_op.inputs[0]
                        target = current_op.outputs[0]
                        self.alias_map[target] = source
            else:
                # If a COMPUTE_COLUMNS op carries 'logic' (from GraphBuilder._handle_recode),
                # promote it into a case_when compute here.
                if current_op.type == OpType.COMPUTE_COLUMNS and 'logic' in (current_op.parameters or {}):
                    promoted = self._promote_recode_compute(current_op)
                    promoted = self._apply_pending_condition(promoted)
                    self.new_ops.append(promoted)
                else:
                    current_op = self._apply_pending_condition(current_op)
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

        # Handle DO IF / END IF blocks: set/clear pending condition
        # Support two shapes: command may be 'DO' with raw_content 'IF (...)',
        # or command may directly include 'DO IF ...'.
        raw = op.parameters.get('raw_content', '') if op.parameters else ''
        if "DO IF" in command or (command == 'DO' and raw.strip().upper().startswith('IF')):
            # Extract condition text preferably from raw_content (safer)
            raw_content = raw.strip()
            cond = ''
            if raw_content.upper().startswith('IF'):
                # raw like 'IF (age < 18)'
                cond = raw_content[2:].strip()
            elif raw_content.startswith('(') and raw_content.endswith(')'):
                cond = raw_content[1:-1].strip()
            else:
                # fallback: if command contains inline text after 'DO IF'
                parts = command.split('DO IF', 1)
                cond = parts[1].strip() if len(parts) > 1 else ''

            cond = cond.rstrip('.')
            self._doif_stack.append(cond)
            self.pending_condition = cond
            return None

        if command.startswith('END IF') or command == 'END' or command == 'END IF':
            # pop stack
            if self._doif_stack:
                self._doif_stack.pop()
            self.pending_condition = self._doif_stack[-1] if self._doif_stack else None
            return None

        # Handle ELSE: invert last DO IF condition
        if command.strip().upper().startswith('ELSE'):
            if self._doif_stack:
                orig = self._doif_stack[-1]
                self.pending_condition = f"!({orig})"
            return None

        # 1. Dead Code Elimination
        if cmd_root in self.NOISE_COMMANDS or command in self.NOISE_COMMANDS:
            return None # Drop and trigger rewiring

        # 2. Promote SORT
        if "SORT" in command:
            return Operation(
                id=op.id, type=OpType.SORT_ROWS, inputs=op.inputs, outputs=op.outputs,
                parameters={"keys": args or op.parameters.get("raw_content", "unknown")}
            )
            
        # 3. Promote FILTER
        if "SELECT IF" in command or "FILTER" in command or "IF" == command:
            return Operation(
                id=op.id, type=OpType.FILTER_ROWS, inputs=op.inputs, outputs=op.outputs,
                parameters={"condition": args or op.parameters.get("raw_content", "unknown")}
            )

        # 4. Promote MISSING VALUES -> compute using na_if
        if "MISSING" in command:
            # raw_content might be like: "age (-9)"
            raw = op.parameters.get('raw_content', '')
            import re
            # Remove leading keyword like 'VALUES' and normalize spacing
            raw_norm = re.sub(r"^\s*VALUES?\b", "", raw, flags=re.I).strip()
            # match var ( -9 ) or var (- 9 ) etc. allow spaces between sign and digits
            m = re.search(r"([A-Za-z_][A-Za-z0-9_]*)\s*\(\s*([+-]?\s*\d+)\s*\)", raw_norm)
            if m:
                var = m.group(1)
                missing_val = m.group(2).replace(" ", "")
                expr = f"{var} = na_if({var}, {missing_val})"
                return Operation(
                    id=op.id, type=OpType.COMPUTE_COLUMNS, inputs=op.inputs, outputs=op.outputs,
                    parameters={'target': var, 'expression': expr}
                )

        # 5. Promote RECODE mappings when present in parameters or command
        if 'RECODE' in command or 'logic' in op.parameters:
            # logic example: "(0 THRU 49 = 0) (50 THRU 100 = 1)"
            logic = op.parameters.get('logic', '')
            # source variable: prefer explicit keys, fall back to a generic 'var'
            src_var = op.parameters.get('source') or op.parameters.get('target') or op.parameters.get('var') or 'var'
            import re
            parts = re.findall(r"\(([^=]+)=\s*([^\)]+)\)", logic)
            if parts:
                cases = []
                for left, right in parts:
                    condition_text = left.strip()
                    out_val = right.strip()
                    m = re.match(r"([0-9]+)\s+THRU\s+([0-9]+)", condition_text)
                    if m:
                        a, b = m.group(1), m.group(2)
                        cases.append(f"between({src_var},{a},{b}) ~ {out_val}")
                    else:
                        cases.append(f"{condition_text} ~ {out_val}")

                case_when_expr = "case_when(" + ", ".join(cases) + ", TRUE ~ " + src_var + ")"
                target = op.parameters.get('target') or src_var
                return Operation(
                    id=op.id, type=OpType.COMPUTE_COLUMNS, inputs=op.inputs, outputs=op.outputs,
                    parameters={'target': target, 'expression': case_when_expr}
                )

        return op

    def _promote_recode_compute(self, op: Operation) -> Operation:
        """Convert an existing COMPUTE_COLUMNS op that carries 'logic' into a case_when compute."""
        logic = op.parameters.get('logic', '')
        src_var = op.parameters.get('source') or op.parameters.get('target') or 'var'
        import re
        parts = re.findall(r"\(([^=]+)=\s*([^\)]+)\)", logic)
        if parts:
            cases = []
            for left, right in parts:
                condition_text = left.strip()
                out_val = right.strip()
                m = re.match(r"([0-9]+)\s+THRU\s+([0-9]+)", condition_text)
                if m:
                    a, b = m.group(1), m.group(2)
                    cases.append(f"between({src_var},{a},{b}) ~ {out_val}")
                else:
                    cases.append(f"{condition_text} ~ {out_val}")

            case_when_expr = "case_when(" + ", ".join(cases) + ", TRUE ~ " + src_var + ")"
            target = op.parameters.get('target') or src_var
            return Operation(
                id=op.id, type=OpType.COMPUTE_COLUMNS, inputs=op.inputs, outputs=op.outputs,
                parameters={'target': target, 'expression': case_when_expr}
            )

        return op

    def _apply_pending_condition(self, op: Operation | None) -> Operation | None:
        """If a DO IF condition is active, wrap compute expressions with if_else()."""
        if op is None or not self.pending_condition:
            return op

        # Handle single compute
        if op.type == OpType.COMPUTE_COLUMNS and 'expression' in (op.parameters or {}):
            expr = op.parameters.get('expression', '')
            target = op.parameters.get('target')
            if '=' in expr:
                lhs, rhs = expr.split('=', 1)
                lhs = lhs.strip()
                rhs = rhs.strip()
                wrapped = f"if_else({self.pending_condition}, {rhs}, {lhs})"
                op.parameters['expression'] = f"{lhs} = {wrapped}"
            else:
                # expression is RHS only (e.g., '0'), use target from parameters
                rhs = expr.strip()
                if not target:
                    return op
                wrapped = f"if_else({self.pending_condition}, {rhs}, {target})"
                op.parameters['expression'] = f"{target} = {wrapped}"
            return op

        # Handle batched computes (list of computes)
        if op.type == OpType.BATCH_COMPUTE and op.parameters:
            computes = op.parameters.get('computes', [])
            new_computes = []
            for comp in computes:
                expr = comp.get('expression', '')
                if '=' in expr:
                    lhs, rhs = expr.split('=', 1)
                    lhs = lhs.strip()
                    rhs = rhs.strip()
                    wrapped = f"if_else({self.pending_condition}, {rhs}, {lhs})"
                    comp['expression'] = f"{lhs} = {wrapped}"
                else:
                    # expression is RHS-only
                    lhs = comp.get('target')
                    rhs = expr.strip()
                    if lhs:
                        wrapped = f"if_else({self.pending_condition}, {rhs}, {lhs})"
                        comp['expression'] = f"{lhs} = {wrapped}"
                new_computes.append(comp)
            op.parameters['computes'] = new_computes
            return op

        return op