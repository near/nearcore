use super::{Error, ModuleCtx};
use parity_wasm::elements::{BlockType, Type};

/// Control stack frame.
#[derive(Debug)]
struct Frame {
    /// Stack becomes polymorphic only after an instruction that
    /// never passes control further was executed.
    is_polymorphic: bool,

    /// Count of values which will be pushed after the exit
    /// from the current block.
    end_arity: u32,

    /// Count of values which should be poped upon a branch to
    /// this frame.
    ///
    /// This might be diffirent from `end_arity` since branch
    /// to the loop header can't take any values.
    branch_arity: u32,

    /// Stack height before entering in the block.
    start_height: u32,
}

/// This is a compound stack that abstracts tracking height of the value stack
/// and manipulation of the control stack.
struct Stack {
    height: u32,
    control_stack: Vec<Frame>,
}

impl Stack {
    fn new() -> Stack {
        Stack { height: 0, control_stack: Vec::new() }
    }

    /// Returns current height of the value stack.
    fn height(&self) -> u32 {
        self.height
    }

    /// Returns a reference to a frame by specified depth relative to the top of
    /// control stack.
    fn frame(&self, rel_depth: u32) -> Result<&Frame, Error> {
        let control_stack_height: usize = self.control_stack.len();
        let last_idx = control_stack_height
            .checked_sub(1)
            .ok_or_else(|| Error("control stack is empty".into()))?;
        let idx = last_idx
            .checked_sub(rel_depth as usize)
            .ok_or_else(|| Error("control stack out-of-bounds".into()))?;
        Ok(&self.control_stack[idx])
    }

    /// Mark successive instructions as unreachable.
    ///
    /// This effectively makes stack polymorphic.
    fn mark_unreachable(&mut self) -> Result<(), Error> {
        let top_frame =
            self.control_stack.last_mut().ok_or_else(|| Error("stack must be non-empty".into()))?;
        top_frame.is_polymorphic = true;
        Ok(())
    }

    /// Push control frame into the control stack.
    fn push_frame(&mut self, frame: Frame) {
        self.control_stack.push(frame);
    }

    /// Pop control frame from the control stack.
    ///
    /// Returns `Err` if the control stack is empty.
    fn pop_frame(&mut self) -> Result<Frame, Error> {
        self.control_stack.pop().ok_or_else(|| Error("stack must be non-empty".into()))
    }

    /// Truncate the height of value stack to the specified height.
    fn trunc(&mut self, new_height: u32) {
        self.height = new_height;
    }

    /// Push specified number of values into the value stack.
    ///
    /// Returns `Err` if the height overflow usize value.
    fn push_values(&mut self, value_count: u32) -> Result<(), Error> {
        self.height =
            self.height.checked_add(value_count).ok_or_else(|| Error("stack overflow".into()))?;
        Ok(())
    }

    /// Pop specified number of values from the value stack.
    ///
    /// Returns `Err` if the stack happen to be negative value after
    /// values popped.
    fn pop_values(&mut self, value_count: u32) -> Result<(), Error> {
        if value_count == 0 {
            return Ok(());
        }
        {
            let top_frame = self.frame(0)?;
            if self.height == top_frame.start_height {
                // It is an error to pop more values than was pushed in the current frame
                // (ie pop values pushed in the parent frame), unless the frame became
                // polymorphic.
                return if top_frame.is_polymorphic {
                    Ok(())
                } else {
                    Err(Error("trying to pop more values than pushed".into()))
                };
            }
        }

        self.height =
            self.height.checked_sub(value_count).ok_or_else(|| Error("stack underflow".into()))?;

        Ok(())
    }
}

/// This function expects the function to be validated.
pub(crate) fn compute(func_idx: u32, module_ctx: &ModuleCtx<'_>) -> Result<u32, Error> {
    use parity_wasm::elements::Instruction::*;

    let module = module_ctx.module;

    let func_section =
        module.function_section().ok_or_else(|| Error("No function section".into()))?;
    let code_section = module.code_section().ok_or_else(|| Error("No code section".into()))?;
    let type_section = module.type_section().ok_or_else(|| Error("No type section".into()))?;

    // Get a signature and a body of the specified function.
    let func_sig_idx = func_section
        .entries()
        .get(func_idx as usize)
        .ok_or_else(|| Error("Function is not found in func section".into()))?
        .type_ref();
    let Type::Function(func_signature) = type_section
        .types()
        .get(func_sig_idx as usize)
        .ok_or_else(|| Error("Function is not found in func section".into()))?;
    let body = code_section
        .bodies()
        .get(func_idx as usize)
        .ok_or_else(|| Error("Function body for the index isn't found".into()))?;
    let instructions = body.code();

    let mut stack = Stack::new();
    let mut max_height: u32 = 0;
    let mut pc = 0;

    // Add implicit frame for the function. Breaks to this frame and execution of
    // the last end should deal with this frame.
    let func_arity = func_signature.results().len() as u32;
    stack.push_frame(Frame {
        is_polymorphic: false,
        end_arity: func_arity,
        branch_arity: func_arity,
        start_height: 0,
    });

    loop {
        if pc >= instructions.elements().len() {
            break;
        }

        // If current value stack is higher than maximal height observed so far,
        // save the new height.
        // However, we don't increase maximal value in unreachable code.
        if stack.height() > max_height && !stack.frame(0)?.is_polymorphic {
            max_height = stack.height();
        }

        let opcode = &instructions.elements()[pc];

        match opcode {
            Nop => {}
            Block(ty) | Loop(ty) | If(ty) => {
                let end_arity = if *ty == BlockType::NoResult { 0 } else { 1 };
                let branch_arity = if let Loop(_) = *opcode { 0 } else { end_arity };
                if let If(_) = *opcode {
                    stack.pop_values(1)?;
                }
                let height = stack.height();
                stack.push_frame(Frame {
                    is_polymorphic: false,
                    end_arity,
                    branch_arity,
                    start_height: height,
                });
            }
            Else => {
                // The frame at the top should be pushed by `If`. So we leave
                // it as is.
            }
            End => {
                let frame = stack.pop_frame()?;
                stack.trunc(frame.start_height);
                stack.push_values(frame.end_arity)?;
            }
            Unreachable => {
                stack.mark_unreachable()?;
            }
            Br(target) => {
                // Pop values for the destination block result.
                let target_arity = stack.frame(*target)?.branch_arity;
                stack.pop_values(target_arity)?;

                // This instruction unconditionally transfers control to the specified block,
                // thus all instruction until the end of the current block is deemed unreachable
                stack.mark_unreachable()?;
            }
            BrIf(target) => {
                // Pop values for the destination block result.
                let target_arity = stack.frame(*target)?.branch_arity;
                stack.pop_values(target_arity)?;

                // Pop condition value.
                stack.pop_values(1)?;

                // Push values back.
                stack.push_values(target_arity)?;
            }
            BrTable(br_table_data) => {
                let arity_of_default = stack.frame(br_table_data.default)?.branch_arity;

                // Check that all jump targets have an equal arities.
                for target in &*br_table_data.table {
                    let arity = stack.frame(*target)?.branch_arity;
                    if arity != arity_of_default {
                        return Err(Error("Arity of all jump-targets must be equal".into()));
                    }
                }

                // Because all jump targets have an equal arities, we can just take arity of
                // the default branch.
                stack.pop_values(arity_of_default)?;

                // This instruction doesn't let control flow to go further, since the control flow
                // should take either one of branches depending on the value or the default branch.
                stack.mark_unreachable()?;
            }
            Return => {
                // Pop return values of the function. Mark successive instructions as unreachable
                // since this instruction doesn't let control flow to go further.
                stack.pop_values(func_arity)?;
                stack.mark_unreachable()?;
            }
            Call(idx) => {
                let ty = module_ctx.resolve_func_type(*idx)?;

                // Pop values for arguments of the function.
                stack.pop_values(ty.params().len() as u32)?;

                // Push result of the function execution to the stack.
                let callee_arity = ty.results().len() as u32;
                stack.push_values(callee_arity)?;
            }
            CallIndirect(x, _) => {
                let Type::Function(ty) = type_section
                    .types()
                    .get(*x as usize)
                    .ok_or_else(|| Error("Type not found".into()))?;

                // Pop the offset into the function table.
                stack.pop_values(1)?;

                // Pop values for arguments of the function.
                stack.pop_values(ty.params().len() as u32)?;

                // Push result of the function execution to the stack.
                let callee_arity = ty.results().len() as u32;
                stack.push_values(callee_arity)?;
            }
            Drop => {
                stack.pop_values(1)?;
            }
            Select => {
                // Pop two values and one condition.
                stack.pop_values(2)?;
                stack.pop_values(1)?;

                // Push the selected value.
                stack.push_values(1)?;
            }
            GetLocal(_) => {
                stack.push_values(1)?;
            }
            SetLocal(_) => {
                stack.pop_values(1)?;
            }
            TeeLocal(_) => {
                // This instruction pops and pushes the value, so
                // effectively it doesn't modify the stack height.
                stack.pop_values(1)?;
                stack.push_values(1)?;
            }
            GetGlobal(_) => {
                stack.push_values(1)?;
            }
            SetGlobal(_) => {
                stack.pop_values(1)?;
            }
            I32Load(_, _)
            | I64Load(_, _)
            | F32Load(_, _)
            | F64Load(_, _)
            | I32Load8S(_, _)
            | I32Load8U(_, _)
            | I32Load16S(_, _)
            | I32Load16U(_, _)
            | I64Load8S(_, _)
            | I64Load8U(_, _)
            | I64Load16S(_, _)
            | I64Load16U(_, _)
            | I64Load32S(_, _)
            | I64Load32U(_, _) => {
                // These instructions pop the address and pushes the result,
                // which effictively don't modify the stack height.
                stack.pop_values(1)?;
                stack.push_values(1)?;
            }

            I32Store(_, _)
            | I64Store(_, _)
            | F32Store(_, _)
            | F64Store(_, _)
            | I32Store8(_, _)
            | I32Store16(_, _)
            | I64Store8(_, _)
            | I64Store16(_, _)
            | I64Store32(_, _) => {
                // These instructions pop the address and the value.
                stack.pop_values(2)?;
            }

            CurrentMemory(_) => {
                // Pushes current memory size
                stack.push_values(1)?;
            }
            GrowMemory(_) => {
                // Grow memory takes the value of pages to grow and pushes
                stack.pop_values(1)?;
                stack.push_values(1)?;
            }

            I32Const(_) | I64Const(_) | F32Const(_) | F64Const(_) => {
                // These instructions just push the single literal value onto the stack.
                stack.push_values(1)?;
            }

            I32Eqz | I64Eqz => {
                // These instructions pop the value and compare it against zero, and pushes
                // the result of the comparison.
                stack.pop_values(1)?;
                stack.push_values(1)?;
            }

            I32Eq | I32Ne | I32LtS | I32LtU | I32GtS | I32GtU | I32LeS | I32LeU | I32GeS
            | I32GeU | I64Eq | I64Ne | I64LtS | I64LtU | I64GtS | I64GtU | I64LeS | I64LeU
            | I64GeS | I64GeU | F32Eq | F32Ne | F32Lt | F32Gt | F32Le | F32Ge | F64Eq | F64Ne
            | F64Lt | F64Gt | F64Le | F64Ge => {
                // Comparison operations take two operands and produce one result.
                stack.pop_values(2)?;
                stack.push_values(1)?;
            }

            I32Clz | I32Ctz | I32Popcnt | I64Clz | I64Ctz | I64Popcnt | F32Abs | F32Neg
            | F32Ceil | F32Floor | F32Trunc | F32Nearest | F32Sqrt | F64Abs | F64Neg | F64Ceil
            | F64Floor | F64Trunc | F64Nearest | F64Sqrt => {
                // Unary operators take one operand and produce one result.
                stack.pop_values(1)?;
                stack.push_values(1)?;
            }

            I32Add | I32Sub | I32Mul | I32DivS | I32DivU | I32RemS | I32RemU | I32And | I32Or
            | I32Xor | I32Shl | I32ShrS | I32ShrU | I32Rotl | I32Rotr | I64Add | I64Sub
            | I64Mul | I64DivS | I64DivU | I64RemS | I64RemU | I64And | I64Or | I64Xor | I64Shl
            | I64ShrS | I64ShrU | I64Rotl | I64Rotr | F32Add | F32Sub | F32Mul | F32Div
            | F32Min | F32Max | F32Copysign | F64Add | F64Sub | F64Mul | F64Div | F64Min
            | F64Max | F64Copysign => {
                // Binary operators take two operands and produce one result.
                stack.pop_values(2)?;
                stack.push_values(1)?;
            }

            I32WrapI64 | I32TruncSF32 | I32TruncUF32 | I32TruncSF64 | I32TruncUF64
            | I64ExtendSI32 | I64ExtendUI32 | I64TruncSF32 | I64TruncUF32 | I64TruncSF64
            | I64TruncUF64 | F32ConvertSI32 | F32ConvertUI32 | F32ConvertSI64 | F32ConvertUI64
            | F32DemoteF64 | F64ConvertSI32 | F64ConvertUI32 | F64ConvertSI64 | F64ConvertUI64
            | F64PromoteF32 | I32ReinterpretF32 | I64ReinterpretF64 | F32ReinterpretI32
            | F64ReinterpretI64 => {
                // Conversion operators take one value and produce one result.
                stack.pop_values(1)?;
                stack.push_values(1)?;
            }
        }
        pc += 1;
    }

    Ok(max_height)
}

#[cfg(test)]
mod tests {
    use super::*;
    use parity_wasm::elements;

    fn parse_wat(source: &str) -> elements::Module {
        let wasm = wat::parse_str(source).expect("Failed to wat2wasm");
        elements::deserialize_buffer(&wasm).expect("Failed to deserialize the module")
    }

    #[test]
    fn simple_test() {
        let module = parse_wat(
            r#"
(module
    (func
        i32.const 1
            i32.const 2
                i32.const 3
                drop
            drop
        drop
    )
)
"#,
        );

        let module_ctx = ModuleCtx::new(&module);
        let height = compute(0, &module_ctx).unwrap();
        assert_eq!(height, 3);
    }

    #[test]
    fn implicit_and_explicit_return() {
        let module = parse_wat(
            r#"
(module
    (func (result i32)
        i32.const 0
        return
    )
)
"#,
        );

        let module_ctx = ModuleCtx::new(&module);
        let height = compute(0, &module_ctx).unwrap();
        assert_eq!(height, 1);
    }

    #[test]
    fn dont_count_in_unreachable() {
        let module = parse_wat(
            r#"
(module
  (memory 0)
  (func (result i32)
    unreachable
    grow_memory
  )
)
"#,
        );

        let module_ctx = ModuleCtx::new(&module);
        let height = compute(0, &module_ctx).unwrap();
        assert_eq!(height, 0);
    }

    #[test]
    fn yet_another_test() {
        const SOURCE: &str = r#"
(module
  (memory 0)
  (func
    ;; Push two values and then pop them.
    ;; This will make max depth to be equal to 2.
    i32.const 0
    i32.const 1
    drop
    drop

    ;; Code after `unreachable` shouldn't have an effect
    ;; on the max depth.
    unreachable
    i32.const 0
    i32.const 1
    i32.const 2
  )
)
"#;
        let module = parse_wat(SOURCE);

        let module_ctx = ModuleCtx::new(&module);
        let height = compute(0, &module_ctx).unwrap();
        assert_eq!(height, 2);
    }

    #[test]
    fn call_indirect() {
        let module = parse_wat(
            r#"
(module
    (table $ptr 1 1 funcref)
    (elem $ptr (i32.const 0) func 1)
    (func $main
        (call_indirect (i32.const 0))
        (call_indirect (i32.const 0))
        (call_indirect (i32.const 0))
    )
    (func $callee
        i64.const 42
        drop
    )
)
"#,
        );

        let module_ctx = ModuleCtx::new(&module);
        let height = compute(0, &module_ctx).unwrap();
        assert_eq!(height, 1);
    }

    #[test]
    fn breaks() {
        let module = parse_wat(
            r#"
(module
    (func $main
        block (result i32)
            block (result i32)
                i32.const 99
                br 1
            end
        end
        drop
    )
)
"#,
        );

        let module_ctx = ModuleCtx::new(&module);
        let height = compute(0, &module_ctx).unwrap();
        assert_eq!(height, 1);
    }

    #[test]
    fn if_else_works() {
        let module = parse_wat(
            r#"
(module
    (func $main
        i32.const 7
        i32.const 1
        if (result i32)
            i32.const 42
        else
            i32.const 99
        end
        i32.const 97
        drop
        drop
        drop
    )
)
"#,
        );

        let module_ctx = ModuleCtx::new(&module);
        let height = compute(0, &module_ctx).unwrap();
        assert_eq!(height, 3);
    }
}
