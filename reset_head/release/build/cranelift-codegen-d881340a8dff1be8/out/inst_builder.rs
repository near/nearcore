/// Convenience methods for building instructions.
///
/// The `InstBuilder` trait has one method per instruction opcode for
/// conveniently constructing the instruction with minimum arguments.
/// Polymorphic instructions infer their result types from the input
/// arguments when possible. In some cases, an explicit `ctrl_typevar`
/// argument is required.
///
/// The opcode methods return the new instruction's result values, or
/// the `Inst` itself for instructions that don't have any results.
///
/// There is also a method per instruction format. These methods all
/// return an `Inst`.
pub trait InstBuilder<'f>: InstBuilderBase<'f> {
    /// Jump.
    ///
    /// Unconditionally jump to a basic block, passing the specified
    /// block arguments. The number and types of arguments must match the
    /// destination block.
    ///
    /// Inputs:
    ///
    /// - block: Destination basic block
    /// - args: block arguments
    #[allow(non_snake_case)]
    fn jump(mut self, block: ir::Block, args: &[Value]) -> Inst {
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(args.iter().cloned(), pool);
        }
        self.Jump(Opcode::Jump, types::INVALID, block, vlist).0
    }

    /// Branch when zero.
    ///
    /// Take the branch when ``c = 0``.
    ///
    /// Inputs:
    ///
    /// - c: Controlling value to test
    /// - block: Destination basic block
    /// - args: block arguments
    #[allow(non_snake_case)]
    fn brz(mut self, c: ir::Value, block: ir::Block, args: &[Value]) -> Inst {
        let ctrl_typevar = self.data_flow_graph().value_type(c);
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.push(c, pool);
            vlist.extend(args.iter().cloned(), pool);
        }
        self.Branch(Opcode::Brz, ctrl_typevar, block, vlist).0
    }

    /// Branch when non-zero.
    ///
    /// Take the branch when ``c != 0``.
    ///
    /// Inputs:
    ///
    /// - c: Controlling value to test
    /// - block: Destination basic block
    /// - args: block arguments
    #[allow(non_snake_case)]
    fn brnz(mut self, c: ir::Value, block: ir::Block, args: &[Value]) -> Inst {
        let ctrl_typevar = self.data_flow_graph().value_type(c);
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.push(c, pool);
            vlist.extend(args.iter().cloned(), pool);
        }
        self.Branch(Opcode::Brnz, ctrl_typevar, block, vlist).0
    }

    /// Indirect branch via jump table.
    ///
    /// Use ``x`` as an unsigned index into the jump table ``JT``. If a jump
    /// table entry is found, branch to the corresponding block. If no entry was
    /// found or the index is out-of-bounds, branch to the given default block.
    ///
    /// Note that this branch instruction can't pass arguments to the targeted
    /// blocks. Split critical edges as needed to work around this.
    ///
    /// Do not confuse this with "tables" in WebAssembly. ``br_table`` is for
    /// jump tables with destinations within the current function only -- think
    /// of a ``match`` in Rust or a ``switch`` in C.  If you want to call a
    /// function in a dynamic library, that will typically use
    /// ``call_indirect``.
    ///
    /// Inputs:
    ///
    /// - x: i32 index into jump table
    /// - block: Destination basic block
    /// - JT: A jump table.
    #[allow(non_snake_case)]
    fn br_table(self, x: ir::Value, block: ir::Block, JT: ir::JumpTable) -> Inst {
        self.BranchTable(Opcode::BrTable, types::INVALID, block, JT, x).0
    }

    /// Encodes an assembly debug trap.
    #[allow(non_snake_case)]
    fn debugtrap(self) -> Inst {
        self.NullAry(Opcode::Debugtrap, types::INVALID).0
    }

    /// Terminate execution unconditionally.
    ///
    /// Inputs:
    ///
    /// - code: A trap reason code.
    #[allow(non_snake_case)]
    fn trap<T1: Into<ir::TrapCode>>(self, code: T1) -> Inst {
        let code = code.into();
        self.Trap(Opcode::Trap, types::INVALID, code).0
    }

    /// Trap when zero.
    ///
    /// if ``c`` is non-zero, execution continues at the following instruction.
    ///
    /// Inputs:
    ///
    /// - c: Controlling value to test
    /// - code: A trap reason code.
    #[allow(non_snake_case)]
    fn trapz<T1: Into<ir::TrapCode>>(self, c: ir::Value, code: T1) -> Inst {
        let code = code.into();
        let ctrl_typevar = self.data_flow_graph().value_type(c);
        self.CondTrap(Opcode::Trapz, ctrl_typevar, code, c).0
    }

    /// A resumable trap.
    ///
    /// This instruction allows non-conditional traps to be used as non-terminal instructions.
    ///
    /// Inputs:
    ///
    /// - code: A trap reason code.
    #[allow(non_snake_case)]
    fn resumable_trap<T1: Into<ir::TrapCode>>(self, code: T1) -> Inst {
        let code = code.into();
        self.Trap(Opcode::ResumableTrap, types::INVALID, code).0
    }

    /// Trap when non-zero.
    ///
    /// If ``c`` is zero, execution continues at the following instruction.
    ///
    /// Inputs:
    ///
    /// - c: Controlling value to test
    /// - code: A trap reason code.
    #[allow(non_snake_case)]
    fn trapnz<T1: Into<ir::TrapCode>>(self, c: ir::Value, code: T1) -> Inst {
        let code = code.into();
        let ctrl_typevar = self.data_flow_graph().value_type(c);
        self.CondTrap(Opcode::Trapnz, ctrl_typevar, code, c).0
    }

    /// A resumable trap to be called when the passed condition is non-zero.
    ///
    /// If ``c`` is zero, execution continues at the following instruction.
    ///
    /// Inputs:
    ///
    /// - c: Controlling value to test
    /// - code: A trap reason code.
    #[allow(non_snake_case)]
    fn resumable_trapnz<T1: Into<ir::TrapCode>>(self, c: ir::Value, code: T1) -> Inst {
        let code = code.into();
        let ctrl_typevar = self.data_flow_graph().value_type(c);
        self.CondTrap(Opcode::ResumableTrapnz, ctrl_typevar, code, c).0
    }

    /// Return from the function.
    ///
    /// Unconditionally transfer control to the calling function, passing the
    /// provided return values. The list of return values must match the
    /// function signature's return types.
    ///
    /// Inputs:
    ///
    /// - rvals: return values
    #[allow(non_snake_case)]
    fn return_(mut self, rvals: &[Value]) -> Inst {
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(rvals.iter().cloned(), pool);
        }
        self.MultiAry(Opcode::Return, types::INVALID, vlist).0
    }

    /// Direct function call.
    ///
    /// Call a function which has been declared in the preamble. The argument
    /// types must match the function's signature.
    ///
    /// Inputs:
    ///
    /// - FN: function to call, declared by `function`
    /// - args: call arguments
    ///
    /// Outputs:
    ///
    /// - rvals: return values
    #[allow(non_snake_case)]
    fn call(mut self, FN: ir::FuncRef, args: &[Value]) -> Inst {
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(args.iter().cloned(), pool);
        }
        self.Call(Opcode::Call, types::INVALID, FN, vlist).0
    }

    /// Indirect function call.
    ///
    /// Call the function pointed to by `callee` with the given arguments. The
    /// called function must match the specified signature.
    ///
    /// Note that this is different from WebAssembly's ``call_indirect``; the
    /// callee is a native address, rather than a table index. For WebAssembly,
    /// `table_addr` and `load` are used to obtain a native address
    /// from a table.
    ///
    /// Inputs:
    ///
    /// - SIG: function signature
    /// - callee: address of function to call
    /// - args: call arguments
    ///
    /// Outputs:
    ///
    /// - rvals: return values
    #[allow(non_snake_case)]
    fn call_indirect(mut self, SIG: ir::SigRef, callee: ir::Value, args: &[Value]) -> Inst {
        let ctrl_typevar = self.data_flow_graph().value_type(callee);
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.push(callee, pool);
            vlist.extend(args.iter().cloned(), pool);
        }
        self.CallIndirect(Opcode::CallIndirect, ctrl_typevar, SIG, vlist).0
    }

    /// Get the address of a function.
    ///
    /// Compute the absolute address of a function declared in the preamble.
    /// The returned address can be used as a ``callee`` argument to
    /// `call_indirect`. This is also a method for calling functions that
    /// are too far away to be addressable by a direct `call`
    /// instruction.
    ///
    /// Inputs:
    ///
    /// - iAddr (controlling type variable): An integer address type
    /// - FN: function to call, declared by `function`
    ///
    /// Outputs:
    ///
    /// - addr: An integer address type
    #[allow(non_snake_case)]
    fn func_addr(self, iAddr: crate::ir::Type, FN: ir::FuncRef) -> Value {
        let (inst, dfg) = self.FuncAddr(Opcode::FuncAddr, iAddr, FN);
        dfg.first_result(inst)
    }

    /// Vector splat.
    ///
    /// Return a vector whose lanes are all ``x``.
    ///
    /// Inputs:
    ///
    /// - TxN (controlling type variable): A SIMD vector type
    /// - x: Value to splat to all lanes
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type
    #[allow(non_snake_case)]
    fn splat(self, TxN: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::Splat, TxN, x);
        dfg.first_result(inst)
    }

    /// Vector swizzle.
    ///
    /// Returns a new vector with byte-width lanes selected from the lanes of the first input
    /// vector ``x`` specified in the second input vector ``s``. The indices ``i`` in range
    /// ``[0, 15]`` select the ``i``-th element of ``x``. For indices outside of the range the
    /// resulting lane is 0. Note that this operates on byte-width lanes.
    ///
    /// Inputs:
    ///
    /// - TxN (controlling type variable): A SIMD vector type
    /// - x: Vector to modify by re-arranging lanes
    /// - y: Mask for re-arranging lanes
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type
    #[allow(non_snake_case)]
    fn swizzle(self, TxN: crate::ir::Type, x: ir::Value, y: ir::Value) -> Value {
        let (inst, dfg) = self.Binary(Opcode::Swizzle, TxN, x, y);
        dfg.first_result(inst)
    }

    /// Insert ``y`` as lane ``Idx`` in x.
    ///
    /// The lane index, ``Idx``, is an immediate value, not an SSA value. It
    /// must indicate a valid lane index for the type of ``x``.
    ///
    /// Inputs:
    ///
    /// - x: The vector to modify
    /// - y: New lane value
    /// - Idx: Lane index
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type
    #[allow(non_snake_case)]
    fn insertlane<T1: Into<ir::immediates::Uimm8>>(self, x: ir::Value, y: ir::Value, Idx: T1) -> Value {
        let Idx = Idx.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.TernaryImm8(Opcode::Insertlane, ctrl_typevar, Idx, x, y);
        dfg.first_result(inst)
    }

    /// Extract lane ``Idx`` from ``x``.
    ///
    /// The lane index, ``Idx``, is an immediate value, not an SSA value. It
    /// must indicate a valid lane index for the type of ``x``. Note that the upper bits of ``a``
    /// may or may not be zeroed depending on the ISA but the type system should prevent using
    /// ``a`` as anything other than the extracted value.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type
    /// - Idx: Lane index
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn extractlane<T1: Into<ir::immediates::Uimm8>>(self, x: ir::Value, Idx: T1) -> Value {
        let Idx = Idx.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm8(Opcode::Extractlane, ctrl_typevar, Idx, x);
        dfg.first_result(inst)
    }

    /// Signed integer minimum.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn smin(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Smin, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Unsigned integer minimum.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn umin(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Umin, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Signed integer maximum.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn smax(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Smax, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Unsigned integer maximum.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn umax(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Umax, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Unsigned average with rounding: `a := (x + y + 1) // 2`
    ///
    /// The addition does not lose any information (such as from overflow).
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integers
    /// - y: A SIMD vector type containing integers
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type containing integers
    #[allow(non_snake_case)]
    fn avg_round(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::AvgRound, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Add with unsigned saturation.
    ///
    /// This is similar to `iadd` but the operands are interpreted as unsigned integers and their
    /// summed result, instead of wrapping, will be saturated to the highest unsigned integer for
    /// the controlling type (e.g. `0xFF` for i8).
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integers
    /// - y: A SIMD vector type containing integers
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type containing integers
    #[allow(non_snake_case)]
    fn uadd_sat(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::UaddSat, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Add with signed saturation.
    ///
    /// This is similar to `iadd` but the operands are interpreted as signed integers and their
    /// summed result, instead of wrapping, will be saturated to the lowest or highest
    /// signed integer for the controlling type (e.g. `0x80` or `0x7F` for i8). For example,
    /// since an `sadd_sat.i8` of `0x70` and `0x70` is greater than `0x7F`, the result will be
    /// clamped to `0x7F`.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integers
    /// - y: A SIMD vector type containing integers
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type containing integers
    #[allow(non_snake_case)]
    fn sadd_sat(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::SaddSat, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Subtract with unsigned saturation.
    ///
    /// This is similar to `isub` but the operands are interpreted as unsigned integers and their
    /// difference, instead of wrapping, will be saturated to the lowest unsigned integer for
    /// the controlling type (e.g. `0x00` for i8).
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integers
    /// - y: A SIMD vector type containing integers
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type containing integers
    #[allow(non_snake_case)]
    fn usub_sat(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::UsubSat, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Subtract with signed saturation.
    ///
    /// This is similar to `isub` but the operands are interpreted as signed integers and their
    /// difference, instead of wrapping, will be saturated to the lowest or highest
    /// signed integer for the controlling type (e.g. `0x80` or `0x7F` for i8).
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integers
    /// - y: A SIMD vector type containing integers
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type containing integers
    #[allow(non_snake_case)]
    fn ssub_sat(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::SsubSat, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Load from memory at ``p + Offset``.
    ///
    /// This is a polymorphic instruction that can load any value type which
    /// has a memory representation.
    ///
    /// Inputs:
    ///
    /// - Mem (controlling type variable): Any type that can be stored in memory
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn load<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, Mem: crate::ir::Type, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let (inst, dfg) = self.Load(Opcode::Load, Mem, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Store ``x`` to memory at ``p + Offset``.
    ///
    /// This is a polymorphic instruction that can store any value type with a
    /// memory representation.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - x: Value to be stored
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    #[allow(non_snake_case)]
    fn store<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, x: ir::Value, p: ir::Value, Offset: T2) -> Inst {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        self.Store(Opcode::Store, ctrl_typevar, MemFlags, Offset, x, p).0
    }

    /// Load 8 bits from memory at ``p + Offset`` and zero-extend.
    ///
    /// This is equivalent to ``load.i8`` followed by ``uextend``.
    ///
    /// Inputs:
    ///
    /// - iExt8 (controlling type variable): An integer type with more than 8 bits
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: An integer type with more than 8 bits
    #[allow(non_snake_case)]
    fn uload8<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, iExt8: crate::ir::Type, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let (inst, dfg) = self.Load(Opcode::Uload8, iExt8, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load 8 bits from memory at ``p + Offset`` and sign-extend.
    ///
    /// This is equivalent to ``load.i8`` followed by ``sextend``.
    ///
    /// Inputs:
    ///
    /// - iExt8 (controlling type variable): An integer type with more than 8 bits
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: An integer type with more than 8 bits
    #[allow(non_snake_case)]
    fn sload8<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, iExt8: crate::ir::Type, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let (inst, dfg) = self.Load(Opcode::Sload8, iExt8, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Store the low 8 bits of ``x`` to memory at ``p + Offset``.
    ///
    /// This is equivalent to ``ireduce.i8`` followed by ``store.i8``.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - x: An integer type with more than 8 bits
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    #[allow(non_snake_case)]
    fn istore8<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, x: ir::Value, p: ir::Value, Offset: T2) -> Inst {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        self.Store(Opcode::Istore8, ctrl_typevar, MemFlags, Offset, x, p).0
    }

    /// Load 16 bits from memory at ``p + Offset`` and zero-extend.
    ///
    /// This is equivalent to ``load.i16`` followed by ``uextend``.
    ///
    /// Inputs:
    ///
    /// - iExt16 (controlling type variable): An integer type with more than 16 bits
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: An integer type with more than 16 bits
    #[allow(non_snake_case)]
    fn uload16<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, iExt16: crate::ir::Type, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let (inst, dfg) = self.Load(Opcode::Uload16, iExt16, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load 16 bits from memory at ``p + Offset`` and sign-extend.
    ///
    /// This is equivalent to ``load.i16`` followed by ``sextend``.
    ///
    /// Inputs:
    ///
    /// - iExt16 (controlling type variable): An integer type with more than 16 bits
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: An integer type with more than 16 bits
    #[allow(non_snake_case)]
    fn sload16<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, iExt16: crate::ir::Type, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let (inst, dfg) = self.Load(Opcode::Sload16, iExt16, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Store the low 16 bits of ``x`` to memory at ``p + Offset``.
    ///
    /// This is equivalent to ``ireduce.i16`` followed by ``store.i16``.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - x: An integer type with more than 16 bits
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    #[allow(non_snake_case)]
    fn istore16<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, x: ir::Value, p: ir::Value, Offset: T2) -> Inst {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        self.Store(Opcode::Istore16, ctrl_typevar, MemFlags, Offset, x, p).0
    }

    /// Load 32 bits from memory at ``p + Offset`` and zero-extend.
    ///
    /// This is equivalent to ``load.i32`` followed by ``uextend``.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: An integer type with more than 32 bits
    #[allow(non_snake_case)]
    fn uload32<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(p);
        let (inst, dfg) = self.Load(Opcode::Uload32, ctrl_typevar, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load 32 bits from memory at ``p + Offset`` and sign-extend.
    ///
    /// This is equivalent to ``load.i32`` followed by ``sextend``.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: An integer type with more than 32 bits
    #[allow(non_snake_case)]
    fn sload32<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(p);
        let (inst, dfg) = self.Load(Opcode::Sload32, ctrl_typevar, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Store the low 32 bits of ``x`` to memory at ``p + Offset``.
    ///
    /// This is equivalent to ``ireduce.i32`` followed by ``store.i32``.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - x: An integer type with more than 32 bits
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    #[allow(non_snake_case)]
    fn istore32<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, x: ir::Value, p: ir::Value, Offset: T2) -> Inst {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        self.Store(Opcode::Istore32, ctrl_typevar, MemFlags, Offset, x, p).0
    }

    /// Load an 8x8 vector (64 bits) from memory at ``p + Offset`` and zero-extend into an i16x8
    /// vector.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn uload8x8<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(p);
        let (inst, dfg) = self.Load(Opcode::Uload8x8, ctrl_typevar, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load an 8x8 vector (64 bits) from memory at ``p + Offset`` and sign-extend into an i16x8
    /// vector.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn sload8x8<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(p);
        let (inst, dfg) = self.Load(Opcode::Sload8x8, ctrl_typevar, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load a 16x4 vector (64 bits) from memory at ``p + Offset`` and zero-extend into an i32x4
    /// vector.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn uload16x4<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(p);
        let (inst, dfg) = self.Load(Opcode::Uload16x4, ctrl_typevar, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load a 16x4 vector (64 bits) from memory at ``p + Offset`` and sign-extend into an i32x4
    /// vector.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn sload16x4<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(p);
        let (inst, dfg) = self.Load(Opcode::Sload16x4, ctrl_typevar, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load an 32x2 vector (64 bits) from memory at ``p + Offset`` and zero-extend into an i64x2
    /// vector.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn uload32x2<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(p);
        let (inst, dfg) = self.Load(Opcode::Uload32x2, ctrl_typevar, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load a 32x2 vector (64 bits) from memory at ``p + Offset`` and sign-extend into an i64x2
    /// vector.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn sload32x2<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(p);
        let (inst, dfg) = self.Load(Opcode::Sload32x2, ctrl_typevar, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load a value from a stack slot at the constant offset.
    ///
    /// This is a polymorphic instruction that can load any value type which
    /// has a memory representation.
    ///
    /// The offset is an immediate constant, not an SSA value. The memory
    /// access cannot go out of bounds, i.e.
    /// `sizeof(a) + Offset <= sizeof(SS)`.
    ///
    /// Inputs:
    ///
    /// - Mem (controlling type variable): Any type that can be stored in memory
    /// - SS: A stack slot
    /// - Offset: In-bounds offset into stack slot
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn stack_load<T1: Into<ir::immediates::Offset32>>(self, Mem: crate::ir::Type, SS: ir::StackSlot, Offset: T1) -> Value {
        let Offset = Offset.into();
        let (inst, dfg) = self.StackLoad(Opcode::StackLoad, Mem, SS, Offset);
        dfg.first_result(inst)
    }

    /// Store a value to a stack slot at a constant offset.
    ///
    /// This is a polymorphic instruction that can store any value type with a
    /// memory representation.
    ///
    /// The offset is an immediate constant, not an SSA value. The memory
    /// access cannot go out of bounds, i.e.
    /// `sizeof(a) + Offset <= sizeof(SS)`.
    ///
    /// Inputs:
    ///
    /// - x: Value to be stored
    /// - SS: A stack slot
    /// - Offset: In-bounds offset into stack slot
    #[allow(non_snake_case)]
    fn stack_store<T1: Into<ir::immediates::Offset32>>(self, x: ir::Value, SS: ir::StackSlot, Offset: T1) -> Inst {
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        self.StackStore(Opcode::StackStore, ctrl_typevar, SS, Offset, x).0
    }

    /// Get the address of a stack slot.
    ///
    /// Compute the absolute address of a byte in a stack slot. The offset must
    /// refer to a byte inside the stack slot:
    /// `0 <= Offset < sizeof(SS)`.
    ///
    /// Inputs:
    ///
    /// - iAddr (controlling type variable): An integer address type
    /// - SS: A stack slot
    /// - Offset: In-bounds offset into stack slot
    ///
    /// Outputs:
    ///
    /// - addr: An integer address type
    #[allow(non_snake_case)]
    fn stack_addr<T1: Into<ir::immediates::Offset32>>(self, iAddr: crate::ir::Type, SS: ir::StackSlot, Offset: T1) -> Value {
        let Offset = Offset.into();
        let (inst, dfg) = self.StackLoad(Opcode::StackAddr, iAddr, SS, Offset);
        dfg.first_result(inst)
    }

    /// Load a value from a dynamic stack slot.
    ///
    /// This is a polymorphic instruction that can load any value type which
    /// has a memory representation.
    ///
    /// Inputs:
    ///
    /// - Mem (controlling type variable): Any type that can be stored in memory
    /// - DSS: A dynamic stack slot
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn dynamic_stack_load(self, Mem: crate::ir::Type, DSS: ir::DynamicStackSlot) -> Value {
        let (inst, dfg) = self.DynamicStackLoad(Opcode::DynamicStackLoad, Mem, DSS);
        dfg.first_result(inst)
    }

    /// Store a value to a dynamic stack slot.
    ///
    /// This is a polymorphic instruction that can store any dynamic value type with a
    /// memory representation.
    ///
    /// Inputs:
    ///
    /// - x: Value to be stored
    /// - DSS: A dynamic stack slot
    #[allow(non_snake_case)]
    fn dynamic_stack_store(self, x: ir::Value, DSS: ir::DynamicStackSlot) -> Inst {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        self.DynamicStackStore(Opcode::DynamicStackStore, ctrl_typevar, DSS, x).0
    }

    /// Get the address of a dynamic stack slot.
    ///
    /// Compute the absolute address of the first byte of a dynamic stack slot.
    ///
    /// Inputs:
    ///
    /// - iAddr (controlling type variable): An integer address type
    /// - DSS: A dynamic stack slot
    ///
    /// Outputs:
    ///
    /// - addr: An integer address type
    #[allow(non_snake_case)]
    fn dynamic_stack_addr(self, iAddr: crate::ir::Type, DSS: ir::DynamicStackSlot) -> Value {
        let (inst, dfg) = self.DynamicStackLoad(Opcode::DynamicStackAddr, iAddr, DSS);
        dfg.first_result(inst)
    }

    /// Compute the value of global GV.
    ///
    /// Inputs:
    ///
    /// - Mem (controlling type variable): Any type that can be stored in memory
    /// - GV: A global value.
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn global_value(self, Mem: crate::ir::Type, GV: ir::GlobalValue) -> Value {
        let (inst, dfg) = self.UnaryGlobalValue(Opcode::GlobalValue, Mem, GV);
        dfg.first_result(inst)
    }

    /// Compute the value of global GV, which is a symbolic value.
    ///
    /// Inputs:
    ///
    /// - Mem (controlling type variable): Any type that can be stored in memory
    /// - GV: A global value.
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn symbol_value(self, Mem: crate::ir::Type, GV: ir::GlobalValue) -> Value {
        let (inst, dfg) = self.UnaryGlobalValue(Opcode::SymbolValue, Mem, GV);
        dfg.first_result(inst)
    }

    /// Compute the value of global GV, which is a TLS (thread local storage) value.
    ///
    /// Inputs:
    ///
    /// - Mem (controlling type variable): Any type that can be stored in memory
    /// - GV: A global value.
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn tls_value(self, Mem: crate::ir::Type, GV: ir::GlobalValue) -> Value {
        let (inst, dfg) = self.UnaryGlobalValue(Opcode::TlsValue, Mem, GV);
        dfg.first_result(inst)
    }

    /// Bounds check and compute absolute address of ``index + Offset`` in heap memory.
    ///
    /// Verify that the range ``index .. index + Offset + Size`` is in bounds for the
    /// heap ``H``, and generate an absolute address that is safe to dereference.
    ///
    /// 1. If ``index + Offset + Size`` is less than or equal ot the heap bound, return an
    ///    absolute address corresponding to a byte offset of ``index + Offset`` from the
    ///    heap's base address.
    ///
    /// 2. If ``index + Offset + Size`` is greater than the heap bound, return the
    ///    ``NULL`` pointer or any other address that is guaranteed to generate a trap
    ///    when accessed.
    ///
    /// Inputs:
    ///
    /// - iAddr (controlling type variable): An integer address type
    /// - H: A heap.
    /// - index: An unsigned heap offset
    /// - Offset: Static offset immediate in bytes
    /// - Size: Static size immediate in bytes
    ///
    /// Outputs:
    ///
    /// - addr: An integer address type
    #[allow(non_snake_case)]
    fn heap_addr<T1: Into<ir::immediates::Uimm32>, T2: Into<ir::immediates::Uimm8>>(self, iAddr: crate::ir::Type, H: ir::Heap, index: ir::Value, Offset: T1, Size: T2) -> Value {
        let Offset = Offset.into();
        let Size = Size.into();
        let (inst, dfg) = self.HeapAddr(Opcode::HeapAddr, iAddr, H, Offset, Size, index);
        dfg.first_result(inst)
    }

    /// Load a value from the given heap at address ``index + offset``,
    /// trapping on out-of-bounds accesses.
    ///
    /// Checks that ``index + offset .. index + offset + sizeof(a)`` is
    /// within the heap's bounds, trapping if it is not. Otherwise, when
    /// that range is in bounds, loads the value from the heap.
    ///
    /// Traps on ``index + offset + sizeof(a)`` overflow.
    ///
    /// Inputs:
    ///
    /// - Mem (controlling type variable): Any type that can be stored in memory
    /// - heap_imm: Reference to out-of-line heap access immediates
    /// - index: Dynamic index (in bytes) into the heap
    ///
    /// Outputs:
    ///
    /// - a: The value loaded from the heap
    #[allow(non_snake_case)]
    fn heap_load<T1: Into<ir::HeapImm>>(self, Mem: crate::ir::Type, heap_imm: T1, index: ir::Value) -> Value {
        let heap_imm = heap_imm.into();
        let (inst, dfg) = self.HeapLoad(Opcode::HeapLoad, Mem, heap_imm, index);
        dfg.first_result(inst)
    }

    /// Store ``a`` into the given heap at address ``index + offset``,
    /// trapping on out-of-bounds accesses.
    ///
    /// Checks that ``index + offset .. index + offset + sizeof(a)`` is
    /// within the heap's bounds, trapping if it is not. Otherwise, when
    /// that range is in bounds, stores the value into the heap.
    ///
    /// Traps on ``index + offset + sizeof(a)`` overflow.
    ///
    /// Inputs:
    ///
    /// - heap_imm: Reference to out-of-line heap access immediates
    /// - index: Dynamic index (in bytes) into the heap
    /// - a: The value stored into the heap
    #[allow(non_snake_case)]
    fn heap_store<T1: Into<ir::HeapImm>>(self, heap_imm: T1, index: ir::Value, a: ir::Value) -> Inst {
        let heap_imm = heap_imm.into();
        let ctrl_typevar = self.data_flow_graph().value_type(index);
        self.HeapStore(Opcode::HeapStore, ctrl_typevar, heap_imm, index, a).0
    }

    /// Gets the content of the pinned register, when it's enabled.
    ///
    /// Inputs:
    ///
    /// - iAddr (controlling type variable): An integer address type
    ///
    /// Outputs:
    ///
    /// - addr: An integer address type
    #[allow(non_snake_case)]
    fn get_pinned_reg(self, iAddr: crate::ir::Type) -> Value {
        let (inst, dfg) = self.NullAry(Opcode::GetPinnedReg, iAddr);
        dfg.first_result(inst)
    }

    /// Sets the content of the pinned register, when it's enabled.
    ///
    /// Inputs:
    ///
    /// - addr: An integer address type
    #[allow(non_snake_case)]
    fn set_pinned_reg(self, addr: ir::Value) -> Inst {
        let ctrl_typevar = self.data_flow_graph().value_type(addr);
        self.Unary(Opcode::SetPinnedReg, ctrl_typevar, addr).0
    }

    /// Get the address in the frame pointer register.
    ///
    /// Usage of this instruction requires setting `preserve_frame_pointers` to `true`.
    ///
    /// Inputs:
    ///
    /// - iAddr (controlling type variable): An integer address type
    ///
    /// Outputs:
    ///
    /// - addr: An integer address type
    #[allow(non_snake_case)]
    fn get_frame_pointer(self, iAddr: crate::ir::Type) -> Value {
        let (inst, dfg) = self.NullAry(Opcode::GetFramePointer, iAddr);
        dfg.first_result(inst)
    }

    /// Get the address in the stack pointer register.
    ///
    /// Inputs:
    ///
    /// - iAddr (controlling type variable): An integer address type
    ///
    /// Outputs:
    ///
    /// - addr: An integer address type
    #[allow(non_snake_case)]
    fn get_stack_pointer(self, iAddr: crate::ir::Type) -> Value {
        let (inst, dfg) = self.NullAry(Opcode::GetStackPointer, iAddr);
        dfg.first_result(inst)
    }

    /// Get the PC where this function will transfer control to when it returns.
    ///
    /// Usage of this instruction requires setting `preserve_frame_pointers` to `true`.
    ///
    /// Inputs:
    ///
    /// - iAddr (controlling type variable): An integer address type
    ///
    /// Outputs:
    ///
    /// - addr: An integer address type
    #[allow(non_snake_case)]
    fn get_return_address(self, iAddr: crate::ir::Type) -> Value {
        let (inst, dfg) = self.NullAry(Opcode::GetReturnAddress, iAddr);
        dfg.first_result(inst)
    }

    /// Bounds check and compute absolute address of a table entry.
    ///
    /// Verify that the offset ``p`` is in bounds for the table T, and generate
    /// an absolute address that is safe to dereference.
    ///
    /// ``Offset`` must be less than the size of a table element.
    ///
    /// 1. If ``p`` is not greater than the table bound, return an absolute
    ///    address corresponding to a byte offset of ``p`` from the table's
    ///    base address.
    /// 2. If ``p`` is greater than the table bound, generate a trap.
    ///
    /// Inputs:
    ///
    /// - iAddr (controlling type variable): An integer address type
    /// - T: A table.
    /// - p: An unsigned table offset
    /// - Offset: Byte offset from element address
    ///
    /// Outputs:
    ///
    /// - addr: An integer address type
    #[allow(non_snake_case)]
    fn table_addr<T1: Into<ir::immediates::Offset32>>(self, iAddr: crate::ir::Type, T: ir::Table, p: ir::Value, Offset: T1) -> Value {
        let Offset = Offset.into();
        let (inst, dfg) = self.TableAddr(Opcode::TableAddr, iAddr, T, Offset, p);
        dfg.first_result(inst)
    }

    /// Integer constant.
    ///
    /// Create a scalar integer SSA value with an immediate constant value, or
    /// an integer vector where all the lanes have the same value.
    ///
    /// Inputs:
    ///
    /// - NarrowInt (controlling type variable): An integer type with lanes type to `i64`
    /// - N: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A constant integer scalar or vector value
    #[allow(non_snake_case)]
    fn iconst<T1: Into<ir::immediates::Imm64>>(self, NarrowInt: crate::ir::Type, N: T1) -> Value {
        let N = N.into();
        let (inst, dfg) = self.UnaryImm(Opcode::Iconst, NarrowInt, N);
        dfg.first_result(inst)
    }

    /// Floating point constant.
    ///
    /// Create a `f32` SSA value with an immediate constant value.
    ///
    /// Inputs:
    ///
    /// - N: A 32-bit immediate floating point number.
    ///
    /// Outputs:
    ///
    /// - a: A constant f32 scalar value
    #[allow(non_snake_case)]
    fn f32const<T1: Into<ir::immediates::Ieee32>>(self, N: T1) -> Value {
        let N = N.into();
        let (inst, dfg) = self.UnaryIeee32(Opcode::F32const, types::INVALID, N);
        dfg.first_result(inst)
    }

    /// Floating point constant.
    ///
    /// Create a `f64` SSA value with an immediate constant value.
    ///
    /// Inputs:
    ///
    /// - N: A 64-bit immediate floating point number.
    ///
    /// Outputs:
    ///
    /// - a: A constant f64 scalar value
    #[allow(non_snake_case)]
    fn f64const<T1: Into<ir::immediates::Ieee64>>(self, N: T1) -> Value {
        let N = N.into();
        let (inst, dfg) = self.UnaryIeee64(Opcode::F64const, types::INVALID, N);
        dfg.first_result(inst)
    }

    /// SIMD vector constant.
    ///
    /// Construct a vector with the given immediate bytes.
    ///
    /// Inputs:
    ///
    /// - TxN (controlling type variable): A SIMD vector type
    /// - N: The 16 immediate bytes of a 128-bit vector
    ///
    /// Outputs:
    ///
    /// - a: A constant vector value
    #[allow(non_snake_case)]
    fn vconst<T1: Into<ir::Constant>>(self, TxN: crate::ir::Type, N: T1) -> Value {
        let N = N.into();
        let (inst, dfg) = self.UnaryConst(Opcode::Vconst, TxN, N);
        dfg.first_result(inst)
    }

    /// SIMD vector shuffle.
    ///
    /// Shuffle two vectors using the given immediate bytes. For each of the 16 bytes of the
    /// immediate, a value i of 0-15 selects the i-th element of the first vector and a value i of
    /// 16-31 selects the (i-16)th element of the second vector. Immediate values outside of the
    /// 0-31 range place a 0 in the resulting vector lane.
    ///
    /// Inputs:
    ///
    /// - a: A vector value
    /// - b: A vector value
    /// - mask: The 16 immediate bytes used for selecting the elements to shuffle
    ///
    /// Outputs:
    ///
    /// - a: A vector value
    #[allow(non_snake_case)]
    fn shuffle<T1: Into<ir::Immediate>>(self, a: ir::Value, b: ir::Value, mask: T1) -> Value {
        let mask = mask.into();
        let (inst, dfg) = self.Shuffle(Opcode::Shuffle, types::INVALID, mask, a, b);
        dfg.first_result(inst)
    }

    /// Null constant value for reference types.
    ///
    /// Create a scalar reference SSA value with a constant null value.
    ///
    /// Inputs:
    ///
    /// - Ref (controlling type variable): A scalar reference type
    ///
    /// Outputs:
    ///
    /// - a: A constant reference null value
    #[allow(non_snake_case)]
    fn null(self, Ref: crate::ir::Type) -> Value {
        let (inst, dfg) = self.NullAry(Opcode::Null, Ref);
        dfg.first_result(inst)
    }

    /// Just a dummy instruction.
    ///
    /// Note: this doesn't compile to a machine code nop.
    #[allow(non_snake_case)]
    fn nop(self) -> Inst {
        self.NullAry(Opcode::Nop, types::INVALID).0
    }

    /// Conditional select.
    ///
    /// This instruction selects whole values. Use `vselect` for
    /// lane-wise selection.
    ///
    /// Inputs:
    ///
    /// - c: Controlling value to test
    /// - x: Value to use when `c` is true
    /// - y: Value to use when `c` is false
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, or reference scalar or vector type
    #[allow(non_snake_case)]
    fn select(self, c: ir::Value, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Ternary(Opcode::Select, ctrl_typevar, c, x, y);
        dfg.first_result(inst)
    }

    /// Conditional select intended for Spectre guards.
    ///
    /// This operation is semantically equivalent to a select instruction.
    /// However, it is guaranteed to not be removed or otherwise altered by any
    /// optimization pass, and is guaranteed to result in a conditional-move
    /// instruction, not a branch-based lowering.  As such, it is suitable
    /// for use when producing Spectre guards. For example, a bounds-check
    /// may guard against unsafe speculation past a bounds-check conditional
    /// branch by passing the address or index to be accessed through a
    /// conditional move, also gated on the same condition. Because no
    /// Spectre-vulnerable processors are known to perform speculation on
    /// conditional move instructions, this is guaranteed to pick the
    /// correct input. If the selected input in case of overflow is a "safe"
    /// value, for example a null pointer that causes an exception in the
    /// speculative path, this ensures that no Spectre vulnerability will
    /// exist.
    ///
    /// Inputs:
    ///
    /// - c: Controlling value to test
    /// - x: Value to use when `c` is true
    /// - y: Value to use when `c` is false
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, or reference scalar or vector type
    #[allow(non_snake_case)]
    fn select_spectre_guard(self, c: ir::Value, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Ternary(Opcode::SelectSpectreGuard, ctrl_typevar, c, x, y);
        dfg.first_result(inst)
    }

    /// Conditional select of bits.
    ///
    /// For each bit in `c`, this instruction selects the corresponding bit from `x` if the bit
    /// in `c` is 1 and the corresponding bit from `y` if the bit in `c` is 0. See also:
    /// `select`, `vselect`.
    ///
    /// Inputs:
    ///
    /// - c: Controlling value to test
    /// - x: Value to use when `c` is true
    /// - y: Value to use when `c` is false
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, or reference scalar or vector type
    #[allow(non_snake_case)]
    fn bitselect(self, c: ir::Value, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Ternary(Opcode::Bitselect, ctrl_typevar, c, x, y);
        dfg.first_result(inst)
    }

    /// Split a vector into two halves.
    ///
    /// Split the vector `x` into two separate values, each containing half of
    /// the lanes from ``x``. The result may be two scalars if ``x`` only had
    /// two lanes.
    ///
    /// Inputs:
    ///
    /// - x: Vector to split
    ///
    /// Outputs:
    ///
    /// - lo: Low-numbered lanes of `x`
    /// - hi: High-numbered lanes of `x`
    #[allow(non_snake_case)]
    fn vsplit(self, x: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Vsplit, ctrl_typevar, x);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Vector concatenation.
    ///
    /// Return a vector formed by concatenating ``x`` and ``y``. The resulting
    /// vector type has twice as many lanes as each of the inputs. The lanes of
    /// ``x`` appear as the low-numbered lanes, and the lanes of ``y`` become
    /// the high-numbered lanes of ``a``.
    ///
    /// It is possible to form a vector by concatenating two scalars.
    ///
    /// Inputs:
    ///
    /// - x: Low-numbered lanes
    /// - y: High-numbered lanes
    ///
    /// Outputs:
    ///
    /// - a: Concatenation of `x` and `y`
    #[allow(non_snake_case)]
    fn vconcat(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Vconcat, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Vector lane select.
    ///
    /// Select lanes from ``x`` or ``y`` controlled by the lanes of the truthy
    /// vector ``c``.
    ///
    /// Inputs:
    ///
    /// - c: Controlling vector
    /// - x: Value to use where `c` is true
    /// - y: Value to use where `c` is false
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type
    #[allow(non_snake_case)]
    fn vselect(self, c: ir::Value, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Ternary(Opcode::Vselect, ctrl_typevar, c, x, y);
        dfg.first_result(inst)
    }

    /// Reduce a vector to a scalar boolean.
    ///
    /// Return a scalar boolean true if any lane in ``a`` is non-zero, false otherwise.
    ///
    /// Inputs:
    ///
    /// - a: A SIMD vector type
    ///
    /// Outputs:
    ///
    /// - s: An integer type with 8 bits.
    /// WARNING: arithmetic on 8bit integers is incomplete
    #[allow(non_snake_case)]
    fn vany_true(self, a: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(a);
        let (inst, dfg) = self.Unary(Opcode::VanyTrue, ctrl_typevar, a);
        dfg.first_result(inst)
    }

    /// Reduce a vector to a scalar boolean.
    ///
    /// Return a scalar boolean true if all lanes in ``i`` are non-zero, false otherwise.
    ///
    /// Inputs:
    ///
    /// - a: A SIMD vector type
    ///
    /// Outputs:
    ///
    /// - s: An integer type with 8 bits.
    /// WARNING: arithmetic on 8bit integers is incomplete
    #[allow(non_snake_case)]
    fn vall_true(self, a: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(a);
        let (inst, dfg) = self.Unary(Opcode::VallTrue, ctrl_typevar, a);
        dfg.first_result(inst)
    }

    /// Reduce a vector to a scalar integer.
    ///
    /// Return a scalar integer, consisting of the concatenation of the most significant bit
    /// of each lane of ``a``.
    ///
    /// Inputs:
    ///
    /// - Int (controlling type variable): A scalar or vector integer type
    /// - a: A SIMD vector type
    ///
    /// Outputs:
    ///
    /// - x: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn vhigh_bits(self, Int: crate::ir::Type, a: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::VhighBits, Int, a);
        dfg.first_result(inst)
    }

    /// Integer comparison.
    ///
    /// The condition code determines if the operands are interpreted as signed
    /// or unsigned integers.
    ///
    /// | Signed | Unsigned | Condition             |
    /// |--------|----------|-----------------------|
    /// | eq     | eq       | Equal                 |
    /// | ne     | ne       | Not equal             |
    /// | slt    | ult      | Less than             |
    /// | sge    | uge      | Greater than or equal |
    /// | sgt    | ugt      | Greater than          |
    /// | sle    | ule      | Less than or equal    |
    ///
    /// When this instruction compares integer vectors, it returns a vector of
    /// lane-wise comparisons.
    ///
    /// Inputs:
    ///
    /// - Cond: An integer comparison condition code.
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn icmp<T1: Into<ir::condcodes::IntCC>>(self, Cond: T1, x: ir::Value, y: ir::Value) -> Value {
        let Cond = Cond.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.IntCompare(Opcode::Icmp, ctrl_typevar, Cond, x, y);
        dfg.first_result(inst)
    }

    /// Compare scalar integer to a constant.
    ///
    /// This is the same as the `icmp` instruction, except one operand is
    /// a sign extended 64 bit immediate constant.
    ///
    /// This instruction can only compare scalars. Use `icmp` for
    /// lane-wise vector comparisons.
    ///
    /// Inputs:
    ///
    /// - Cond: An integer comparison condition code.
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: An integer type with 8 bits.
    /// WARNING: arithmetic on 8bit integers is incomplete
    #[allow(non_snake_case)]
    fn icmp_imm<T1: Into<ir::condcodes::IntCC>, T2: Into<ir::immediates::Imm64>>(self, Cond: T1, x: ir::Value, Y: T2) -> Value {
        let Cond = Cond.into();
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.IntCompareImm(Opcode::IcmpImm, ctrl_typevar, Cond, Y, x);
        dfg.first_result(inst)
    }

    /// Compare scalar integers and return flags.
    ///
    /// Compare two scalar integer values and return integer CPU flags
    /// representing the result.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - f: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    #[allow(non_snake_case)]
    fn ifcmp(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Ifcmp, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Compare scalar integer to a constant and return flags.
    ///
    /// Like `icmp_imm`, but returns integer CPU flags instead of testing
    /// a specific condition code.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - f: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    #[allow(non_snake_case)]
    fn ifcmp_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::IfcmpImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Wrapping integer addition: `a := x + y \pmod{2^B}`.
    ///
    /// This instruction does not depend on the signed/unsigned interpretation
    /// of the operands.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn iadd(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Iadd, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Wrapping integer subtraction: `a := x - y \pmod{2^B}`.
    ///
    /// This instruction does not depend on the signed/unsigned interpretation
    /// of the operands.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn isub(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Isub, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Integer negation: `a := -x \pmod{2^B}`.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn ineg(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Ineg, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Integer absolute value with wrapping: `a := |x|`.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn iabs(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Iabs, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Wrapping integer multiplication: `a := x y \pmod{2^B}`.
    ///
    /// This instruction does not depend on the signed/unsigned interpretation
    /// of the operands.
    ///
    /// Polymorphic over all integer types (vector and scalar).
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn imul(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Imul, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Unsigned integer multiplication, producing the high half of a
    /// double-length result.
    ///
    /// Polymorphic over all integer types (vector and scalar).
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn umulhi(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Umulhi, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Signed integer multiplication, producing the high half of a
    /// double-length result.
    ///
    /// Polymorphic over all integer types (vector and scalar).
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn smulhi(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Smulhi, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Fixed-point multiplication of numbers in the QN format, where N + 1
    /// is the number bitwidth:
    /// `a := signed_saturate((x * y + 1 << (Q - 1)) >> Q)`
    ///
    /// Polymorphic over all integer types (scalar and vector) with 16- or
    /// 32-bit numbers.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type with 16- or 32-bit numbers
    /// - y: A scalar or vector integer type with 16- or 32-bit numbers
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type with 16- or 32-bit numbers
    #[allow(non_snake_case)]
    fn sqmul_round_sat(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::SqmulRoundSat, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Unsigned integer division: `a := \lfloor {x \over y} \rfloor`.
    ///
    /// This operation traps if the divisor is zero.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn udiv(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Udiv, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Signed integer division rounded toward zero: `a := sign(xy)
    /// \lfloor {|x| \over |y|}\rfloor`.
    ///
    /// This operation traps if the divisor is zero, or if the result is not
    /// representable in `B` bits two's complement. This only happens
    /// when `x = -2^{B-1}, y = -1`.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn sdiv(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Sdiv, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Unsigned integer remainder.
    ///
    /// This operation traps if the divisor is zero.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn urem(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Urem, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Signed integer remainder. The result has the sign of the dividend.
    ///
    /// This operation traps if the divisor is zero.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn srem(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Srem, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Add immediate integer.
    ///
    /// Same as `iadd`, but one operand is a sign extended 64 bit immediate constant.
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn iadd_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::IaddImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Integer multiplication by immediate constant.
    ///
    /// Same as `imul`, but one operand is a sign extended 64 bit immediate constant.
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn imul_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::ImulImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Unsigned integer division by an immediate constant.
    ///
    /// Same as `udiv`, but one operand is a zero extended 64 bit immediate constant.
    ///
    /// This operation traps if the divisor is zero.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn udiv_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::UdivImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Signed integer division by an immediate constant.
    ///
    /// Same as `sdiv`, but one operand is a sign extended 64 bit immediate constant.
    ///
    /// This operation traps if the divisor is zero, or if the result is not
    /// representable in `B` bits two's complement. This only happens
    /// when `x = -2^{B-1}, Y = -1`.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn sdiv_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::SdivImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Unsigned integer remainder with immediate divisor.
    ///
    /// Same as `urem`, but one operand is a zero extended 64 bit immediate constant.
    ///
    /// This operation traps if the divisor is zero.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn urem_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::UremImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Signed integer remainder with immediate divisor.
    ///
    /// Same as `srem`, but one operand is a sign extended 64 bit immediate constant.
    ///
    /// This operation traps if the divisor is zero.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn srem_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::SremImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Immediate reverse wrapping subtraction: `a := Y - x \pmod{2^B}`.
    ///
    /// The immediate operand is a sign extended 64 bit constant.
    ///
    /// Also works as integer negation when `Y = 0`. Use `iadd_imm`
    /// with a negative immediate operand for the reverse immediate
    /// subtraction.
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn irsub_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::IrsubImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Add integers with carry in.
    ///
    /// Same as `iadd` with an additional carry input. Computes:
    ///
    /// ```text
    ///     a = x + y + c_{in} \pmod 2^B
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    /// - c_in: Input carry flag
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn iadd_cin(self, x: ir::Value, y: ir::Value, c_in: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(y);
        let (inst, dfg) = self.Ternary(Opcode::IaddCin, ctrl_typevar, x, y, c_in);
        dfg.first_result(inst)
    }

    /// Add integers with carry in.
    ///
    /// Same as `iadd` with an additional carry flag input. Computes:
    ///
    /// ```text
    ///     a = x + y + c_{in} \pmod 2^B
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    /// - c_in: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn iadd_ifcin(self, x: ir::Value, y: ir::Value, c_in: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(y);
        let (inst, dfg) = self.Ternary(Opcode::IaddIfcin, ctrl_typevar, x, y, c_in);
        dfg.first_result(inst)
    }

    /// Add integers with carry out.
    ///
    /// Same as `iadd` with an additional carry output.
    ///
    /// ```text
    ///     a &= x + y \pmod 2^B \\
    ///     c_{out} &= x+y >= 2^B
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    /// - c_out: Output carry flag
    #[allow(non_snake_case)]
    fn iadd_cout(self, x: ir::Value, y: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::IaddCout, ctrl_typevar, x, y);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Add integers with carry out.
    ///
    /// Same as `iadd` with an additional carry flag output.
    ///
    /// ```text
    ///     a &= x + y \pmod 2^B \\
    ///     c_{out} &= x+y >= 2^B
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    /// - c_out: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    #[allow(non_snake_case)]
    fn iadd_ifcout(self, x: ir::Value, y: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::IaddIfcout, ctrl_typevar, x, y);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Add integers with carry in and out.
    ///
    /// Same as `iadd` with an additional carry input and output.
    ///
    /// ```text
    ///     a &= x + y + c_{in} \pmod 2^B \\
    ///     c_{out} &= x + y + c_{in} >= 2^B
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    /// - c_in: Input carry flag
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    /// - c_out: Output carry flag
    #[allow(non_snake_case)]
    fn iadd_carry(self, x: ir::Value, y: ir::Value, c_in: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(y);
        let (inst, dfg) = self.Ternary(Opcode::IaddCarry, ctrl_typevar, x, y, c_in);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Add integers with carry in and out.
    ///
    /// Same as `iadd` with an additional carry flag input and output.
    ///
    /// ```text
    ///     a &= x + y + c_{in} \pmod 2^B \\
    ///     c_{out} &= x + y + c_{in} >= 2^B
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    /// - c_in: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    /// - c_out: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    #[allow(non_snake_case)]
    fn iadd_ifcarry(self, x: ir::Value, y: ir::Value, c_in: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(y);
        let (inst, dfg) = self.Ternary(Opcode::IaddIfcarry, ctrl_typevar, x, y, c_in);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Unsigned addition of x and y, trapping if the result overflows.
    ///
    /// Accepts 32 or 64-bit integers, and does not support vector types.
    ///
    /// Inputs:
    ///
    /// - x: A 32 or 64-bit scalar integer type
    /// - y: A 32 or 64-bit scalar integer type
    /// - code: A trap reason code.
    ///
    /// Outputs:
    ///
    /// - a: A 32 or 64-bit scalar integer type
    #[allow(non_snake_case)]
    fn uadd_overflow_trap<T1: Into<ir::TrapCode>>(self, x: ir::Value, y: ir::Value, code: T1) -> Value {
        let code = code.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.IntAddTrap(Opcode::UaddOverflowTrap, ctrl_typevar, code, x, y);
        dfg.first_result(inst)
    }

    /// Subtract integers with borrow in.
    ///
    /// Same as `isub` with an additional borrow flag input. Computes:
    ///
    /// ```text
    ///     a = x - (y + b_{in}) \pmod 2^B
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    /// - b_in: Input borrow flag
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn isub_bin(self, x: ir::Value, y: ir::Value, b_in: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(y);
        let (inst, dfg) = self.Ternary(Opcode::IsubBin, ctrl_typevar, x, y, b_in);
        dfg.first_result(inst)
    }

    /// Subtract integers with borrow in.
    ///
    /// Same as `isub` with an additional borrow flag input. Computes:
    ///
    /// ```text
    ///     a = x - (y + b_{in}) \pmod 2^B
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    /// - b_in: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn isub_ifbin(self, x: ir::Value, y: ir::Value, b_in: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(y);
        let (inst, dfg) = self.Ternary(Opcode::IsubIfbin, ctrl_typevar, x, y, b_in);
        dfg.first_result(inst)
    }

    /// Subtract integers with borrow out.
    ///
    /// Same as `isub` with an additional borrow flag output.
    ///
    /// ```text
    ///     a &= x - y \pmod 2^B \\
    ///     b_{out} &= x < y
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    /// - b_out: Output borrow flag
    #[allow(non_snake_case)]
    fn isub_bout(self, x: ir::Value, y: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::IsubBout, ctrl_typevar, x, y);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Subtract integers with borrow out.
    ///
    /// Same as `isub` with an additional borrow flag output.
    ///
    /// ```text
    ///     a &= x - y \pmod 2^B \\
    ///     b_{out} &= x < y
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    /// - b_out: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    #[allow(non_snake_case)]
    fn isub_ifbout(self, x: ir::Value, y: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::IsubIfbout, ctrl_typevar, x, y);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Subtract integers with borrow in and out.
    ///
    /// Same as `isub` with an additional borrow flag input and output.
    ///
    /// ```text
    ///     a &= x - (y + b_{in}) \pmod 2^B \\
    ///     b_{out} &= x < y + b_{in}
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    /// - b_in: Input borrow flag
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    /// - b_out: Output borrow flag
    #[allow(non_snake_case)]
    fn isub_borrow(self, x: ir::Value, y: ir::Value, b_in: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(y);
        let (inst, dfg) = self.Ternary(Opcode::IsubBorrow, ctrl_typevar, x, y, b_in);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Subtract integers with borrow in and out.
    ///
    /// Same as `isub` with an additional borrow flag input and output.
    ///
    /// ```text
    ///     a &= x - (y + b_{in}) \pmod 2^B \\
    ///     b_{out} &= x < y + b_{in}
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    /// - b_in: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    /// - b_out: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    #[allow(non_snake_case)]
    fn isub_ifborrow(self, x: ir::Value, y: ir::Value, b_in: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(y);
        let (inst, dfg) = self.Ternary(Opcode::IsubIfborrow, ctrl_typevar, x, y, b_in);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Bitwise and.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, or vector type
    /// - y: Any integer, float, or vector type
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, or vector type
    #[allow(non_snake_case)]
    fn band(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Band, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Bitwise or.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, or vector type
    /// - y: Any integer, float, or vector type
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, or vector type
    #[allow(non_snake_case)]
    fn bor(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Bor, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Bitwise xor.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, or vector type
    /// - y: Any integer, float, or vector type
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, or vector type
    #[allow(non_snake_case)]
    fn bxor(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Bxor, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Bitwise not.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, or vector type
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, or vector type
    #[allow(non_snake_case)]
    fn bnot(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Bnot, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Bitwise and not.
    ///
    /// Computes `x & ~y`.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, or vector type
    /// - y: Any integer, float, or vector type
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, or vector type
    #[allow(non_snake_case)]
    fn band_not(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::BandNot, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Bitwise or not.
    ///
    /// Computes `x | ~y`.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, or vector type
    /// - y: Any integer, float, or vector type
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, or vector type
    #[allow(non_snake_case)]
    fn bor_not(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::BorNot, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Bitwise xor not.
    ///
    /// Computes `x ^ ~y`.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, or vector type
    /// - y: Any integer, float, or vector type
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, or vector type
    #[allow(non_snake_case)]
    fn bxor_not(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::BxorNot, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Bitwise and with immediate.
    ///
    /// Same as `band`, but one operand is a zero extended 64 bit immediate constant.
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn band_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::BandImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Bitwise or with immediate.
    ///
    /// Same as `bor`, but one operand is a zero extended 64 bit immediate constant.
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn bor_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::BorImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Bitwise xor with immediate.
    ///
    /// Same as `bxor`, but one operand is a zero extended 64 bit immediate constant.
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn bxor_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::BxorImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Rotate left.
    ///
    /// Rotate the bits in ``x`` by ``y`` places.
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - y: Number of bits to shift
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn rotl(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Rotl, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Rotate right.
    ///
    /// Rotate the bits in ``x`` by ``y`` places.
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - y: Number of bits to shift
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn rotr(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Rotr, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Rotate left by immediate.
    ///
    /// Same as `rotl`, but one operand is a zero extended 64 bit immediate constant.
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn rotl_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::RotlImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Rotate right by immediate.
    ///
    /// Same as `rotr`, but one operand is a zero extended 64 bit immediate constant.
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn rotr_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::RotrImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Integer shift left. Shift the bits in ``x`` towards the MSB by ``y``
    /// places. Shift in zero bits to the LSB.
    ///
    /// The shift amount is masked to the size of ``x``.
    ///
    /// When shifting a B-bits integer type, this instruction computes:
    ///
    /// ```text
    ///     s &:= y \pmod B,
    ///     a &:= x \cdot 2^s \pmod{2^B}.
    /// ```
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - y: Number of bits to shift
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn ishl(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Ishl, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Unsigned shift right. Shift bits in ``x`` towards the LSB by ``y``
    /// places, shifting in zero bits to the MSB. Also called a *logical
    /// shift*.
    ///
    /// The shift amount is masked to the size of the register.
    ///
    /// When shifting a B-bits integer type, this instruction computes:
    ///
    /// ```text
    ///     s &:= y \pmod B,
    ///     a &:= \lfloor x \cdot 2^{-s} \rfloor.
    /// ```
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - y: Number of bits to shift
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn ushr(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Ushr, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Signed shift right. Shift bits in ``x`` towards the LSB by ``y``
    /// places, shifting in sign bits to the MSB. Also called an *arithmetic
    /// shift*.
    ///
    /// The shift amount is masked to the size of the register.
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - y: Number of bits to shift
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn sshr(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Sshr, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Integer shift left by immediate.
    ///
    /// The shift amount is masked to the size of ``x``.
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn ishl_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::IshlImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Unsigned shift right by immediate.
    ///
    /// The shift amount is masked to the size of the register.
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn ushr_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::UshrImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Signed shift right by immediate.
    ///
    /// The shift amount is masked to the size of the register.
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn sshr_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::SshrImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Reverse the bits of a integer.
    ///
    /// Reverses the bits in ``x``.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn bitrev(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Bitrev, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Count leading zero bits.
    ///
    /// Starting from the MSB in ``x``, count the number of zero bits before
    /// reaching the first one bit. When ``x`` is zero, returns the size of x
    /// in bits.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn clz(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Clz, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Count leading sign bits.
    ///
    /// Starting from the MSB after the sign bit in ``x``, count the number of
    /// consecutive bits identical to the sign bit. When ``x`` is 0 or -1,
    /// returns one less than the size of x in bits.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn cls(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Cls, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Count trailing zeros.
    ///
    /// Starting from the LSB in ``x``, count the number of zero bits before
    /// reaching the first one bit. When ``x`` is zero, returns the size of x
    /// in bits.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn ctz(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Ctz, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Reverse the byte order of an integer.
    ///
    /// Reverses the bytes in ``x``.
    ///
    /// Inputs:
    ///
    /// - x: A multi byte scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A multi byte scalar integer type
    #[allow(non_snake_case)]
    fn bswap(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Bswap, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Population count
    ///
    /// Count the number of one bits in ``x``.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn popcnt(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Popcnt, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Floating point comparison.
    ///
    /// Two IEEE 754-2008 floating point numbers, `x` and `y`, relate to each
    /// other in exactly one of four ways:
    ///
    /// ```text
    /// == ==========================================
    /// UN Unordered when one or both numbers is NaN.
    /// EQ When `x = y`. (And `0.0 = -0.0`).
    /// LT When `x < y`.
    /// GT When `x > y`.
    /// == ==========================================
    /// ```
    ///
    /// The 14 `floatcc` condition codes each correspond to a subset of
    /// the four relations, except for the empty set which would always be
    /// false, and the full set which would always be true.
    ///
    /// The condition codes are divided into 7 'ordered' conditions which don't
    /// include UN, and 7 unordered conditions which all include UN.
    ///
    /// ```text
    /// +-------+------------+---------+------------+-------------------------+
    /// |Ordered             |Unordered             |Condition                |
    /// +=======+============+=========+============+=========================+
    /// |ord    |EQ | LT | GT|uno      |UN          |NaNs absent / present.   |
    /// +-------+------------+---------+------------+-------------------------+
    /// |eq     |EQ          |ueq      |UN | EQ     |Equal                    |
    /// +-------+------------+---------+------------+-------------------------+
    /// |one    |LT | GT     |ne       |UN | LT | GT|Not equal                |
    /// +-------+------------+---------+------------+-------------------------+
    /// |lt     |LT          |ult      |UN | LT     |Less than                |
    /// +-------+------------+---------+------------+-------------------------+
    /// |le     |LT | EQ     |ule      |UN | LT | EQ|Less than or equal       |
    /// +-------+------------+---------+------------+-------------------------+
    /// |gt     |GT          |ugt      |UN | GT     |Greater than             |
    /// +-------+------------+---------+------------+-------------------------+
    /// |ge     |GT | EQ     |uge      |UN | GT | EQ|Greater than or equal    |
    /// +-------+------------+---------+------------+-------------------------+
    /// ```
    ///
    /// The standard C comparison operators, `<, <=, >, >=`, are all ordered,
    /// so they are false if either operand is NaN. The C equality operator,
    /// `==`, is ordered, and since inequality is defined as the logical
    /// inverse it is *unordered*. They map to the `floatcc` condition
    /// codes as follows:
    ///
    /// ```text
    /// ==== ====== ============
    /// C    `Cond` Subset
    /// ==== ====== ============
    /// `==` eq     EQ
    /// `!=` ne     UN | LT | GT
    /// `<`  lt     LT
    /// `<=` le     LT | EQ
    /// `>`  gt     GT
    /// `>=` ge     GT | EQ
    /// ==== ====== ============
    /// ```
    ///
    /// This subset of condition codes also corresponds to the WebAssembly
    /// floating point comparisons of the same name.
    ///
    /// When this instruction compares floating point vectors, it returns a
    /// vector with the results of lane-wise comparisons.
    ///
    /// Inputs:
    ///
    /// - Cond: A floating point comparison condition code
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn fcmp<T1: Into<ir::condcodes::FloatCC>>(self, Cond: T1, x: ir::Value, y: ir::Value) -> Value {
        let Cond = Cond.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.FloatCompare(Opcode::Fcmp, ctrl_typevar, Cond, x, y);
        dfg.first_result(inst)
    }

    /// Floating point comparison returning flags.
    ///
    /// Compares two numbers like `fcmp`, but returns floating point CPU
    /// flags instead of testing a specific condition.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - f: CPU flags representing the result of a floating point comparison. These
    /// flags can be tested with a :type:`floatcc` condition code.
    #[allow(non_snake_case)]
    fn ffcmp(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Ffcmp, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Floating point addition.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: Result of applying operator to each lane
    #[allow(non_snake_case)]
    fn fadd(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Fadd, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Floating point subtraction.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: Result of applying operator to each lane
    #[allow(non_snake_case)]
    fn fsub(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Fsub, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Floating point multiplication.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: Result of applying operator to each lane
    #[allow(non_snake_case)]
    fn fmul(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Fmul, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Floating point division.
    ///
    /// Unlike the integer division instructions ` and
    /// `udiv`, this can't trap. Division by zero is infinity or
    /// NaN, depending on the dividend.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: Result of applying operator to each lane
    #[allow(non_snake_case)]
    fn fdiv(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Fdiv, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Floating point square root.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: Result of applying operator to each lane
    #[allow(non_snake_case)]
    fn sqrt(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Sqrt, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Floating point fused multiply-and-add.
    ///
    /// Computes `a := xy+z` without any intermediate rounding of the
    /// product.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    /// - z: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: Result of applying operator to each lane
    #[allow(non_snake_case)]
    fn fma(self, x: ir::Value, y: ir::Value, z: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(y);
        let (inst, dfg) = self.Ternary(Opcode::Fma, ctrl_typevar, x, y, z);
        dfg.first_result(inst)
    }

    /// Floating point negation.
    ///
    /// Note that this is a pure bitwise operation.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: ``x`` with its sign bit inverted
    #[allow(non_snake_case)]
    fn fneg(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Fneg, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Floating point absolute value.
    ///
    /// Note that this is a pure bitwise operation.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: ``x`` with its sign bit cleared
    #[allow(non_snake_case)]
    fn fabs(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Fabs, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Floating point copy sign.
    ///
    /// Note that this is a pure bitwise operation. The sign bit from ``y`` is
    /// copied to the sign bit of ``x``.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: ``x`` with its sign bit changed to that of ``y``
    #[allow(non_snake_case)]
    fn fcopysign(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Fcopysign, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Floating point minimum, propagating NaNs using the WebAssembly rules.
    ///
    /// If either operand is NaN, this returns NaN with an unspecified sign. Furthermore, if
    /// each input NaN consists of a mantissa whose most significant bit is 1 and the rest is
    /// 0, then the output has the same form. Otherwise, the output mantissa's most significant
    /// bit is 1 and the rest is unspecified.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: The smaller of ``x`` and ``y``
    #[allow(non_snake_case)]
    fn fmin(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Fmin, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Floating point pseudo-minimum, propagating NaNs.  This behaves differently from ``fmin``.
    /// See <https://github.com/WebAssembly/simd/pull/122> for background.
    ///
    /// The behaviour is defined as ``fmin_pseudo(a, b) = (b < a) ? b : a``, and the behaviour
    /// for zero or NaN inputs follows from the behaviour of ``<`` with such inputs.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: The smaller of ``x`` and ``y``
    #[allow(non_snake_case)]
    fn fmin_pseudo(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::FminPseudo, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Floating point maximum, propagating NaNs using the WebAssembly rules.
    ///
    /// If either operand is NaN, this returns NaN with an unspecified sign. Furthermore, if
    /// each input NaN consists of a mantissa whose most significant bit is 1 and the rest is
    /// 0, then the output has the same form. Otherwise, the output mantissa's most significant
    /// bit is 1 and the rest is unspecified.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: The larger of ``x`` and ``y``
    #[allow(non_snake_case)]
    fn fmax(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Fmax, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Floating point pseudo-maximum, propagating NaNs.  This behaves differently from ``fmax``.
    /// See <https://github.com/WebAssembly/simd/pull/122> for background.
    ///
    /// The behaviour is defined as ``fmax_pseudo(a, b) = (a < b) ? b : a``, and the behaviour
    /// for zero or NaN inputs follows from the behaviour of ``<`` with such inputs.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: The larger of ``x`` and ``y``
    #[allow(non_snake_case)]
    fn fmax_pseudo(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::FmaxPseudo, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Round floating point round to integral, towards positive infinity.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: ``x`` rounded to integral value
    #[allow(non_snake_case)]
    fn ceil(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Ceil, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Round floating point round to integral, towards negative infinity.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: ``x`` rounded to integral value
    #[allow(non_snake_case)]
    fn floor(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Floor, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Round floating point round to integral, towards zero.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: ``x`` rounded to integral value
    #[allow(non_snake_case)]
    fn trunc(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Trunc, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Round floating point round to integral, towards nearest with ties to
    /// even.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: ``x`` rounded to integral value
    #[allow(non_snake_case)]
    fn nearest(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Nearest, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Reference verification.
    ///
    /// The condition code determines if the reference type in question is
    /// null or not.
    ///
    /// Inputs:
    ///
    /// - x: A scalar reference type
    ///
    /// Outputs:
    ///
    /// - a: An integer type with 8 bits.
    /// WARNING: arithmetic on 8bit integers is incomplete
    #[allow(non_snake_case)]
    fn is_null(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::IsNull, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Reference verification.
    ///
    /// The condition code determines if the reference type in question is
    /// invalid or not.
    ///
    /// Inputs:
    ///
    /// - x: A scalar reference type
    ///
    /// Outputs:
    ///
    /// - a: An integer type with 8 bits.
    /// WARNING: arithmetic on 8bit integers is incomplete
    #[allow(non_snake_case)]
    fn is_invalid(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::IsInvalid, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Reinterpret the bits in `x` as a different type.
    ///
    /// The input and output types must be storable to memory and of the same
    /// size. A bitcast is equivalent to storing one type and loading the other
    /// type from the same address, both using the specified MemFlags.
    ///
    /// Note that this operation only supports the `big` or `little` MemFlags.
    /// The specified byte order only affects the result in the case where
    /// input and output types differ in lane count/size.  In this case, the
    /// operation is only valid if a byte order specifier is provided.
    ///
    /// Inputs:
    ///
    /// - MemTo (controlling type variable):
    /// - MemFlags: Memory operation flags
    /// - x: Any type that can be stored in memory
    ///
    /// Outputs:
    ///
    /// - a: Bits of `x` reinterpreted
    #[allow(non_snake_case)]
    fn bitcast<T1: Into<ir::MemFlags>>(self, MemTo: crate::ir::Type, MemFlags: T1, x: ir::Value) -> Value {
        let MemFlags = MemFlags.into();
        let (inst, dfg) = self.LoadNoOffset(Opcode::Bitcast, MemTo, MemFlags, x);
        dfg.first_result(inst)
    }

    /// Copies a scalar value to a vector value.  The scalar is copied into the
    /// least significant lane of the vector, and all other lanes will be zero.
    ///
    /// Inputs:
    ///
    /// - TxN (controlling type variable): A SIMD vector type
    /// - s: A scalar value
    ///
    /// Outputs:
    ///
    /// - a: A vector value
    #[allow(non_snake_case)]
    fn scalar_to_vector(self, TxN: crate::ir::Type, s: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::ScalarToVector, TxN, s);
        dfg.first_result(inst)
    }

    /// Convert `x` to an integer mask.
    ///
    /// True maps to all 1s and false maps to all 0s. The result type must have
    /// the same number of vector lanes as the input.
    ///
    /// Inputs:
    ///
    /// - IntTo (controlling type variable): An integer type with the same number of lanes
    /// - x: A scalar or vector whose values are truthy
    ///
    /// Outputs:
    ///
    /// - a: An integer type with the same number of lanes
    #[allow(non_snake_case)]
    fn bmask(self, IntTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::Bmask, IntTo, x);
        dfg.first_result(inst)
    }

    /// Convert `x` to a smaller integer type by discarding
    /// the most significant bits.
    ///
    /// This is the same as reducing modulo `2^n`.
    ///
    /// Inputs:
    ///
    /// - IntTo (controlling type variable): A smaller integer type
    /// - x: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A smaller integer type
    #[allow(non_snake_case)]
    fn ireduce(self, IntTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::Ireduce, IntTo, x);
        dfg.first_result(inst)
    }

    /// Combine `x` and `y` into a vector with twice the lanes but half the integer width while
    /// saturating overflowing values to the signed maximum and minimum.
    ///
    /// The lanes will be concatenated after narrowing. For example, when `x` and `y` are `i32x4`
    /// and `x = [x3, x2, x1, x0]` and `y = [y3, y2, y1, y0]`, then after narrowing the value
    /// returned is an `i16x8`: `a = [y3', y2', y1', y0', x3', x2', x1', x0']`.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integer lanes 16, 32, or 64 bits wide
    /// - y: A SIMD vector type containing integer lanes 16, 32, or 64 bits wide
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn snarrow(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Snarrow, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Combine `x` and `y` into a vector with twice the lanes but half the integer width while
    /// saturating overflowing values to the unsigned maximum and minimum.
    ///
    /// Note that all input lanes are considered signed: any negative lanes will overflow and be
    /// replaced with the unsigned minimum, `0x00`.
    ///
    /// The lanes will be concatenated after narrowing. For example, when `x` and `y` are `i32x4`
    /// and `x = [x3, x2, x1, x0]` and `y = [y3, y2, y1, y0]`, then after narrowing the value
    /// returned is an `i16x8`: `a = [y3', y2', y1', y0', x3', x2', x1', x0']`.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integer lanes 16, 32, or 64 bits wide
    /// - y: A SIMD vector type containing integer lanes 16, 32, or 64 bits wide
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn unarrow(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Unarrow, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Combine `x` and `y` into a vector with twice the lanes but half the integer width while
    /// saturating overflowing values to the unsigned maximum and minimum.
    ///
    /// Note that all input lanes are considered unsigned: any negative values will be interpreted as unsigned, overflowing and being replaced with the unsigned maximum.
    ///
    /// The lanes will be concatenated after narrowing. For example, when `x` and `y` are `i32x4`
    /// and `x = [x3, x2, x1, x0]` and `y = [y3, y2, y1, y0]`, then after narrowing the value
    /// returned is an `i16x8`: `a = [y3', y2', y1', y0', x3', x2', x1', x0']`.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integer lanes 16, 32, or 64 bits wide
    /// - y: A SIMD vector type containing integer lanes 16, 32, or 64 bits wide
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn uunarrow(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Uunarrow, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Widen the low lanes of `x` using signed extension.
    ///
    /// This will double the lane width and halve the number of lanes.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integer lanes 8, 16, or 32 bits wide.
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn swiden_low(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::SwidenLow, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Widen the high lanes of `x` using signed extension.
    ///
    /// This will double the lane width and halve the number of lanes.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integer lanes 8, 16, or 32 bits wide.
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn swiden_high(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::SwidenHigh, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Widen the low lanes of `x` using unsigned extension.
    ///
    /// This will double the lane width and halve the number of lanes.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integer lanes 8, 16, or 32 bits wide.
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn uwiden_low(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::UwidenLow, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Widen the high lanes of `x` using unsigned extension.
    ///
    /// This will double the lane width and halve the number of lanes.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integer lanes 8, 16, or 32 bits wide.
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn uwiden_high(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::UwidenHigh, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Does lane-wise integer pairwise addition on two operands, putting the
    /// combined results into a single vector result. Here a pair refers to adjacent
    /// lanes in a vector, i.e. i*2 + (i*2+1) for i == num_lanes/2. The first operand
    /// pairwise add results will make up the low half of the resulting vector while
    /// the second operand pairwise add results will make up the upper half of the
    /// resulting vector.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integer lanes 8, 16, or 32 bits wide.
    /// - y: A SIMD vector type containing integer lanes 8, 16, or 32 bits wide.
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type containing integer lanes 8, 16, or 32 bits wide.
    #[allow(non_snake_case)]
    fn iadd_pairwise(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::IaddPairwise, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Takes corresponding elements in `x` and `y`, performs a sign-extending length-doubling
    /// multiplication on them, then adds adjacent pairs of elements to form the result.  For
    /// example, if the input vectors are `[x3, x2, x1, x0]` and `[y3, y2, y1, y0]`, it produces
    /// the vector `[r1, r0]`, where `r1 = sx(x3) * sx(y3) + sx(x2) * sx(y2)` and
    /// `r0 = sx(x1) * sx(y1) + sx(x0) * sx(y0)`, and `sx(n)` sign-extends `n` to twice its width.
    ///
    /// This will double the lane width and halve the number of lanes.  So the resulting
    /// vector has the same number of bits as `x` and `y` do (individually).
    ///
    /// See <https://github.com/WebAssembly/simd/pull/127> for background info.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing 8 integer lanes each 16 bits wide.
    /// - y: A SIMD vector type containing 8 integer lanes each 16 bits wide.
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn widening_pairwise_dot_product_s(self, x: ir::Value, y: ir::Value) -> Value {
        let (inst, dfg) = self.Binary(Opcode::WideningPairwiseDotProductS, types::INVALID, x, y);
        dfg.first_result(inst)
    }

    /// Convert `x` to a larger integer type by zero-extending.
    ///
    /// Each lane in `x` is converted to a larger integer type by adding
    /// zeroes. The result has the same numerical value as `x` when both are
    /// interpreted as unsigned integers.
    ///
    /// The result type must have the same number of vector lanes as the input,
    /// and each lane must not have fewer bits that the input lanes. If the
    /// input and output types are the same, this is a no-op.
    ///
    /// Inputs:
    ///
    /// - IntTo (controlling type variable): A larger integer type with the same number of lanes
    /// - x: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A larger integer type with the same number of lanes
    #[allow(non_snake_case)]
    fn uextend(self, IntTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::Uextend, IntTo, x);
        dfg.first_result(inst)
    }

    /// Convert `x` to a larger integer type by sign-extending.
    ///
    /// Each lane in `x` is converted to a larger integer type by replicating
    /// the sign bit. The result has the same numerical value as `x` when both
    /// are interpreted as signed integers.
    ///
    /// The result type must have the same number of vector lanes as the input,
    /// and each lane must not have fewer bits that the input lanes. If the
    /// input and output types are the same, this is a no-op.
    ///
    /// Inputs:
    ///
    /// - IntTo (controlling type variable): A larger integer type with the same number of lanes
    /// - x: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A larger integer type with the same number of lanes
    #[allow(non_snake_case)]
    fn sextend(self, IntTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::Sextend, IntTo, x);
        dfg.first_result(inst)
    }

    /// Convert `x` to a larger floating point format.
    ///
    /// Each lane in `x` is converted to the destination floating point format.
    /// This is an exact operation.
    ///
    /// Cranelift currently only supports two floating point formats
    /// - `f32` and `f64`. This may change in the future.
    ///
    /// The result type must have the same number of vector lanes as the input,
    /// and the result lanes must not have fewer bits than the input lanes.
    ///
    /// Inputs:
    ///
    /// - FloatTo (controlling type variable): A scalar or vector floating point number
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector floating point number
    #[allow(non_snake_case)]
    fn fpromote(self, FloatTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::Fpromote, FloatTo, x);
        dfg.first_result(inst)
    }

    /// Convert `x` to a smaller floating point format.
    ///
    /// Each lane in `x` is converted to the destination floating point format
    /// by rounding to nearest, ties to even.
    ///
    /// Cranelift currently only supports two floating point formats
    /// - `f32` and `f64`. This may change in the future.
    ///
    /// The result type must have the same number of vector lanes as the input,
    /// and the result lanes must not have more bits than the input lanes.
    ///
    /// Inputs:
    ///
    /// - FloatTo (controlling type variable): A scalar or vector floating point number
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector floating point number
    #[allow(non_snake_case)]
    fn fdemote(self, FloatTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::Fdemote, FloatTo, x);
        dfg.first_result(inst)
    }

    /// Convert `x` to a smaller floating point format.
    ///
    /// Each lane in `x` is converted to the destination floating point format
    /// by rounding to nearest, ties to even.
    ///
    /// Cranelift currently only supports two floating point formats
    /// - `f32` and `f64`. This may change in the future.
    ///
    /// Fvdemote differs from fdemote in that with fvdemote it targets vectors.
    /// Fvdemote is constrained to having the input type being F64x2 and the result
    /// type being F32x4. The result lane that was the upper half of the input lane
    /// is initialized to zero.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type consisting of 2 lanes of 64-bit floats
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type consisting of 4 lanes of 32-bit floats
    #[allow(non_snake_case)]
    fn fvdemote(self, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::Fvdemote, types::INVALID, x);
        dfg.first_result(inst)
    }

    /// Converts packed single precision floating point to packed double precision floating point.
    ///
    /// Considering only the lower half of the register, the low lanes in `x` are interpreted as
    /// single precision floats that are then converted to a double precision floats.
    ///
    /// The result type will have half the number of vector lanes as the input. Fvpromote_low is
    /// constrained to input F32x4 with a result type of F64x2.
    ///
    /// Inputs:
    ///
    /// - a: A SIMD vector type consisting of 4 lanes of 32-bit floats
    ///
    /// Outputs:
    ///
    /// - x: A SIMD vector type consisting of 2 lanes of 64-bit floats
    #[allow(non_snake_case)]
    fn fvpromote_low(self, a: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::FvpromoteLow, types::INVALID, a);
        dfg.first_result(inst)
    }

    /// Converts floating point scalars to unsigned integer.
    ///
    /// Only operates on `x` if it is a scalar. If `x` is NaN or if
    /// the unsigned integral value cannot be represented in the result
    /// type, this instruction traps.
    ///
    /// Inputs:
    ///
    /// - IntTo (controlling type variable): A larger integer type with the same number of lanes
    /// - x: A scalar only floating point number
    ///
    /// Outputs:
    ///
    /// - a: A larger integer type with the same number of lanes
    #[allow(non_snake_case)]
    fn fcvt_to_uint(self, IntTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::FcvtToUint, IntTo, x);
        dfg.first_result(inst)
    }

    /// Converts floating point scalars to signed integer.
    ///
    /// Only operates on `x` if it is a scalar. If `x` is NaN or if
    /// the unsigned integral value cannot be represented in the result
    /// type, this instruction traps.
    ///
    /// Inputs:
    ///
    /// - IntTo (controlling type variable): A larger integer type with the same number of lanes
    /// - x: A scalar only floating point number
    ///
    /// Outputs:
    ///
    /// - a: A larger integer type with the same number of lanes
    #[allow(non_snake_case)]
    fn fcvt_to_sint(self, IntTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::FcvtToSint, IntTo, x);
        dfg.first_result(inst)
    }

    /// Convert floating point to unsigned integer as fcvt_to_uint does, but
    /// saturates the input instead of trapping. NaN and negative values are
    /// converted to 0.
    ///
    /// Inputs:
    ///
    /// - IntTo (controlling type variable): A larger integer type with the same number of lanes
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: A larger integer type with the same number of lanes
    #[allow(non_snake_case)]
    fn fcvt_to_uint_sat(self, IntTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::FcvtToUintSat, IntTo, x);
        dfg.first_result(inst)
    }

    /// Convert floating point to signed integer as fcvt_to_sint does, but
    /// saturates the input instead of trapping. NaN values are converted to 0.
    ///
    /// Inputs:
    ///
    /// - IntTo (controlling type variable): A larger integer type with the same number of lanes
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: A larger integer type with the same number of lanes
    #[allow(non_snake_case)]
    fn fcvt_to_sint_sat(self, IntTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::FcvtToSintSat, IntTo, x);
        dfg.first_result(inst)
    }

    /// Convert unsigned integer to floating point.
    ///
    /// Each lane in `x` is interpreted as an unsigned integer and converted to
    /// floating point using round to nearest, ties to even.
    ///
    /// The result type must have the same number of vector lanes as the input.
    ///
    /// Inputs:
    ///
    /// - FloatTo (controlling type variable): A scalar or vector floating point number
    /// - x: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector floating point number
    #[allow(non_snake_case)]
    fn fcvt_from_uint(self, FloatTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::FcvtFromUint, FloatTo, x);
        dfg.first_result(inst)
    }

    /// Convert signed integer to floating point.
    ///
    /// Each lane in `x` is interpreted as a signed integer and converted to
    /// floating point using round to nearest, ties to even.
    ///
    /// The result type must have the same number of vector lanes as the input.
    ///
    /// Inputs:
    ///
    /// - FloatTo (controlling type variable): A scalar or vector floating point number
    /// - x: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector floating point number
    #[allow(non_snake_case)]
    fn fcvt_from_sint(self, FloatTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::FcvtFromSint, FloatTo, x);
        dfg.first_result(inst)
    }

    /// Converts packed signed 32-bit integers to packed double precision floating point.
    ///
    /// Considering only the low half of the register, each lane in `x` is interpreted as a
    /// signed 32-bit integer that is then converted to a double precision float. This
    /// instruction differs from fcvt_from_sint in that it converts half the number of lanes
    /// which are converted to occupy twice the number of bits. No rounding should be needed
    /// for the resulting float.
    ///
    /// The result type will have half the number of vector lanes as the input.
    ///
    /// Inputs:
    ///
    /// - FloatTo (controlling type variable): A scalar or vector floating point number
    /// - x: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector floating point number
    #[allow(non_snake_case)]
    fn fcvt_low_from_sint(self, FloatTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::FcvtLowFromSint, FloatTo, x);
        dfg.first_result(inst)
    }

    /// Split an integer into low and high parts.
    ///
    /// Vectors of integers are split lane-wise, so the results have the same
    /// number of lanes as the input, but the lanes are half the size.
    ///
    /// Returns the low half of `x` and the high half of `x` as two independent
    /// values.
    ///
    /// Inputs:
    ///
    /// - x: An integer type with lanes from `i16` upwards
    ///
    /// Outputs:
    ///
    /// - lo: The low bits of `x`
    /// - hi: The high bits of `x`
    #[allow(non_snake_case)]
    fn isplit(self, x: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Isplit, ctrl_typevar, x);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Concatenate low and high bits to form a larger integer type.
    ///
    /// Vectors of integers are concatenated lane-wise such that the result has
    /// the same number of lanes as the inputs, but the lanes are twice the
    /// size.
    ///
    /// Inputs:
    ///
    /// - lo: An integer type with lanes type to `i64`
    /// - hi: An integer type with lanes type to `i64`
    ///
    /// Outputs:
    ///
    /// - a: The concatenation of `lo` and `hi`
    #[allow(non_snake_case)]
    fn iconcat(self, lo: ir::Value, hi: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(lo);
        let (inst, dfg) = self.Binary(Opcode::Iconcat, ctrl_typevar, lo, hi);
        dfg.first_result(inst)
    }

    /// Atomically read-modify-write memory at `p`, with second operand `x`.  The old value is
    /// returned.  `p` has the type of the target word size, and `x` may be an integer type of
    /// 8, 16, 32 or 64 bits, even on a 32-bit target.  The type of the returned value is the
    /// same as the type of `x`.  This operation is sequentially consistent and creates
    /// happens-before edges that order normal (non-atomic) loads and stores.
    ///
    /// Inputs:
    ///
    /// - AtomicMem (controlling type variable): Any type that can be stored in memory, which can be used in an atomic operation
    /// - MemFlags: Memory operation flags
    /// - AtomicRmwOp: Atomic Read-Modify-Write Ops
    /// - p: An integer address type
    /// - x: Value to be atomically stored
    ///
    /// Outputs:
    ///
    /// - a: Value atomically loaded
    #[allow(non_snake_case)]
    fn atomic_rmw<T1: Into<ir::MemFlags>, T2: Into<ir::AtomicRmwOp>>(self, AtomicMem: crate::ir::Type, MemFlags: T1, AtomicRmwOp: T2, p: ir::Value, x: ir::Value) -> Value {
        let MemFlags = MemFlags.into();
        let AtomicRmwOp = AtomicRmwOp.into();
        let (inst, dfg) = self.AtomicRmw(Opcode::AtomicRmw, AtomicMem, MemFlags, AtomicRmwOp, p, x);
        dfg.first_result(inst)
    }

    /// Perform an atomic compare-and-swap operation on memory at `p`, with expected value `e`,
    /// storing `x` if the value at `p` equals `e`.  The old value at `p` is returned,
    /// regardless of whether the operation succeeds or fails.  `p` has the type of the target
    /// word size, and `x` and `e` must have the same type and the same size, which may be an
    /// integer type of 8, 16, 32 or 64 bits, even on a 32-bit target.  The type of the returned
    /// value is the same as the type of `x` and `e`.  This operation is sequentially
    /// consistent and creates happens-before edges that order normal (non-atomic) loads and
    /// stores.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - e: Expected value in CAS
    /// - x: Value to be atomically stored
    ///
    /// Outputs:
    ///
    /// - a: Value atomically loaded
    #[allow(non_snake_case)]
    fn atomic_cas<T1: Into<ir::MemFlags>>(self, MemFlags: T1, p: ir::Value, e: ir::Value, x: ir::Value) -> Value {
        let MemFlags = MemFlags.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.AtomicCas(Opcode::AtomicCas, ctrl_typevar, MemFlags, p, e, x);
        dfg.first_result(inst)
    }

    /// Atomically load from memory at `p`.
    ///
    /// This is a polymorphic instruction that can load any value type which has a memory
    /// representation.  It should only be used for integer types with 8, 16, 32 or 64 bits.
    /// This operation is sequentially consistent and creates happens-before edges that order
    /// normal (non-atomic) loads and stores.
    ///
    /// Inputs:
    ///
    /// - AtomicMem (controlling type variable): Any type that can be stored in memory, which can be used in an atomic operation
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    ///
    /// Outputs:
    ///
    /// - a: Value atomically loaded
    #[allow(non_snake_case)]
    fn atomic_load<T1: Into<ir::MemFlags>>(self, AtomicMem: crate::ir::Type, MemFlags: T1, p: ir::Value) -> Value {
        let MemFlags = MemFlags.into();
        let (inst, dfg) = self.LoadNoOffset(Opcode::AtomicLoad, AtomicMem, MemFlags, p);
        dfg.first_result(inst)
    }

    /// Atomically store `x` to memory at `p`.
    ///
    /// This is a polymorphic instruction that can store any value type with a memory
    /// representation.  It should only be used for integer types with 8, 16, 32 or 64 bits.
    /// This operation is sequentially consistent and creates happens-before edges that order
    /// normal (non-atomic) loads and stores.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - x: Value to be atomically stored
    /// - p: An integer address type
    #[allow(non_snake_case)]
    fn atomic_store<T1: Into<ir::MemFlags>>(self, MemFlags: T1, x: ir::Value, p: ir::Value) -> Inst {
        let MemFlags = MemFlags.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        self.StoreNoOffset(Opcode::AtomicStore, ctrl_typevar, MemFlags, x, p).0
    }

    /// A memory fence.  This must provide ordering to ensure that, at a minimum, neither loads
    /// nor stores of any kind may move forwards or backwards across the fence.  This operation
    /// is sequentially consistent.
    #[allow(non_snake_case)]
    fn fence(self) -> Inst {
        self.NullAry(Opcode::Fence, types::INVALID).0
    }

    /// Return a fixed length sub vector, extracted from a dynamic vector.
    ///
    /// Inputs:
    ///
    /// - x: The dynamic vector to extract from
    /// - y: 128-bit vector index
    ///
    /// Outputs:
    ///
    /// - a: New fixed vector
    #[allow(non_snake_case)]
    fn extract_vector<T1: Into<ir::immediates::Uimm8>>(self, x: ir::Value, y: T1) -> Value {
        let y = y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm8(Opcode::ExtractVector, ctrl_typevar, y, x);
        dfg.first_result(inst)
    }

    /// AtomicCas(imms=(flags: ir::MemFlags), vals=3)
    #[allow(non_snake_case)]
    fn AtomicCas(self, opcode: Opcode, ctrl_typevar: Type, flags: ir::MemFlags, arg0: Value, arg1: Value, arg2: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::AtomicCas {
            opcode,
            flags,
            args: [arg0, arg1, arg2],
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// AtomicRmw(imms=(flags: ir::MemFlags, op: ir::AtomicRmwOp), vals=2)
    #[allow(non_snake_case)]
    fn AtomicRmw(self, opcode: Opcode, ctrl_typevar: Type, flags: ir::MemFlags, op: ir::AtomicRmwOp, arg0: Value, arg1: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::AtomicRmw {
            opcode,
            flags,
            op,
            args: [arg0, arg1],
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// Binary(imms=(), vals=2)
    #[allow(non_snake_case)]
    fn Binary(self, opcode: Opcode, ctrl_typevar: Type, arg0: Value, arg1: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Binary {
            opcode,
            args: [arg0, arg1],
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// BinaryImm64(imms=(imm: ir::immediates::Imm64), vals=1)
    #[allow(non_snake_case)]
    fn BinaryImm64(self, opcode: Opcode, ctrl_typevar: Type, imm: ir::immediates::Imm64, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let mut data = ir::InstructionData::BinaryImm64 {
            opcode,
            imm,
            arg: arg0,
        };
        data.sign_extend_immediates(ctrl_typevar);
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// BinaryImm8(imms=(imm: ir::immediates::Uimm8), vals=1)
    #[allow(non_snake_case)]
    fn BinaryImm8(self, opcode: Opcode, ctrl_typevar: Type, imm: ir::immediates::Uimm8, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::BinaryImm8 {
            opcode,
            imm,
            arg: arg0,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// Branch(imms=(destination: ir::Block), vals=1)
    #[allow(non_snake_case)]
    fn Branch(self, opcode: Opcode, ctrl_typevar: Type, destination: ir::Block, args: ir::ValueList) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Branch {
            opcode,
            destination,
            args,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// BranchTable(imms=(destination: ir::Block, table: ir::JumpTable), vals=1)
    #[allow(non_snake_case)]
    fn BranchTable(self, opcode: Opcode, ctrl_typevar: Type, destination: ir::Block, table: ir::JumpTable, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::BranchTable {
            opcode,
            destination,
            table,
            arg: arg0,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// Call(imms=(func_ref: ir::FuncRef), vals=0)
    #[allow(non_snake_case)]
    fn Call(self, opcode: Opcode, ctrl_typevar: Type, func_ref: ir::FuncRef, args: ir::ValueList) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Call {
            opcode,
            func_ref,
            args,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// CallIndirect(imms=(sig_ref: ir::SigRef), vals=1)
    #[allow(non_snake_case)]
    fn CallIndirect(self, opcode: Opcode, ctrl_typevar: Type, sig_ref: ir::SigRef, args: ir::ValueList) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::CallIndirect {
            opcode,
            sig_ref,
            args,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// CondTrap(imms=(code: ir::TrapCode), vals=1)
    #[allow(non_snake_case)]
    fn CondTrap(self, opcode: Opcode, ctrl_typevar: Type, code: ir::TrapCode, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::CondTrap {
            opcode,
            code,
            arg: arg0,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// DynamicStackLoad(imms=(dynamic_stack_slot: ir::DynamicStackSlot), vals=0)
    #[allow(non_snake_case)]
    fn DynamicStackLoad(self, opcode: Opcode, ctrl_typevar: Type, dynamic_stack_slot: ir::DynamicStackSlot) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::DynamicStackLoad {
            opcode,
            dynamic_stack_slot,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// DynamicStackStore(imms=(dynamic_stack_slot: ir::DynamicStackSlot), vals=1)
    #[allow(non_snake_case)]
    fn DynamicStackStore(self, opcode: Opcode, ctrl_typevar: Type, dynamic_stack_slot: ir::DynamicStackSlot, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::DynamicStackStore {
            opcode,
            dynamic_stack_slot,
            arg: arg0,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// FloatCompare(imms=(cond: ir::condcodes::FloatCC), vals=2)
    #[allow(non_snake_case)]
    fn FloatCompare(self, opcode: Opcode, ctrl_typevar: Type, cond: ir::condcodes::FloatCC, arg0: Value, arg1: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::FloatCompare {
            opcode,
            cond,
            args: [arg0, arg1],
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// FuncAddr(imms=(func_ref: ir::FuncRef), vals=0)
    #[allow(non_snake_case)]
    fn FuncAddr(self, opcode: Opcode, ctrl_typevar: Type, func_ref: ir::FuncRef) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::FuncAddr {
            opcode,
            func_ref,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// HeapAddr(imms=(heap: ir::Heap, offset: ir::immediates::Uimm32, size: ir::immediates::Uimm8), vals=1)
    #[allow(non_snake_case)]
    fn HeapAddr(self, opcode: Opcode, ctrl_typevar: Type, heap: ir::Heap, offset: ir::immediates::Uimm32, size: ir::immediates::Uimm8, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::HeapAddr {
            opcode,
            heap,
            offset,
            size,
            arg: arg0,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// HeapLoad(imms=(heap_imm: ir::HeapImm), vals=1)
    #[allow(non_snake_case)]
    fn HeapLoad(self, opcode: Opcode, ctrl_typevar: Type, heap_imm: ir::HeapImm, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::HeapLoad {
            opcode,
            heap_imm,
            arg: arg0,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// HeapStore(imms=(heap_imm: ir::HeapImm), vals=2)
    #[allow(non_snake_case)]
    fn HeapStore(self, opcode: Opcode, ctrl_typevar: Type, heap_imm: ir::HeapImm, arg0: Value, arg1: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::HeapStore {
            opcode,
            heap_imm,
            args: [arg0, arg1],
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// IntAddTrap(imms=(code: ir::TrapCode), vals=2)
    #[allow(non_snake_case)]
    fn IntAddTrap(self, opcode: Opcode, ctrl_typevar: Type, code: ir::TrapCode, arg0: Value, arg1: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::IntAddTrap {
            opcode,
            code,
            args: [arg0, arg1],
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// IntCompare(imms=(cond: ir::condcodes::IntCC), vals=2)
    #[allow(non_snake_case)]
    fn IntCompare(self, opcode: Opcode, ctrl_typevar: Type, cond: ir::condcodes::IntCC, arg0: Value, arg1: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::IntCompare {
            opcode,
            cond,
            args: [arg0, arg1],
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// IntCompareImm(imms=(cond: ir::condcodes::IntCC, imm: ir::immediates::Imm64), vals=1)
    #[allow(non_snake_case)]
    fn IntCompareImm(self, opcode: Opcode, ctrl_typevar: Type, cond: ir::condcodes::IntCC, imm: ir::immediates::Imm64, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let mut data = ir::InstructionData::IntCompareImm {
            opcode,
            cond,
            imm,
            arg: arg0,
        };
        data.sign_extend_immediates(ctrl_typevar);
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// Jump(imms=(destination: ir::Block), vals=0)
    #[allow(non_snake_case)]
    fn Jump(self, opcode: Opcode, ctrl_typevar: Type, destination: ir::Block, args: ir::ValueList) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Jump {
            opcode,
            destination,
            args,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// Load(imms=(flags: ir::MemFlags, offset: ir::immediates::Offset32), vals=1)
    #[allow(non_snake_case)]
    fn Load(self, opcode: Opcode, ctrl_typevar: Type, flags: ir::MemFlags, offset: ir::immediates::Offset32, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Load {
            opcode,
            flags,
            offset,
            arg: arg0,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// LoadNoOffset(imms=(flags: ir::MemFlags), vals=1)
    #[allow(non_snake_case)]
    fn LoadNoOffset(self, opcode: Opcode, ctrl_typevar: Type, flags: ir::MemFlags, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::LoadNoOffset {
            opcode,
            flags,
            arg: arg0,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// MultiAry(imms=(), vals=0)
    #[allow(non_snake_case)]
    fn MultiAry(self, opcode: Opcode, ctrl_typevar: Type, args: ir::ValueList) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::MultiAry {
            opcode,
            args,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// NullAry(imms=(), vals=0)
    #[allow(non_snake_case)]
    fn NullAry(self, opcode: Opcode, ctrl_typevar: Type) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::NullAry {
            opcode,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// Shuffle(imms=(imm: ir::Immediate), vals=2)
    #[allow(non_snake_case)]
    fn Shuffle(self, opcode: Opcode, ctrl_typevar: Type, imm: ir::Immediate, arg0: Value, arg1: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Shuffle {
            opcode,
            imm,
            args: [arg0, arg1],
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// StackLoad(imms=(stack_slot: ir::StackSlot, offset: ir::immediates::Offset32), vals=0)
    #[allow(non_snake_case)]
    fn StackLoad(self, opcode: Opcode, ctrl_typevar: Type, stack_slot: ir::StackSlot, offset: ir::immediates::Offset32) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::StackLoad {
            opcode,
            stack_slot,
            offset,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// StackStore(imms=(stack_slot: ir::StackSlot, offset: ir::immediates::Offset32), vals=1)
    #[allow(non_snake_case)]
    fn StackStore(self, opcode: Opcode, ctrl_typevar: Type, stack_slot: ir::StackSlot, offset: ir::immediates::Offset32, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::StackStore {
            opcode,
            stack_slot,
            offset,
            arg: arg0,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// Store(imms=(flags: ir::MemFlags, offset: ir::immediates::Offset32), vals=2)
    #[allow(non_snake_case)]
    fn Store(self, opcode: Opcode, ctrl_typevar: Type, flags: ir::MemFlags, offset: ir::immediates::Offset32, arg0: Value, arg1: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Store {
            opcode,
            flags,
            offset,
            args: [arg0, arg1],
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// StoreNoOffset(imms=(flags: ir::MemFlags), vals=2)
    #[allow(non_snake_case)]
    fn StoreNoOffset(self, opcode: Opcode, ctrl_typevar: Type, flags: ir::MemFlags, arg0: Value, arg1: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::StoreNoOffset {
            opcode,
            flags,
            args: [arg0, arg1],
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// TableAddr(imms=(table: ir::Table, offset: ir::immediates::Offset32), vals=1)
    #[allow(non_snake_case)]
    fn TableAddr(self, opcode: Opcode, ctrl_typevar: Type, table: ir::Table, offset: ir::immediates::Offset32, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::TableAddr {
            opcode,
            table,
            offset,
            arg: arg0,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// Ternary(imms=(), vals=3)
    #[allow(non_snake_case)]
    fn Ternary(self, opcode: Opcode, ctrl_typevar: Type, arg0: Value, arg1: Value, arg2: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Ternary {
            opcode,
            args: [arg0, arg1, arg2],
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// TernaryImm8(imms=(imm: ir::immediates::Uimm8), vals=2)
    #[allow(non_snake_case)]
    fn TernaryImm8(self, opcode: Opcode, ctrl_typevar: Type, imm: ir::immediates::Uimm8, arg0: Value, arg1: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::TernaryImm8 {
            opcode,
            imm,
            args: [arg0, arg1],
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// Trap(imms=(code: ir::TrapCode), vals=0)
    #[allow(non_snake_case)]
    fn Trap(self, opcode: Opcode, ctrl_typevar: Type, code: ir::TrapCode) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Trap {
            opcode,
            code,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// Unary(imms=(), vals=1)
    #[allow(non_snake_case)]
    fn Unary(self, opcode: Opcode, ctrl_typevar: Type, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Unary {
            opcode,
            arg: arg0,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// UnaryConst(imms=(constant_handle: ir::Constant), vals=0)
    #[allow(non_snake_case)]
    fn UnaryConst(self, opcode: Opcode, ctrl_typevar: Type, constant_handle: ir::Constant) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::UnaryConst {
            opcode,
            constant_handle,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// UnaryGlobalValue(imms=(global_value: ir::GlobalValue), vals=0)
    #[allow(non_snake_case)]
    fn UnaryGlobalValue(self, opcode: Opcode, ctrl_typevar: Type, global_value: ir::GlobalValue) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::UnaryGlobalValue {
            opcode,
            global_value,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// UnaryIeee32(imms=(imm: ir::immediates::Ieee32), vals=0)
    #[allow(non_snake_case)]
    fn UnaryIeee32(self, opcode: Opcode, ctrl_typevar: Type, imm: ir::immediates::Ieee32) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::UnaryIeee32 {
            opcode,
            imm,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// UnaryIeee64(imms=(imm: ir::immediates::Ieee64), vals=0)
    #[allow(non_snake_case)]
    fn UnaryIeee64(self, opcode: Opcode, ctrl_typevar: Type, imm: ir::immediates::Ieee64) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::UnaryIeee64 {
            opcode,
            imm,
        };
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }

    /// UnaryImm(imms=(imm: ir::immediates::Imm64), vals=0)
    #[allow(non_snake_case)]
    fn UnaryImm(self, opcode: Opcode, ctrl_typevar: Type, imm: ir::immediates::Imm64) -> (Inst, &'f mut ir::DataFlowGraph) {
        let mut data = ir::InstructionData::UnaryImm {
            opcode,
            imm,
        };
        data.sign_extend_immediates(ctrl_typevar);
        debug_assert_eq!(opcode.format(), InstructionFormat::from(&data), "Wrong InstructionFormat for Opcode: {}", opcode);
        self.build(data, ctrl_typevar)
    }
}
