/// An instruction format
///
/// Every opcode has a corresponding instruction format
/// which is represented by both the `InstructionFormat`
/// and the `InstructionData` enums.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum InstructionFormat {
    /// AtomicCas(imms=(flags: ir::MemFlags), vals=3)
    AtomicCas,
    /// AtomicRmw(imms=(flags: ir::MemFlags, op: ir::AtomicRmwOp), vals=2)
    AtomicRmw,
    /// Binary(imms=(), vals=2)
    Binary,
    /// BinaryImm64(imms=(imm: ir::immediates::Imm64), vals=1)
    BinaryImm64,
    /// BinaryImm8(imms=(imm: ir::immediates::Uimm8), vals=1)
    BinaryImm8,
    /// Branch(imms=(destination: ir::Block), vals=1)
    Branch,
    /// BranchTable(imms=(destination: ir::Block, table: ir::JumpTable), vals=1)
    BranchTable,
    /// Call(imms=(func_ref: ir::FuncRef), vals=0)
    Call,
    /// CallIndirect(imms=(sig_ref: ir::SigRef), vals=1)
    CallIndirect,
    /// CondTrap(imms=(code: ir::TrapCode), vals=1)
    CondTrap,
    /// DynamicStackLoad(imms=(dynamic_stack_slot: ir::DynamicStackSlot), vals=0)
    DynamicStackLoad,
    /// DynamicStackStore(imms=(dynamic_stack_slot: ir::DynamicStackSlot), vals=1)
    DynamicStackStore,
    /// FloatCompare(imms=(cond: ir::condcodes::FloatCC), vals=2)
    FloatCompare,
    /// FuncAddr(imms=(func_ref: ir::FuncRef), vals=0)
    FuncAddr,
    /// HeapAddr(imms=(heap: ir::Heap, offset: ir::immediates::Uimm32, size: ir::immediates::Uimm8), vals=1)
    HeapAddr,
    /// HeapLoad(imms=(heap_imm: ir::HeapImm), vals=1)
    HeapLoad,
    /// HeapStore(imms=(heap_imm: ir::HeapImm), vals=2)
    HeapStore,
    /// IntAddTrap(imms=(code: ir::TrapCode), vals=2)
    IntAddTrap,
    /// IntCompare(imms=(cond: ir::condcodes::IntCC), vals=2)
    IntCompare,
    /// IntCompareImm(imms=(cond: ir::condcodes::IntCC, imm: ir::immediates::Imm64), vals=1)
    IntCompareImm,
    /// Jump(imms=(destination: ir::Block), vals=0)
    Jump,
    /// Load(imms=(flags: ir::MemFlags, offset: ir::immediates::Offset32), vals=1)
    Load,
    /// LoadNoOffset(imms=(flags: ir::MemFlags), vals=1)
    LoadNoOffset,
    /// MultiAry(imms=(), vals=0)
    MultiAry,
    /// NullAry(imms=(), vals=0)
    NullAry,
    /// Shuffle(imms=(imm: ir::Immediate), vals=2)
    Shuffle,
    /// StackLoad(imms=(stack_slot: ir::StackSlot, offset: ir::immediates::Offset32), vals=0)
    StackLoad,
    /// StackStore(imms=(stack_slot: ir::StackSlot, offset: ir::immediates::Offset32), vals=1)
    StackStore,
    /// Store(imms=(flags: ir::MemFlags, offset: ir::immediates::Offset32), vals=2)
    Store,
    /// StoreNoOffset(imms=(flags: ir::MemFlags), vals=2)
    StoreNoOffset,
    /// TableAddr(imms=(table: ir::Table, offset: ir::immediates::Offset32), vals=1)
    TableAddr,
    /// Ternary(imms=(), vals=3)
    Ternary,
    /// TernaryImm8(imms=(imm: ir::immediates::Uimm8), vals=2)
    TernaryImm8,
    /// Trap(imms=(code: ir::TrapCode), vals=0)
    Trap,
    /// Unary(imms=(), vals=1)
    Unary,
    /// UnaryConst(imms=(constant_handle: ir::Constant), vals=0)
    UnaryConst,
    /// UnaryGlobalValue(imms=(global_value: ir::GlobalValue), vals=0)
    UnaryGlobalValue,
    /// UnaryIeee32(imms=(imm: ir::immediates::Ieee32), vals=0)
    UnaryIeee32,
    /// UnaryIeee64(imms=(imm: ir::immediates::Ieee64), vals=0)
    UnaryIeee64,
    /// UnaryImm(imms=(imm: ir::immediates::Imm64), vals=0)
    UnaryImm,
}

impl<'a> From<&'a InstructionData> for InstructionFormat {
    fn from(inst: &'a InstructionData) -> Self {
        match *inst {
            InstructionData::AtomicCas { .. } => {
                Self::AtomicCas
            }
            InstructionData::AtomicRmw { .. } => {
                Self::AtomicRmw
            }
            InstructionData::Binary { .. } => {
                Self::Binary
            }
            InstructionData::BinaryImm64 { .. } => {
                Self::BinaryImm64
            }
            InstructionData::BinaryImm8 { .. } => {
                Self::BinaryImm8
            }
            InstructionData::Branch { .. } => {
                Self::Branch
            }
            InstructionData::BranchTable { .. } => {
                Self::BranchTable
            }
            InstructionData::Call { .. } => {
                Self::Call
            }
            InstructionData::CallIndirect { .. } => {
                Self::CallIndirect
            }
            InstructionData::CondTrap { .. } => {
                Self::CondTrap
            }
            InstructionData::DynamicStackLoad { .. } => {
                Self::DynamicStackLoad
            }
            InstructionData::DynamicStackStore { .. } => {
                Self::DynamicStackStore
            }
            InstructionData::FloatCompare { .. } => {
                Self::FloatCompare
            }
            InstructionData::FuncAddr { .. } => {
                Self::FuncAddr
            }
            InstructionData::HeapAddr { .. } => {
                Self::HeapAddr
            }
            InstructionData::HeapLoad { .. } => {
                Self::HeapLoad
            }
            InstructionData::HeapStore { .. } => {
                Self::HeapStore
            }
            InstructionData::IntAddTrap { .. } => {
                Self::IntAddTrap
            }
            InstructionData::IntCompare { .. } => {
                Self::IntCompare
            }
            InstructionData::IntCompareImm { .. } => {
                Self::IntCompareImm
            }
            InstructionData::Jump { .. } => {
                Self::Jump
            }
            InstructionData::Load { .. } => {
                Self::Load
            }
            InstructionData::LoadNoOffset { .. } => {
                Self::LoadNoOffset
            }
            InstructionData::MultiAry { .. } => {
                Self::MultiAry
            }
            InstructionData::NullAry { .. } => {
                Self::NullAry
            }
            InstructionData::Shuffle { .. } => {
                Self::Shuffle
            }
            InstructionData::StackLoad { .. } => {
                Self::StackLoad
            }
            InstructionData::StackStore { .. } => {
                Self::StackStore
            }
            InstructionData::Store { .. } => {
                Self::Store
            }
            InstructionData::StoreNoOffset { .. } => {
                Self::StoreNoOffset
            }
            InstructionData::TableAddr { .. } => {
                Self::TableAddr
            }
            InstructionData::Ternary { .. } => {
                Self::Ternary
            }
            InstructionData::TernaryImm8 { .. } => {
                Self::TernaryImm8
            }
            InstructionData::Trap { .. } => {
                Self::Trap
            }
            InstructionData::Unary { .. } => {
                Self::Unary
            }
            InstructionData::UnaryConst { .. } => {
                Self::UnaryConst
            }
            InstructionData::UnaryGlobalValue { .. } => {
                Self::UnaryGlobalValue
            }
            InstructionData::UnaryIeee32 { .. } => {
                Self::UnaryIeee32
            }
            InstructionData::UnaryIeee64 { .. } => {
                Self::UnaryIeee64
            }
            InstructionData::UnaryImm { .. } => {
                Self::UnaryImm
            }
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Hash)]
#[cfg_attr(feature = "enable-serde", derive(Serialize, Deserialize))]
#[allow(missing_docs)]
pub enum InstructionData {
    AtomicCas {
        opcode: Opcode,
        args: [Value; 3],
        flags: ir::MemFlags,
    },
    AtomicRmw {
        opcode: Opcode,
        args: [Value; 2],
        flags: ir::MemFlags,
        op: ir::AtomicRmwOp,
    },
    Binary {
        opcode: Opcode,
        args: [Value; 2],
    },
    BinaryImm64 {
        opcode: Opcode,
        arg: Value,
        imm: ir::immediates::Imm64,
    },
    BinaryImm8 {
        opcode: Opcode,
        arg: Value,
        imm: ir::immediates::Uimm8,
    },
    Branch {
        opcode: Opcode,
        args: ValueList,
        destination: ir::Block,
    },
    BranchTable {
        opcode: Opcode,
        arg: Value,
        destination: ir::Block,
        table: ir::JumpTable,
    },
    Call {
        opcode: Opcode,
        args: ValueList,
        func_ref: ir::FuncRef,
    },
    CallIndirect {
        opcode: Opcode,
        args: ValueList,
        sig_ref: ir::SigRef,
    },
    CondTrap {
        opcode: Opcode,
        arg: Value,
        code: ir::TrapCode,
    },
    DynamicStackLoad {
        opcode: Opcode,
        dynamic_stack_slot: ir::DynamicStackSlot,
    },
    DynamicStackStore {
        opcode: Opcode,
        arg: Value,
        dynamic_stack_slot: ir::DynamicStackSlot,
    },
    FloatCompare {
        opcode: Opcode,
        args: [Value; 2],
        cond: ir::condcodes::FloatCC,
    },
    FuncAddr {
        opcode: Opcode,
        func_ref: ir::FuncRef,
    },
    HeapAddr {
        opcode: Opcode,
        arg: Value,
        heap: ir::Heap,
        offset: ir::immediates::Uimm32,
        size: ir::immediates::Uimm8,
    },
    HeapLoad {
        opcode: Opcode,
        arg: Value,
        heap_imm: ir::HeapImm,
    },
    HeapStore {
        opcode: Opcode,
        args: [Value; 2],
        heap_imm: ir::HeapImm,
    },
    IntAddTrap {
        opcode: Opcode,
        args: [Value; 2],
        code: ir::TrapCode,
    },
    IntCompare {
        opcode: Opcode,
        args: [Value; 2],
        cond: ir::condcodes::IntCC,
    },
    IntCompareImm {
        opcode: Opcode,
        arg: Value,
        cond: ir::condcodes::IntCC,
        imm: ir::immediates::Imm64,
    },
    Jump {
        opcode: Opcode,
        args: ValueList,
        destination: ir::Block,
    },
    Load {
        opcode: Opcode,
        arg: Value,
        flags: ir::MemFlags,
        offset: ir::immediates::Offset32,
    },
    LoadNoOffset {
        opcode: Opcode,
        arg: Value,
        flags: ir::MemFlags,
    },
    MultiAry {
        opcode: Opcode,
        args: ValueList,
    },
    NullAry {
        opcode: Opcode,
    },
    Shuffle {
        opcode: Opcode,
        args: [Value; 2],
        imm: ir::Immediate,
    },
    StackLoad {
        opcode: Opcode,
        stack_slot: ir::StackSlot,
        offset: ir::immediates::Offset32,
    },
    StackStore {
        opcode: Opcode,
        arg: Value,
        stack_slot: ir::StackSlot,
        offset: ir::immediates::Offset32,
    },
    Store {
        opcode: Opcode,
        args: [Value; 2],
        flags: ir::MemFlags,
        offset: ir::immediates::Offset32,
    },
    StoreNoOffset {
        opcode: Opcode,
        args: [Value; 2],
        flags: ir::MemFlags,
    },
    TableAddr {
        opcode: Opcode,
        arg: Value,
        table: ir::Table,
        offset: ir::immediates::Offset32,
    },
    Ternary {
        opcode: Opcode,
        args: [Value; 3],
    },
    TernaryImm8 {
        opcode: Opcode,
        args: [Value; 2],
        imm: ir::immediates::Uimm8,
    },
    Trap {
        opcode: Opcode,
        code: ir::TrapCode,
    },
    Unary {
        opcode: Opcode,
        arg: Value,
    },
    UnaryConst {
        opcode: Opcode,
        constant_handle: ir::Constant,
    },
    UnaryGlobalValue {
        opcode: Opcode,
        global_value: ir::GlobalValue,
    },
    UnaryIeee32 {
        opcode: Opcode,
        imm: ir::immediates::Ieee32,
    },
    UnaryIeee64 {
        opcode: Opcode,
        imm: ir::immediates::Ieee64,
    },
    UnaryImm {
        opcode: Opcode,
        imm: ir::immediates::Imm64,
    },
}
#[derive(Copy, Clone, Debug, PartialEq, Hash)]
#[derive(Eq)]
#[cfg_attr(feature = "enable-serde", derive(Serialize, Deserialize))]
#[allow(missing_docs)]
pub enum InstructionImms {
    AtomicCas {
        opcode: Opcode,
        flags: ir::MemFlags,
    },
    AtomicRmw {
        opcode: Opcode,
        flags: ir::MemFlags,
        op: ir::AtomicRmwOp,
    },
    Binary {
        opcode: Opcode,
    },
    BinaryImm64 {
        opcode: Opcode,
        imm: ir::immediates::Imm64,
    },
    BinaryImm8 {
        opcode: Opcode,
        imm: ir::immediates::Uimm8,
    },
    Branch {
        opcode: Opcode,
        destination: ir::Block,
    },
    BranchTable {
        opcode: Opcode,
        destination: ir::Block,
        table: ir::JumpTable,
    },
    Call {
        opcode: Opcode,
        func_ref: ir::FuncRef,
    },
    CallIndirect {
        opcode: Opcode,
        sig_ref: ir::SigRef,
    },
    CondTrap {
        opcode: Opcode,
        code: ir::TrapCode,
    },
    DynamicStackLoad {
        opcode: Opcode,
        dynamic_stack_slot: ir::DynamicStackSlot,
    },
    DynamicStackStore {
        opcode: Opcode,
        dynamic_stack_slot: ir::DynamicStackSlot,
    },
    FloatCompare {
        opcode: Opcode,
        cond: ir::condcodes::FloatCC,
    },
    FuncAddr {
        opcode: Opcode,
        func_ref: ir::FuncRef,
    },
    HeapAddr {
        opcode: Opcode,
        heap: ir::Heap,
        offset: ir::immediates::Uimm32,
        size: ir::immediates::Uimm8,
    },
    HeapLoad {
        opcode: Opcode,
        heap_imm: ir::HeapImm,
    },
    HeapStore {
        opcode: Opcode,
        heap_imm: ir::HeapImm,
    },
    IntAddTrap {
        opcode: Opcode,
        code: ir::TrapCode,
    },
    IntCompare {
        opcode: Opcode,
        cond: ir::condcodes::IntCC,
    },
    IntCompareImm {
        opcode: Opcode,
        cond: ir::condcodes::IntCC,
        imm: ir::immediates::Imm64,
    },
    Jump {
        opcode: Opcode,
        destination: ir::Block,
    },
    Load {
        opcode: Opcode,
        flags: ir::MemFlags,
        offset: ir::immediates::Offset32,
    },
    LoadNoOffset {
        opcode: Opcode,
        flags: ir::MemFlags,
    },
    MultiAry {
        opcode: Opcode,
    },
    NullAry {
        opcode: Opcode,
    },
    Shuffle {
        opcode: Opcode,
        imm: ir::Immediate,
    },
    StackLoad {
        opcode: Opcode,
        stack_slot: ir::StackSlot,
        offset: ir::immediates::Offset32,
    },
    StackStore {
        opcode: Opcode,
        stack_slot: ir::StackSlot,
        offset: ir::immediates::Offset32,
    },
    Store {
        opcode: Opcode,
        flags: ir::MemFlags,
        offset: ir::immediates::Offset32,
    },
    StoreNoOffset {
        opcode: Opcode,
        flags: ir::MemFlags,
    },
    TableAddr {
        opcode: Opcode,
        table: ir::Table,
        offset: ir::immediates::Offset32,
    },
    Ternary {
        opcode: Opcode,
    },
    TernaryImm8 {
        opcode: Opcode,
        imm: ir::immediates::Uimm8,
    },
    Trap {
        opcode: Opcode,
        code: ir::TrapCode,
    },
    Unary {
        opcode: Opcode,
    },
    UnaryConst {
        opcode: Opcode,
        constant_handle: ir::Constant,
    },
    UnaryGlobalValue {
        opcode: Opcode,
        global_value: ir::GlobalValue,
    },
    UnaryIeee32 {
        opcode: Opcode,
        imm: ir::immediates::Ieee32,
    },
    UnaryIeee64 {
        opcode: Opcode,
        imm: ir::immediates::Ieee64,
    },
    UnaryImm {
        opcode: Opcode,
        imm: ir::immediates::Imm64,
    },
}

impl InstructionData {
    /// Get the opcode of this instruction.
    pub fn opcode(&self) -> Opcode {
        match *self {
            Self::AtomicCas { opcode, .. } |
            Self::AtomicRmw { opcode, .. } |
            Self::Binary { opcode, .. } |
            Self::BinaryImm64 { opcode, .. } |
            Self::BinaryImm8 { opcode, .. } |
            Self::Branch { opcode, .. } |
            Self::BranchTable { opcode, .. } |
            Self::Call { opcode, .. } |
            Self::CallIndirect { opcode, .. } |
            Self::CondTrap { opcode, .. } |
            Self::DynamicStackLoad { opcode, .. } |
            Self::DynamicStackStore { opcode, .. } |
            Self::FloatCompare { opcode, .. } |
            Self::FuncAddr { opcode, .. } |
            Self::HeapAddr { opcode, .. } |
            Self::HeapLoad { opcode, .. } |
            Self::HeapStore { opcode, .. } |
            Self::IntAddTrap { opcode, .. } |
            Self::IntCompare { opcode, .. } |
            Self::IntCompareImm { opcode, .. } |
            Self::Jump { opcode, .. } |
            Self::Load { opcode, .. } |
            Self::LoadNoOffset { opcode, .. } |
            Self::MultiAry { opcode, .. } |
            Self::NullAry { opcode, .. } |
            Self::Shuffle { opcode, .. } |
            Self::StackLoad { opcode, .. } |
            Self::StackStore { opcode, .. } |
            Self::Store { opcode, .. } |
            Self::StoreNoOffset { opcode, .. } |
            Self::TableAddr { opcode, .. } |
            Self::Ternary { opcode, .. } |
            Self::TernaryImm8 { opcode, .. } |
            Self::Trap { opcode, .. } |
            Self::Unary { opcode, .. } |
            Self::UnaryConst { opcode, .. } |
            Self::UnaryGlobalValue { opcode, .. } |
            Self::UnaryIeee32 { opcode, .. } |
            Self::UnaryIeee64 { opcode, .. } |
            Self::UnaryImm { opcode, .. } => {
                opcode
            }
        }
    }

    /// Get the controlling type variable operand.
    pub fn typevar_operand(&self, pool: &ir::ValueListPool) -> Option<Value> {
        match *self {
            Self::Call { .. } |
            Self::DynamicStackLoad { .. } |
            Self::FuncAddr { .. } |
            Self::Jump { .. } |
            Self::MultiAry { .. } |
            Self::NullAry { .. } |
            Self::StackLoad { .. } |
            Self::Trap { .. } |
            Self::UnaryConst { .. } |
            Self::UnaryGlobalValue { .. } |
            Self::UnaryIeee32 { .. } |
            Self::UnaryIeee64 { .. } |
            Self::UnaryImm { .. } => {
                None
            }
            Self::BinaryImm64 { arg, .. } |
            Self::BinaryImm8 { arg, .. } |
            Self::BranchTable { arg, .. } |
            Self::CondTrap { arg, .. } |
            Self::DynamicStackStore { arg, .. } |
            Self::HeapAddr { arg, .. } |
            Self::HeapLoad { arg, .. } |
            Self::IntCompareImm { arg, .. } |
            Self::Load { arg, .. } |
            Self::LoadNoOffset { arg, .. } |
            Self::StackStore { arg, .. } |
            Self::TableAddr { arg, .. } |
            Self::Unary { arg, .. } => {
                Some(arg)
            }
            Self::AtomicRmw { args: ref args_arity2, .. } |
            Self::Binary { args: ref args_arity2, .. } |
            Self::FloatCompare { args: ref args_arity2, .. } |
            Self::HeapStore { args: ref args_arity2, .. } |
            Self::IntAddTrap { args: ref args_arity2, .. } |
            Self::IntCompare { args: ref args_arity2, .. } |
            Self::Shuffle { args: ref args_arity2, .. } |
            Self::Store { args: ref args_arity2, .. } |
            Self::StoreNoOffset { args: ref args_arity2, .. } |
            Self::TernaryImm8 { args: ref args_arity2, .. } => {
                Some(args_arity2[0])
            }
            Self::Ternary { args: ref args_arity3, .. } => {
                Some(args_arity3[1])
            }
            Self::AtomicCas { args: ref args_arity3, .. } => {
                Some(args_arity3[2])
            }
            Self::Branch { ref args, .. } |
            Self::CallIndirect { ref args, .. } => {
                args.get(0, pool)
            }
        }
    }

    /// Get the value arguments to this instruction.
    pub fn arguments<'a>(&'a self, pool: &'a ir::ValueListPool) -> &[Value] {
        match *self {
            Self::DynamicStackLoad { .. } |
            Self::FuncAddr { .. } |
            Self::NullAry { .. } |
            Self::StackLoad { .. } |
            Self::Trap { .. } |
            Self::UnaryConst { .. } |
            Self::UnaryGlobalValue { .. } |
            Self::UnaryIeee32 { .. } |
            Self::UnaryIeee64 { .. } |
            Self::UnaryImm { .. } => {
                &[]
            }
            Self::AtomicRmw { args: ref args_arity2, .. } |
            Self::Binary { args: ref args_arity2, .. } |
            Self::FloatCompare { args: ref args_arity2, .. } |
            Self::HeapStore { args: ref args_arity2, .. } |
            Self::IntAddTrap { args: ref args_arity2, .. } |
            Self::IntCompare { args: ref args_arity2, .. } |
            Self::Shuffle { args: ref args_arity2, .. } |
            Self::Store { args: ref args_arity2, .. } |
            Self::StoreNoOffset { args: ref args_arity2, .. } |
            Self::TernaryImm8 { args: ref args_arity2, .. } => {
                args_arity2
            }
            Self::AtomicCas { args: ref args_arity3, .. } |
            Self::Ternary { args: ref args_arity3, .. } => {
                args_arity3
            }
            Self::BinaryImm64 { ref arg, .. } |
            Self::BinaryImm8 { ref arg, .. } |
            Self::BranchTable { ref arg, .. } |
            Self::CondTrap { ref arg, .. } |
            Self::DynamicStackStore { ref arg, .. } |
            Self::HeapAddr { ref arg, .. } |
            Self::HeapLoad { ref arg, .. } |
            Self::IntCompareImm { ref arg, .. } |
            Self::Load { ref arg, .. } |
            Self::LoadNoOffset { ref arg, .. } |
            Self::StackStore { ref arg, .. } |
            Self::TableAddr { ref arg, .. } |
            Self::Unary { ref arg, .. } => {
                core::slice::from_ref(arg)
            }
            Self::Branch { ref args, .. } |
            Self::Call { ref args, .. } |
            Self::CallIndirect { ref args, .. } |
            Self::Jump { ref args, .. } |
            Self::MultiAry { ref args, .. } => {
                args.as_slice(pool)
            }
        }
    }

    /// Get mutable references to the value arguments to this
    /// instruction.
    pub fn arguments_mut<'a>(&'a mut self, pool: &'a mut ir::ValueListPool) -> &mut [Value] {
        match *self {
            Self::DynamicStackLoad { .. } |
            Self::FuncAddr { .. } |
            Self::NullAry { .. } |
            Self::StackLoad { .. } |
            Self::Trap { .. } |
            Self::UnaryConst { .. } |
            Self::UnaryGlobalValue { .. } |
            Self::UnaryIeee32 { .. } |
            Self::UnaryIeee64 { .. } |
            Self::UnaryImm { .. } => {
                &mut []
            }
            Self::AtomicRmw { args: ref mut args_arity2, .. } |
            Self::Binary { args: ref mut args_arity2, .. } |
            Self::FloatCompare { args: ref mut args_arity2, .. } |
            Self::HeapStore { args: ref mut args_arity2, .. } |
            Self::IntAddTrap { args: ref mut args_arity2, .. } |
            Self::IntCompare { args: ref mut args_arity2, .. } |
            Self::Shuffle { args: ref mut args_arity2, .. } |
            Self::Store { args: ref mut args_arity2, .. } |
            Self::StoreNoOffset { args: ref mut args_arity2, .. } |
            Self::TernaryImm8 { args: ref mut args_arity2, .. } => {
                args_arity2
            }
            Self::AtomicCas { args: ref mut args_arity3, .. } |
            Self::Ternary { args: ref mut args_arity3, .. } => {
                args_arity3
            }
            Self::BinaryImm64 { ref mut arg, .. } |
            Self::BinaryImm8 { ref mut arg, .. } |
            Self::BranchTable { ref mut arg, .. } |
            Self::CondTrap { ref mut arg, .. } |
            Self::DynamicStackStore { ref mut arg, .. } |
            Self::HeapAddr { ref mut arg, .. } |
            Self::HeapLoad { ref mut arg, .. } |
            Self::IntCompareImm { ref mut arg, .. } |
            Self::Load { ref mut arg, .. } |
            Self::LoadNoOffset { ref mut arg, .. } |
            Self::StackStore { ref mut arg, .. } |
            Self::TableAddr { ref mut arg, .. } |
            Self::Unary { ref mut arg, .. } => {
                core::slice::from_mut(arg)
            }
            Self::Branch { ref mut args, .. } |
            Self::Call { ref mut args, .. } |
            Self::CallIndirect { ref mut args, .. } |
            Self::Jump { ref mut args, .. } |
            Self::MultiAry { ref mut args, .. } => {
                args.as_mut_slice(pool)
            }
        }
    }

    /// Take out the value list with all the value arguments and return
    /// it.
    ///
    /// This leaves the value list in the instruction empty. Use
    /// `put_value_list` to put the value list back.
    pub fn take_value_list(&mut self) -> Option<ir::ValueList> {
        match *self {
            Self::Branch { ref mut args, .. } |
            Self::Call { ref mut args, .. } |
            Self::CallIndirect { ref mut args, .. } |
            Self::Jump { ref mut args, .. } |
            Self::MultiAry { ref mut args, .. } => {
                Some(args.take())
            }
            _ => {
                None
            }
        }
    }

    /// Put back a value list.
    ///
    /// After removing a value list with `take_value_list()`, use this
    /// method to put it back. It is required that this instruction has
    /// a format that accepts a value list, and that the existing value
    /// list is empty. This avoids leaking list pool memory.
    pub fn put_value_list(&mut self, vlist: ir::ValueList) {
        let args = match *self {
            Self::Branch { ref mut args, .. } => args,
            Self::Call { ref mut args, .. } => args,
            Self::CallIndirect { ref mut args, .. } => args,
            Self::Jump { ref mut args, .. } => args,
            Self::MultiAry { ref mut args, .. } => args,
            _ => panic!("No value list: {:?}", self),
        };
        debug_assert!(args.is_empty(), "Value list already in use");
        *args = vlist;
    }

    /// Compare two `InstructionData` for equality.
    ///
    /// This operation requires a reference to a `ValueListPool` to
    /// determine if the contents of any `ValueLists` are equal.
    pub fn eq(&self, other: &Self, pool: &ir::ValueListPool) -> bool {
        if ::core::mem::discriminant(self) != ::core::mem::discriminant(other) {
            return false;
        }
        match (self, other) {
            (&Self::AtomicCas { opcode: ref opcode1, args: ref args1, flags: ref flags1 }, &Self::AtomicCas { opcode: ref opcode2, args: ref args2, flags: ref flags2 }) => {
                opcode1 == opcode2
                && flags1 == flags2
                && args1 == args2
            }
            (&Self::AtomicRmw { opcode: ref opcode1, args: ref args1, flags: ref flags1, op: ref op1 }, &Self::AtomicRmw { opcode: ref opcode2, args: ref args2, flags: ref flags2, op: ref op2 }) => {
                opcode1 == opcode2
                && flags1 == flags2
                && op1 == op2
                && args1 == args2
            }
            (&Self::Binary { opcode: ref opcode1, args: ref args1 }, &Self::Binary { opcode: ref opcode2, args: ref args2 }) => {
                opcode1 == opcode2
                && args1 == args2
            }
            (&Self::BinaryImm64 { opcode: ref opcode1, arg: ref arg1, imm: ref imm1 }, &Self::BinaryImm64 { opcode: ref opcode2, arg: ref arg2, imm: ref imm2 }) => {
                opcode1 == opcode2
                && imm1 == imm2
                && arg1 == arg2
            }
            (&Self::BinaryImm8 { opcode: ref opcode1, arg: ref arg1, imm: ref imm1 }, &Self::BinaryImm8 { opcode: ref opcode2, arg: ref arg2, imm: ref imm2 }) => {
                opcode1 == opcode2
                && imm1 == imm2
                && arg1 == arg2
            }
            (&Self::Branch { opcode: ref opcode1, args: ref args1, destination: ref destination1 }, &Self::Branch { opcode: ref opcode2, args: ref args2, destination: ref destination2 }) => {
                opcode1 == opcode2
                && destination1 == destination2
                && args1.as_slice(pool) == args2.as_slice(pool)
            }
            (&Self::BranchTable { opcode: ref opcode1, arg: ref arg1, destination: ref destination1, table: ref table1 }, &Self::BranchTable { opcode: ref opcode2, arg: ref arg2, destination: ref destination2, table: ref table2 }) => {
                opcode1 == opcode2
                && destination1 == destination2
                && table1 == table2
                && arg1 == arg2
            }
            (&Self::Call { opcode: ref opcode1, args: ref args1, func_ref: ref func_ref1 }, &Self::Call { opcode: ref opcode2, args: ref args2, func_ref: ref func_ref2 }) => {
                opcode1 == opcode2
                && func_ref1 == func_ref2
                && args1.as_slice(pool) == args2.as_slice(pool)
            }
            (&Self::CallIndirect { opcode: ref opcode1, args: ref args1, sig_ref: ref sig_ref1 }, &Self::CallIndirect { opcode: ref opcode2, args: ref args2, sig_ref: ref sig_ref2 }) => {
                opcode1 == opcode2
                && sig_ref1 == sig_ref2
                && args1.as_slice(pool) == args2.as_slice(pool)
            }
            (&Self::CondTrap { opcode: ref opcode1, arg: ref arg1, code: ref code1 }, &Self::CondTrap { opcode: ref opcode2, arg: ref arg2, code: ref code2 }) => {
                opcode1 == opcode2
                && code1 == code2
                && arg1 == arg2
            }
            (&Self::DynamicStackLoad { opcode: ref opcode1, dynamic_stack_slot: ref dynamic_stack_slot1 }, &Self::DynamicStackLoad { opcode: ref opcode2, dynamic_stack_slot: ref dynamic_stack_slot2 }) => {
                opcode1 == opcode2
                && dynamic_stack_slot1 == dynamic_stack_slot2
            }
            (&Self::DynamicStackStore { opcode: ref opcode1, arg: ref arg1, dynamic_stack_slot: ref dynamic_stack_slot1 }, &Self::DynamicStackStore { opcode: ref opcode2, arg: ref arg2, dynamic_stack_slot: ref dynamic_stack_slot2 }) => {
                opcode1 == opcode2
                && dynamic_stack_slot1 == dynamic_stack_slot2
                && arg1 == arg2
            }
            (&Self::FloatCompare { opcode: ref opcode1, args: ref args1, cond: ref cond1 }, &Self::FloatCompare { opcode: ref opcode2, args: ref args2, cond: ref cond2 }) => {
                opcode1 == opcode2
                && cond1 == cond2
                && args1 == args2
            }
            (&Self::FuncAddr { opcode: ref opcode1, func_ref: ref func_ref1 }, &Self::FuncAddr { opcode: ref opcode2, func_ref: ref func_ref2 }) => {
                opcode1 == opcode2
                && func_ref1 == func_ref2
            }
            (&Self::HeapAddr { opcode: ref opcode1, arg: ref arg1, heap: ref heap1, offset: ref offset1, size: ref size1 }, &Self::HeapAddr { opcode: ref opcode2, arg: ref arg2, heap: ref heap2, offset: ref offset2, size: ref size2 }) => {
                opcode1 == opcode2
                && heap1 == heap2
                && offset1 == offset2
                && size1 == size2
                && arg1 == arg2
            }
            (&Self::HeapLoad { opcode: ref opcode1, arg: ref arg1, heap_imm: ref heap_imm1 }, &Self::HeapLoad { opcode: ref opcode2, arg: ref arg2, heap_imm: ref heap_imm2 }) => {
                opcode1 == opcode2
                && heap_imm1 == heap_imm2
                && arg1 == arg2
            }
            (&Self::HeapStore { opcode: ref opcode1, args: ref args1, heap_imm: ref heap_imm1 }, &Self::HeapStore { opcode: ref opcode2, args: ref args2, heap_imm: ref heap_imm2 }) => {
                opcode1 == opcode2
                && heap_imm1 == heap_imm2
                && args1 == args2
            }
            (&Self::IntAddTrap { opcode: ref opcode1, args: ref args1, code: ref code1 }, &Self::IntAddTrap { opcode: ref opcode2, args: ref args2, code: ref code2 }) => {
                opcode1 == opcode2
                && code1 == code2
                && args1 == args2
            }
            (&Self::IntCompare { opcode: ref opcode1, args: ref args1, cond: ref cond1 }, &Self::IntCompare { opcode: ref opcode2, args: ref args2, cond: ref cond2 }) => {
                opcode1 == opcode2
                && cond1 == cond2
                && args1 == args2
            }
            (&Self::IntCompareImm { opcode: ref opcode1, arg: ref arg1, cond: ref cond1, imm: ref imm1 }, &Self::IntCompareImm { opcode: ref opcode2, arg: ref arg2, cond: ref cond2, imm: ref imm2 }) => {
                opcode1 == opcode2
                && cond1 == cond2
                && imm1 == imm2
                && arg1 == arg2
            }
            (&Self::Jump { opcode: ref opcode1, args: ref args1, destination: ref destination1 }, &Self::Jump { opcode: ref opcode2, args: ref args2, destination: ref destination2 }) => {
                opcode1 == opcode2
                && destination1 == destination2
                && args1.as_slice(pool) == args2.as_slice(pool)
            }
            (&Self::Load { opcode: ref opcode1, arg: ref arg1, flags: ref flags1, offset: ref offset1 }, &Self::Load { opcode: ref opcode2, arg: ref arg2, flags: ref flags2, offset: ref offset2 }) => {
                opcode1 == opcode2
                && flags1 == flags2
                && offset1 == offset2
                && arg1 == arg2
            }
            (&Self::LoadNoOffset { opcode: ref opcode1, arg: ref arg1, flags: ref flags1 }, &Self::LoadNoOffset { opcode: ref opcode2, arg: ref arg2, flags: ref flags2 }) => {
                opcode1 == opcode2
                && flags1 == flags2
                && arg1 == arg2
            }
            (&Self::MultiAry { opcode: ref opcode1, args: ref args1 }, &Self::MultiAry { opcode: ref opcode2, args: ref args2 }) => {
                opcode1 == opcode2
                && args1.as_slice(pool) == args2.as_slice(pool)
            }
            (&Self::NullAry { opcode: ref opcode1 }, &Self::NullAry { opcode: ref opcode2 }) => {
                opcode1 == opcode2
            }
            (&Self::Shuffle { opcode: ref opcode1, args: ref args1, imm: ref imm1 }, &Self::Shuffle { opcode: ref opcode2, args: ref args2, imm: ref imm2 }) => {
                opcode1 == opcode2
                && imm1 == imm2
                && args1 == args2
            }
            (&Self::StackLoad { opcode: ref opcode1, stack_slot: ref stack_slot1, offset: ref offset1 }, &Self::StackLoad { opcode: ref opcode2, stack_slot: ref stack_slot2, offset: ref offset2 }) => {
                opcode1 == opcode2
                && stack_slot1 == stack_slot2
                && offset1 == offset2
            }
            (&Self::StackStore { opcode: ref opcode1, arg: ref arg1, stack_slot: ref stack_slot1, offset: ref offset1 }, &Self::StackStore { opcode: ref opcode2, arg: ref arg2, stack_slot: ref stack_slot2, offset: ref offset2 }) => {
                opcode1 == opcode2
                && stack_slot1 == stack_slot2
                && offset1 == offset2
                && arg1 == arg2
            }
            (&Self::Store { opcode: ref opcode1, args: ref args1, flags: ref flags1, offset: ref offset1 }, &Self::Store { opcode: ref opcode2, args: ref args2, flags: ref flags2, offset: ref offset2 }) => {
                opcode1 == opcode2
                && flags1 == flags2
                && offset1 == offset2
                && args1 == args2
            }
            (&Self::StoreNoOffset { opcode: ref opcode1, args: ref args1, flags: ref flags1 }, &Self::StoreNoOffset { opcode: ref opcode2, args: ref args2, flags: ref flags2 }) => {
                opcode1 == opcode2
                && flags1 == flags2
                && args1 == args2
            }
            (&Self::TableAddr { opcode: ref opcode1, arg: ref arg1, table: ref table1, offset: ref offset1 }, &Self::TableAddr { opcode: ref opcode2, arg: ref arg2, table: ref table2, offset: ref offset2 }) => {
                opcode1 == opcode2
                && table1 == table2
                && offset1 == offset2
                && arg1 == arg2
            }
            (&Self::Ternary { opcode: ref opcode1, args: ref args1 }, &Self::Ternary { opcode: ref opcode2, args: ref args2 }) => {
                opcode1 == opcode2
                && args1 == args2
            }
            (&Self::TernaryImm8 { opcode: ref opcode1, args: ref args1, imm: ref imm1 }, &Self::TernaryImm8 { opcode: ref opcode2, args: ref args2, imm: ref imm2 }) => {
                opcode1 == opcode2
                && imm1 == imm2
                && args1 == args2
            }
            (&Self::Trap { opcode: ref opcode1, code: ref code1 }, &Self::Trap { opcode: ref opcode2, code: ref code2 }) => {
                opcode1 == opcode2
                && code1 == code2
            }
            (&Self::Unary { opcode: ref opcode1, arg: ref arg1 }, &Self::Unary { opcode: ref opcode2, arg: ref arg2 }) => {
                opcode1 == opcode2
                && arg1 == arg2
            }
            (&Self::UnaryConst { opcode: ref opcode1, constant_handle: ref constant_handle1 }, &Self::UnaryConst { opcode: ref opcode2, constant_handle: ref constant_handle2 }) => {
                opcode1 == opcode2
                && constant_handle1 == constant_handle2
            }
            (&Self::UnaryGlobalValue { opcode: ref opcode1, global_value: ref global_value1 }, &Self::UnaryGlobalValue { opcode: ref opcode2, global_value: ref global_value2 }) => {
                opcode1 == opcode2
                && global_value1 == global_value2
            }
            (&Self::UnaryIeee32 { opcode: ref opcode1, imm: ref imm1 }, &Self::UnaryIeee32 { opcode: ref opcode2, imm: ref imm2 }) => {
                opcode1 == opcode2
                && imm1 == imm2
            }
            (&Self::UnaryIeee64 { opcode: ref opcode1, imm: ref imm1 }, &Self::UnaryIeee64 { opcode: ref opcode2, imm: ref imm2 }) => {
                opcode1 == opcode2
                && imm1 == imm2
            }
            (&Self::UnaryImm { opcode: ref opcode1, imm: ref imm1 }, &Self::UnaryImm { opcode: ref opcode2, imm: ref imm2 }) => {
                opcode1 == opcode2
                && imm1 == imm2
            }
            _ => unreachable!()
        }
    }

    /// Hash an `InstructionData`.
    ///
    /// This operation requires a reference to a `ValueListPool` to
    /// hash the contents of any `ValueLists`.
    pub fn hash<H: ::core::hash::Hasher>(&self, state: &mut H, pool: &ir::ValueListPool) {
        match *self {
            Self::AtomicCas{opcode, ref args, flags} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&flags, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::AtomicRmw{opcode, ref args, flags, op} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&flags, state);
                ::core::hash::Hash::hash(&op, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::Binary{opcode, ref args} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::BinaryImm64{opcode, ref arg, imm} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&imm, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::BinaryImm8{opcode, ref arg, imm} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&imm, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::Branch{opcode, ref args, destination} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&destination, state);
                ::core::hash::Hash::hash(args.as_slice(pool), state);
            }
            Self::BranchTable{opcode, ref arg, destination, table} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&destination, state);
                ::core::hash::Hash::hash(&table, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::Call{opcode, ref args, func_ref} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&func_ref, state);
                ::core::hash::Hash::hash(args.as_slice(pool), state);
            }
            Self::CallIndirect{opcode, ref args, sig_ref} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&sig_ref, state);
                ::core::hash::Hash::hash(args.as_slice(pool), state);
            }
            Self::CondTrap{opcode, ref arg, code} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&code, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::DynamicStackLoad{opcode, dynamic_stack_slot} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&dynamic_stack_slot, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::DynamicStackStore{opcode, ref arg, dynamic_stack_slot} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&dynamic_stack_slot, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::FloatCompare{opcode, ref args, cond} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&cond, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::FuncAddr{opcode, func_ref} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&func_ref, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::HeapAddr{opcode, ref arg, heap, offset, size} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&heap, state);
                ::core::hash::Hash::hash(&offset, state);
                ::core::hash::Hash::hash(&size, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::HeapLoad{opcode, ref arg, heap_imm} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&heap_imm, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::HeapStore{opcode, ref args, heap_imm} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&heap_imm, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::IntAddTrap{opcode, ref args, code} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&code, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::IntCompare{opcode, ref args, cond} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&cond, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::IntCompareImm{opcode, ref arg, cond, imm} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&cond, state);
                ::core::hash::Hash::hash(&imm, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::Jump{opcode, ref args, destination} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&destination, state);
                ::core::hash::Hash::hash(args.as_slice(pool), state);
            }
            Self::Load{opcode, ref arg, flags, offset} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&flags, state);
                ::core::hash::Hash::hash(&offset, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::LoadNoOffset{opcode, ref arg, flags} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&flags, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::MultiAry{opcode, ref args} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(args.as_slice(pool), state);
            }
            Self::NullAry{opcode} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::Shuffle{opcode, ref args, imm} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&imm, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::StackLoad{opcode, stack_slot, offset} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&stack_slot, state);
                ::core::hash::Hash::hash(&offset, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::StackStore{opcode, ref arg, stack_slot, offset} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&stack_slot, state);
                ::core::hash::Hash::hash(&offset, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::Store{opcode, ref args, flags, offset} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&flags, state);
                ::core::hash::Hash::hash(&offset, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::StoreNoOffset{opcode, ref args, flags} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&flags, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::TableAddr{opcode, ref arg, table, offset} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&table, state);
                ::core::hash::Hash::hash(&offset, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::Ternary{opcode, ref args} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::TernaryImm8{opcode, ref args, imm} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&imm, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::Trap{opcode, code} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&code, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::Unary{opcode, ref arg} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::UnaryConst{opcode, constant_handle} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&constant_handle, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::UnaryGlobalValue{opcode, global_value} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&global_value, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::UnaryIeee32{opcode, imm} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&imm, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::UnaryIeee64{opcode, imm} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&imm, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::UnaryImm{opcode, imm} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&imm, state);
                ::core::hash::Hash::hash(&(), state);
            }
        }
    }
}
impl std::convert::From<&InstructionData> for InstructionImms {
    /// Convert an `InstructionData` into an `InstructionImms`.
    fn from(data: &InstructionData) -> InstructionImms {
        match data {
            InstructionData::AtomicCas {
                opcode,
                flags,
                ..
            } => InstructionImms::AtomicCas {
                opcode: *opcode,
                flags: flags.clone(),
            },
            InstructionData::AtomicRmw {
                opcode,
                flags,
                op,
                ..
            } => InstructionImms::AtomicRmw {
                opcode: *opcode,
                flags: flags.clone(),
                op: op.clone(),
            },
            InstructionData::Binary {
                opcode,
                ..
            } => InstructionImms::Binary {
                opcode: *opcode,
            },
            InstructionData::BinaryImm64 {
                opcode,
                imm,
                ..
            } => InstructionImms::BinaryImm64 {
                opcode: *opcode,
                imm: imm.clone(),
            },
            InstructionData::BinaryImm8 {
                opcode,
                imm,
                ..
            } => InstructionImms::BinaryImm8 {
                opcode: *opcode,
                imm: imm.clone(),
            },
            InstructionData::Branch {
                opcode,
                destination,
                ..
            } => InstructionImms::Branch {
                opcode: *opcode,
                destination: destination.clone(),
            },
            InstructionData::BranchTable {
                opcode,
                destination,
                table,
                ..
            } => InstructionImms::BranchTable {
                opcode: *opcode,
                destination: destination.clone(),
                table: table.clone(),
            },
            InstructionData::Call {
                opcode,
                func_ref,
                ..
            } => InstructionImms::Call {
                opcode: *opcode,
                func_ref: func_ref.clone(),
            },
            InstructionData::CallIndirect {
                opcode,
                sig_ref,
                ..
            } => InstructionImms::CallIndirect {
                opcode: *opcode,
                sig_ref: sig_ref.clone(),
            },
            InstructionData::CondTrap {
                opcode,
                code,
                ..
            } => InstructionImms::CondTrap {
                opcode: *opcode,
                code: code.clone(),
            },
            InstructionData::DynamicStackLoad {
                opcode,
                dynamic_stack_slot,
                ..
            } => InstructionImms::DynamicStackLoad {
                opcode: *opcode,
                dynamic_stack_slot: dynamic_stack_slot.clone(),
            },
            InstructionData::DynamicStackStore {
                opcode,
                dynamic_stack_slot,
                ..
            } => InstructionImms::DynamicStackStore {
                opcode: *opcode,
                dynamic_stack_slot: dynamic_stack_slot.clone(),
            },
            InstructionData::FloatCompare {
                opcode,
                cond,
                ..
            } => InstructionImms::FloatCompare {
                opcode: *opcode,
                cond: cond.clone(),
            },
            InstructionData::FuncAddr {
                opcode,
                func_ref,
                ..
            } => InstructionImms::FuncAddr {
                opcode: *opcode,
                func_ref: func_ref.clone(),
            },
            InstructionData::HeapAddr {
                opcode,
                heap,
                offset,
                size,
                ..
            } => InstructionImms::HeapAddr {
                opcode: *opcode,
                heap: heap.clone(),
                offset: offset.clone(),
                size: size.clone(),
            },
            InstructionData::HeapLoad {
                opcode,
                heap_imm,
                ..
            } => InstructionImms::HeapLoad {
                opcode: *opcode,
                heap_imm: heap_imm.clone(),
            },
            InstructionData::HeapStore {
                opcode,
                heap_imm,
                ..
            } => InstructionImms::HeapStore {
                opcode: *opcode,
                heap_imm: heap_imm.clone(),
            },
            InstructionData::IntAddTrap {
                opcode,
                code,
                ..
            } => InstructionImms::IntAddTrap {
                opcode: *opcode,
                code: code.clone(),
            },
            InstructionData::IntCompare {
                opcode,
                cond,
                ..
            } => InstructionImms::IntCompare {
                opcode: *opcode,
                cond: cond.clone(),
            },
            InstructionData::IntCompareImm {
                opcode,
                cond,
                imm,
                ..
            } => InstructionImms::IntCompareImm {
                opcode: *opcode,
                cond: cond.clone(),
                imm: imm.clone(),
            },
            InstructionData::Jump {
                opcode,
                destination,
                ..
            } => InstructionImms::Jump {
                opcode: *opcode,
                destination: destination.clone(),
            },
            InstructionData::Load {
                opcode,
                flags,
                offset,
                ..
            } => InstructionImms::Load {
                opcode: *opcode,
                flags: flags.clone(),
                offset: offset.clone(),
            },
            InstructionData::LoadNoOffset {
                opcode,
                flags,
                ..
            } => InstructionImms::LoadNoOffset {
                opcode: *opcode,
                flags: flags.clone(),
            },
            InstructionData::MultiAry {
                opcode,
                ..
            } => InstructionImms::MultiAry {
                opcode: *opcode,
            },
            InstructionData::NullAry {
                opcode,
                ..
            } => InstructionImms::NullAry {
                opcode: *opcode,
            },
            InstructionData::Shuffle {
                opcode,
                imm,
                ..
            } => InstructionImms::Shuffle {
                opcode: *opcode,
                imm: imm.clone(),
            },
            InstructionData::StackLoad {
                opcode,
                stack_slot,
                offset,
                ..
            } => InstructionImms::StackLoad {
                opcode: *opcode,
                stack_slot: stack_slot.clone(),
                offset: offset.clone(),
            },
            InstructionData::StackStore {
                opcode,
                stack_slot,
                offset,
                ..
            } => InstructionImms::StackStore {
                opcode: *opcode,
                stack_slot: stack_slot.clone(),
                offset: offset.clone(),
            },
            InstructionData::Store {
                opcode,
                flags,
                offset,
                ..
            } => InstructionImms::Store {
                opcode: *opcode,
                flags: flags.clone(),
                offset: offset.clone(),
            },
            InstructionData::StoreNoOffset {
                opcode,
                flags,
                ..
            } => InstructionImms::StoreNoOffset {
                opcode: *opcode,
                flags: flags.clone(),
            },
            InstructionData::TableAddr {
                opcode,
                table,
                offset,
                ..
            } => InstructionImms::TableAddr {
                opcode: *opcode,
                table: table.clone(),
                offset: offset.clone(),
            },
            InstructionData::Ternary {
                opcode,
                ..
            } => InstructionImms::Ternary {
                opcode: *opcode,
            },
            InstructionData::TernaryImm8 {
                opcode,
                imm,
                ..
            } => InstructionImms::TernaryImm8 {
                opcode: *opcode,
                imm: imm.clone(),
            },
            InstructionData::Trap {
                opcode,
                code,
                ..
            } => InstructionImms::Trap {
                opcode: *opcode,
                code: code.clone(),
            },
            InstructionData::Unary {
                opcode,
                ..
            } => InstructionImms::Unary {
                opcode: *opcode,
            },
            InstructionData::UnaryConst {
                opcode,
                constant_handle,
                ..
            } => InstructionImms::UnaryConst {
                opcode: *opcode,
                constant_handle: constant_handle.clone(),
            },
            InstructionData::UnaryGlobalValue {
                opcode,
                global_value,
                ..
            } => InstructionImms::UnaryGlobalValue {
                opcode: *opcode,
                global_value: global_value.clone(),
            },
            InstructionData::UnaryIeee32 {
                opcode,
                imm,
                ..
            } => InstructionImms::UnaryIeee32 {
                opcode: *opcode,
                imm: imm.clone(),
            },
            InstructionData::UnaryIeee64 {
                opcode,
                imm,
                ..
            } => InstructionImms::UnaryIeee64 {
                opcode: *opcode,
                imm: imm.clone(),
            },
            InstructionData::UnaryImm {
                opcode,
                imm,
                ..
            } => InstructionImms::UnaryImm {
                opcode: *opcode,
                imm: imm.clone(),
            },
        }
    }
}

impl InstructionImms {
    /// Get the opcode of this instruction.
    pub fn opcode(&self) -> Opcode {
        match *self {
            Self::AtomicCas { opcode, .. } |
            Self::AtomicRmw { opcode, .. } |
            Self::Binary { opcode, .. } |
            Self::BinaryImm64 { opcode, .. } |
            Self::BinaryImm8 { opcode, .. } |
            Self::Branch { opcode, .. } |
            Self::BranchTable { opcode, .. } |
            Self::Call { opcode, .. } |
            Self::CallIndirect { opcode, .. } |
            Self::CondTrap { opcode, .. } |
            Self::DynamicStackLoad { opcode, .. } |
            Self::DynamicStackStore { opcode, .. } |
            Self::FloatCompare { opcode, .. } |
            Self::FuncAddr { opcode, .. } |
            Self::HeapAddr { opcode, .. } |
            Self::HeapLoad { opcode, .. } |
            Self::HeapStore { opcode, .. } |
            Self::IntAddTrap { opcode, .. } |
            Self::IntCompare { opcode, .. } |
            Self::IntCompareImm { opcode, .. } |
            Self::Jump { opcode, .. } |
            Self::Load { opcode, .. } |
            Self::LoadNoOffset { opcode, .. } |
            Self::MultiAry { opcode, .. } |
            Self::NullAry { opcode, .. } |
            Self::Shuffle { opcode, .. } |
            Self::StackLoad { opcode, .. } |
            Self::StackStore { opcode, .. } |
            Self::Store { opcode, .. } |
            Self::StoreNoOffset { opcode, .. } |
            Self::TableAddr { opcode, .. } |
            Self::Ternary { opcode, .. } |
            Self::TernaryImm8 { opcode, .. } |
            Self::Trap { opcode, .. } |
            Self::Unary { opcode, .. } |
            Self::UnaryConst { opcode, .. } |
            Self::UnaryGlobalValue { opcode, .. } |
            Self::UnaryIeee32 { opcode, .. } |
            Self::UnaryIeee64 { opcode, .. } |
            Self::UnaryImm { opcode, .. } => {
                opcode
            }
        }
    }
}

impl  InstructionImms {
    /// Convert an `InstructionImms` into an `InstructionData` by adding args.
    pub fn with_args(&self, values: &[Value], value_list: &mut ValueListPool) -> InstructionData {
        match self {
            InstructionImms::AtomicCas {
                opcode,
                flags,
            } => {
                InstructionData::AtomicCas {
                    opcode: *opcode,
                    flags: flags.clone(),
                    args: [values[0], values[1], values[2]],
                }
            },
            InstructionImms::AtomicRmw {
                opcode,
                flags,
                op,
            } => {
                InstructionData::AtomicRmw {
                    opcode: *opcode,
                    flags: flags.clone(),
                    op: op.clone(),
                    args: [values[0], values[1]],
                }
            },
            InstructionImms::Binary {
                opcode,
            } => {
                InstructionData::Binary {
                    opcode: *opcode,
                    args: [values[0], values[1]],
                }
            },
            InstructionImms::BinaryImm64 {
                opcode,
                imm,
            } => {
                InstructionData::BinaryImm64 {
                    opcode: *opcode,
                    imm: imm.clone(),
                    arg: values[0],
                }
            },
            InstructionImms::BinaryImm8 {
                opcode,
                imm,
            } => {
                InstructionData::BinaryImm8 {
                    opcode: *opcode,
                    imm: imm.clone(),
                    arg: values[0],
                }
            },
            InstructionImms::Branch {
                opcode,
                destination,
            } => {
            let args = ValueList::from_slice(values, value_list);
                InstructionData::Branch {
                    opcode: *opcode,
                    destination: destination.clone(),
                    args,
                }
            },
            InstructionImms::BranchTable {
                opcode,
                destination,
                table,
            } => {
                InstructionData::BranchTable {
                    opcode: *opcode,
                    destination: destination.clone(),
                    table: table.clone(),
                    arg: values[0],
                }
            },
            InstructionImms::Call {
                opcode,
                func_ref,
            } => {
            let args = ValueList::from_slice(values, value_list);
                InstructionData::Call {
                    opcode: *opcode,
                    func_ref: func_ref.clone(),
                    args,
                }
            },
            InstructionImms::CallIndirect {
                opcode,
                sig_ref,
            } => {
            let args = ValueList::from_slice(values, value_list);
                InstructionData::CallIndirect {
                    opcode: *opcode,
                    sig_ref: sig_ref.clone(),
                    args,
                }
            },
            InstructionImms::CondTrap {
                opcode,
                code,
            } => {
                InstructionData::CondTrap {
                    opcode: *opcode,
                    code: code.clone(),
                    arg: values[0],
                }
            },
            InstructionImms::DynamicStackLoad {
                opcode,
                dynamic_stack_slot,
            } => {
                InstructionData::DynamicStackLoad {
                    opcode: *opcode,
                    dynamic_stack_slot: dynamic_stack_slot.clone(),
                }
            },
            InstructionImms::DynamicStackStore {
                opcode,
                dynamic_stack_slot,
            } => {
                InstructionData::DynamicStackStore {
                    opcode: *opcode,
                    dynamic_stack_slot: dynamic_stack_slot.clone(),
                    arg: values[0],
                }
            },
            InstructionImms::FloatCompare {
                opcode,
                cond,
            } => {
                InstructionData::FloatCompare {
                    opcode: *opcode,
                    cond: cond.clone(),
                    args: [values[0], values[1]],
                }
            },
            InstructionImms::FuncAddr {
                opcode,
                func_ref,
            } => {
                InstructionData::FuncAddr {
                    opcode: *opcode,
                    func_ref: func_ref.clone(),
                }
            },
            InstructionImms::HeapAddr {
                opcode,
                heap,
                offset,
                size,
            } => {
                InstructionData::HeapAddr {
                    opcode: *opcode,
                    heap: heap.clone(),
                    offset: offset.clone(),
                    size: size.clone(),
                    arg: values[0],
                }
            },
            InstructionImms::HeapLoad {
                opcode,
                heap_imm,
            } => {
                InstructionData::HeapLoad {
                    opcode: *opcode,
                    heap_imm: heap_imm.clone(),
                    arg: values[0],
                }
            },
            InstructionImms::HeapStore {
                opcode,
                heap_imm,
            } => {
                InstructionData::HeapStore {
                    opcode: *opcode,
                    heap_imm: heap_imm.clone(),
                    args: [values[0], values[1]],
                }
            },
            InstructionImms::IntAddTrap {
                opcode,
                code,
            } => {
                InstructionData::IntAddTrap {
                    opcode: *opcode,
                    code: code.clone(),
                    args: [values[0], values[1]],
                }
            },
            InstructionImms::IntCompare {
                opcode,
                cond,
            } => {
                InstructionData::IntCompare {
                    opcode: *opcode,
                    cond: cond.clone(),
                    args: [values[0], values[1]],
                }
            },
            InstructionImms::IntCompareImm {
                opcode,
                cond,
                imm,
            } => {
                InstructionData::IntCompareImm {
                    opcode: *opcode,
                    cond: cond.clone(),
                    imm: imm.clone(),
                    arg: values[0],
                }
            },
            InstructionImms::Jump {
                opcode,
                destination,
            } => {
            let args = ValueList::from_slice(values, value_list);
                InstructionData::Jump {
                    opcode: *opcode,
                    destination: destination.clone(),
                    args,
                }
            },
            InstructionImms::Load {
                opcode,
                flags,
                offset,
            } => {
                InstructionData::Load {
                    opcode: *opcode,
                    flags: flags.clone(),
                    offset: offset.clone(),
                    arg: values[0],
                }
            },
            InstructionImms::LoadNoOffset {
                opcode,
                flags,
            } => {
                InstructionData::LoadNoOffset {
                    opcode: *opcode,
                    flags: flags.clone(),
                    arg: values[0],
                }
            },
            InstructionImms::MultiAry {
                opcode,
            } => {
            let args = ValueList::from_slice(values, value_list);
                InstructionData::MultiAry {
                    opcode: *opcode,
                    args,
                }
            },
            InstructionImms::NullAry {
                opcode,
            } => {
                InstructionData::NullAry {
                    opcode: *opcode,
                }
            },
            InstructionImms::Shuffle {
                opcode,
                imm,
            } => {
                InstructionData::Shuffle {
                    opcode: *opcode,
                    imm: imm.clone(),
                    args: [values[0], values[1]],
                }
            },
            InstructionImms::StackLoad {
                opcode,
                stack_slot,
                offset,
            } => {
                InstructionData::StackLoad {
                    opcode: *opcode,
                    stack_slot: stack_slot.clone(),
                    offset: offset.clone(),
                }
            },
            InstructionImms::StackStore {
                opcode,
                stack_slot,
                offset,
            } => {
                InstructionData::StackStore {
                    opcode: *opcode,
                    stack_slot: stack_slot.clone(),
                    offset: offset.clone(),
                    arg: values[0],
                }
            },
            InstructionImms::Store {
                opcode,
                flags,
                offset,
            } => {
                InstructionData::Store {
                    opcode: *opcode,
                    flags: flags.clone(),
                    offset: offset.clone(),
                    args: [values[0], values[1]],
                }
            },
            InstructionImms::StoreNoOffset {
                opcode,
                flags,
            } => {
                InstructionData::StoreNoOffset {
                    opcode: *opcode,
                    flags: flags.clone(),
                    args: [values[0], values[1]],
                }
            },
            InstructionImms::TableAddr {
                opcode,
                table,
                offset,
            } => {
                InstructionData::TableAddr {
                    opcode: *opcode,
                    table: table.clone(),
                    offset: offset.clone(),
                    arg: values[0],
                }
            },
            InstructionImms::Ternary {
                opcode,
            } => {
                InstructionData::Ternary {
                    opcode: *opcode,
                    args: [values[0], values[1], values[2]],
                }
            },
            InstructionImms::TernaryImm8 {
                opcode,
                imm,
            } => {
                InstructionData::TernaryImm8 {
                    opcode: *opcode,
                    imm: imm.clone(),
                    args: [values[0], values[1]],
                }
            },
            InstructionImms::Trap {
                opcode,
                code,
            } => {
                InstructionData::Trap {
                    opcode: *opcode,
                    code: code.clone(),
                }
            },
            InstructionImms::Unary {
                opcode,
            } => {
                InstructionData::Unary {
                    opcode: *opcode,
                    arg: values[0],
                }
            },
            InstructionImms::UnaryConst {
                opcode,
                constant_handle,
            } => {
                InstructionData::UnaryConst {
                    opcode: *opcode,
                    constant_handle: constant_handle.clone(),
                }
            },
            InstructionImms::UnaryGlobalValue {
                opcode,
                global_value,
            } => {
                InstructionData::UnaryGlobalValue {
                    opcode: *opcode,
                    global_value: global_value.clone(),
                }
            },
            InstructionImms::UnaryIeee32 {
                opcode,
                imm,
            } => {
                InstructionData::UnaryIeee32 {
                    opcode: *opcode,
                    imm: imm.clone(),
                }
            },
            InstructionImms::UnaryIeee64 {
                opcode,
                imm,
            } => {
                InstructionData::UnaryIeee64 {
                    opcode: *opcode,
                    imm: imm.clone(),
                }
            },
            InstructionImms::UnaryImm {
                opcode,
                imm,
            } => {
                InstructionData::UnaryImm {
                    opcode: *opcode,
                    imm: imm.clone(),
                }
            },
        }
    }
}


/// An instruction opcode.
///
/// All instructions from all supported ISAs are present.
#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Eq, Debug, Hash)]
#[cfg_attr(
            feature = "enable-serde",
            derive(serde::Serialize, serde::Deserialize)
        )]
pub enum Opcode {
    /// `jump block, args`. (Jump)
    Jump = 1,
    /// `brz c, block, args`. (Branch)
    /// Type inferred from `c`.
    Brz,
    /// `brnz c, block, args`. (Branch)
    /// Type inferred from `c`.
    Brnz,
    /// `br_table x, block, JT`. (BranchTable)
    BrTable,
    /// `debugtrap`. (NullAry)
    Debugtrap,
    /// `trap code`. (Trap)
    Trap,
    /// `trapz c, code`. (CondTrap)
    /// Type inferred from `c`.
    Trapz,
    /// `resumable_trap code`. (Trap)
    ResumableTrap,
    /// `trapnz c, code`. (CondTrap)
    /// Type inferred from `c`.
    Trapnz,
    /// `resumable_trapnz c, code`. (CondTrap)
    /// Type inferred from `c`.
    ResumableTrapnz,
    /// `return rvals`. (MultiAry)
    Return,
    /// `rvals = call FN, args`. (Call)
    Call,
    /// `rvals = call_indirect SIG, callee, args`. (CallIndirect)
    /// Type inferred from `callee`.
    CallIndirect,
    /// `addr = func_addr FN`. (FuncAddr)
    FuncAddr,
    /// `a = splat x`. (Unary)
    Splat,
    /// `a = swizzle x, y`. (Binary)
    Swizzle,
    /// `a = insertlane x, y, Idx`. (TernaryImm8)
    /// Type inferred from `x`.
    Insertlane,
    /// `a = extractlane x, Idx`. (BinaryImm8)
    /// Type inferred from `x`.
    Extractlane,
    /// `a = smin x, y`. (Binary)
    /// Type inferred from `x`.
    Smin,
    /// `a = umin x, y`. (Binary)
    /// Type inferred from `x`.
    Umin,
    /// `a = smax x, y`. (Binary)
    /// Type inferred from `x`.
    Smax,
    /// `a = umax x, y`. (Binary)
    /// Type inferred from `x`.
    Umax,
    /// `a = avg_round x, y`. (Binary)
    /// Type inferred from `x`.
    AvgRound,
    /// `a = uadd_sat x, y`. (Binary)
    /// Type inferred from `x`.
    UaddSat,
    /// `a = sadd_sat x, y`. (Binary)
    /// Type inferred from `x`.
    SaddSat,
    /// `a = usub_sat x, y`. (Binary)
    /// Type inferred from `x`.
    UsubSat,
    /// `a = ssub_sat x, y`. (Binary)
    /// Type inferred from `x`.
    SsubSat,
    /// `a = load MemFlags, p, Offset`. (Load)
    Load,
    /// `store MemFlags, x, p, Offset`. (Store)
    /// Type inferred from `x`.
    Store,
    /// `a = uload8 MemFlags, p, Offset`. (Load)
    Uload8,
    /// `a = sload8 MemFlags, p, Offset`. (Load)
    Sload8,
    /// `istore8 MemFlags, x, p, Offset`. (Store)
    /// Type inferred from `x`.
    Istore8,
    /// `a = uload16 MemFlags, p, Offset`. (Load)
    Uload16,
    /// `a = sload16 MemFlags, p, Offset`. (Load)
    Sload16,
    /// `istore16 MemFlags, x, p, Offset`. (Store)
    /// Type inferred from `x`.
    Istore16,
    /// `a = uload32 MemFlags, p, Offset`. (Load)
    /// Type inferred from `p`.
    Uload32,
    /// `a = sload32 MemFlags, p, Offset`. (Load)
    /// Type inferred from `p`.
    Sload32,
    /// `istore32 MemFlags, x, p, Offset`. (Store)
    /// Type inferred from `x`.
    Istore32,
    /// `a = uload8x8 MemFlags, p, Offset`. (Load)
    /// Type inferred from `p`.
    Uload8x8,
    /// `a = sload8x8 MemFlags, p, Offset`. (Load)
    /// Type inferred from `p`.
    Sload8x8,
    /// `a = uload16x4 MemFlags, p, Offset`. (Load)
    /// Type inferred from `p`.
    Uload16x4,
    /// `a = sload16x4 MemFlags, p, Offset`. (Load)
    /// Type inferred from `p`.
    Sload16x4,
    /// `a = uload32x2 MemFlags, p, Offset`. (Load)
    /// Type inferred from `p`.
    Uload32x2,
    /// `a = sload32x2 MemFlags, p, Offset`. (Load)
    /// Type inferred from `p`.
    Sload32x2,
    /// `a = stack_load SS, Offset`. (StackLoad)
    StackLoad,
    /// `stack_store x, SS, Offset`. (StackStore)
    /// Type inferred from `x`.
    StackStore,
    /// `addr = stack_addr SS, Offset`. (StackLoad)
    StackAddr,
    /// `a = dynamic_stack_load DSS`. (DynamicStackLoad)
    DynamicStackLoad,
    /// `dynamic_stack_store x, DSS`. (DynamicStackStore)
    /// Type inferred from `x`.
    DynamicStackStore,
    /// `addr = dynamic_stack_addr DSS`. (DynamicStackLoad)
    DynamicStackAddr,
    /// `a = global_value GV`. (UnaryGlobalValue)
    GlobalValue,
    /// `a = symbol_value GV`. (UnaryGlobalValue)
    SymbolValue,
    /// `a = tls_value GV`. (UnaryGlobalValue)
    TlsValue,
    /// `addr = heap_addr H, index, Offset, Size`. (HeapAddr)
    HeapAddr,
    /// `a = heap_load heap_imm, index`. (HeapLoad)
    HeapLoad,
    /// `heap_store heap_imm, index, a`. (HeapStore)
    /// Type inferred from `index`.
    HeapStore,
    /// `addr = get_pinned_reg`. (NullAry)
    GetPinnedReg,
    /// `set_pinned_reg addr`. (Unary)
    /// Type inferred from `addr`.
    SetPinnedReg,
    /// `addr = get_frame_pointer`. (NullAry)
    GetFramePointer,
    /// `addr = get_stack_pointer`. (NullAry)
    GetStackPointer,
    /// `addr = get_return_address`. (NullAry)
    GetReturnAddress,
    /// `addr = table_addr T, p, Offset`. (TableAddr)
    TableAddr,
    /// `a = iconst N`. (UnaryImm)
    Iconst,
    /// `a = f32const N`. (UnaryIeee32)
    F32const,
    /// `a = f64const N`. (UnaryIeee64)
    F64const,
    /// `a = vconst N`. (UnaryConst)
    Vconst,
    /// `a = shuffle a, b, mask`. (Shuffle)
    Shuffle,
    /// `a = null`. (NullAry)
    Null,
    /// `nop`. (NullAry)
    Nop,
    /// `a = select c, x, y`. (Ternary)
    /// Type inferred from `x`.
    Select,
    /// `a = select_spectre_guard c, x, y`. (Ternary)
    /// Type inferred from `x`.
    SelectSpectreGuard,
    /// `a = bitselect c, x, y`. (Ternary)
    /// Type inferred from `x`.
    Bitselect,
    /// `lo, hi = vsplit x`. (Unary)
    /// Type inferred from `x`.
    Vsplit,
    /// `a = vconcat x, y`. (Binary)
    /// Type inferred from `x`.
    Vconcat,
    /// `a = vselect c, x, y`. (Ternary)
    /// Type inferred from `x`.
    Vselect,
    /// `s = vany_true a`. (Unary)
    /// Type inferred from `a`.
    VanyTrue,
    /// `s = vall_true a`. (Unary)
    /// Type inferred from `a`.
    VallTrue,
    /// `x = vhigh_bits a`. (Unary)
    VhighBits,
    /// `a = icmp Cond, x, y`. (IntCompare)
    /// Type inferred from `x`.
    Icmp,
    /// `a = icmp_imm Cond, x, Y`. (IntCompareImm)
    /// Type inferred from `x`.
    IcmpImm,
    /// `f = ifcmp x, y`. (Binary)
    /// Type inferred from `x`.
    Ifcmp,
    /// `f = ifcmp_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    IfcmpImm,
    /// `a = iadd x, y`. (Binary)
    /// Type inferred from `x`.
    Iadd,
    /// `a = isub x, y`. (Binary)
    /// Type inferred from `x`.
    Isub,
    /// `a = ineg x`. (Unary)
    /// Type inferred from `x`.
    Ineg,
    /// `a = iabs x`. (Unary)
    /// Type inferred from `x`.
    Iabs,
    /// `a = imul x, y`. (Binary)
    /// Type inferred from `x`.
    Imul,
    /// `a = umulhi x, y`. (Binary)
    /// Type inferred from `x`.
    Umulhi,
    /// `a = smulhi x, y`. (Binary)
    /// Type inferred from `x`.
    Smulhi,
    /// `a = sqmul_round_sat x, y`. (Binary)
    /// Type inferred from `x`.
    SqmulRoundSat,
    /// `a = udiv x, y`. (Binary)
    /// Type inferred from `x`.
    Udiv,
    /// `a = sdiv x, y`. (Binary)
    /// Type inferred from `x`.
    Sdiv,
    /// `a = urem x, y`. (Binary)
    /// Type inferred from `x`.
    Urem,
    /// `a = srem x, y`. (Binary)
    /// Type inferred from `x`.
    Srem,
    /// `a = iadd_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    IaddImm,
    /// `a = imul_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    ImulImm,
    /// `a = udiv_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    UdivImm,
    /// `a = sdiv_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    SdivImm,
    /// `a = urem_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    UremImm,
    /// `a = srem_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    SremImm,
    /// `a = irsub_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    IrsubImm,
    /// `a = iadd_cin x, y, c_in`. (Ternary)
    /// Type inferred from `y`.
    IaddCin,
    /// `a = iadd_ifcin x, y, c_in`. (Ternary)
    /// Type inferred from `y`.
    IaddIfcin,
    /// `a, c_out = iadd_cout x, y`. (Binary)
    /// Type inferred from `x`.
    IaddCout,
    /// `a, c_out = iadd_ifcout x, y`. (Binary)
    /// Type inferred from `x`.
    IaddIfcout,
    /// `a, c_out = iadd_carry x, y, c_in`. (Ternary)
    /// Type inferred from `y`.
    IaddCarry,
    /// `a, c_out = iadd_ifcarry x, y, c_in`. (Ternary)
    /// Type inferred from `y`.
    IaddIfcarry,
    /// `a = uadd_overflow_trap x, y, code`. (IntAddTrap)
    /// Type inferred from `x`.
    UaddOverflowTrap,
    /// `a = isub_bin x, y, b_in`. (Ternary)
    /// Type inferred from `y`.
    IsubBin,
    /// `a = isub_ifbin x, y, b_in`. (Ternary)
    /// Type inferred from `y`.
    IsubIfbin,
    /// `a, b_out = isub_bout x, y`. (Binary)
    /// Type inferred from `x`.
    IsubBout,
    /// `a, b_out = isub_ifbout x, y`. (Binary)
    /// Type inferred from `x`.
    IsubIfbout,
    /// `a, b_out = isub_borrow x, y, b_in`. (Ternary)
    /// Type inferred from `y`.
    IsubBorrow,
    /// `a, b_out = isub_ifborrow x, y, b_in`. (Ternary)
    /// Type inferred from `y`.
    IsubIfborrow,
    /// `a = band x, y`. (Binary)
    /// Type inferred from `x`.
    Band,
    /// `a = bor x, y`. (Binary)
    /// Type inferred from `x`.
    Bor,
    /// `a = bxor x, y`. (Binary)
    /// Type inferred from `x`.
    Bxor,
    /// `a = bnot x`. (Unary)
    /// Type inferred from `x`.
    Bnot,
    /// `a = band_not x, y`. (Binary)
    /// Type inferred from `x`.
    BandNot,
    /// `a = bor_not x, y`. (Binary)
    /// Type inferred from `x`.
    BorNot,
    /// `a = bxor_not x, y`. (Binary)
    /// Type inferred from `x`.
    BxorNot,
    /// `a = band_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    BandImm,
    /// `a = bor_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    BorImm,
    /// `a = bxor_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    BxorImm,
    /// `a = rotl x, y`. (Binary)
    /// Type inferred from `x`.
    Rotl,
    /// `a = rotr x, y`. (Binary)
    /// Type inferred from `x`.
    Rotr,
    /// `a = rotl_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    RotlImm,
    /// `a = rotr_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    RotrImm,
    /// `a = ishl x, y`. (Binary)
    /// Type inferred from `x`.
    Ishl,
    /// `a = ushr x, y`. (Binary)
    /// Type inferred from `x`.
    Ushr,
    /// `a = sshr x, y`. (Binary)
    /// Type inferred from `x`.
    Sshr,
    /// `a = ishl_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    IshlImm,
    /// `a = ushr_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    UshrImm,
    /// `a = sshr_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    SshrImm,
    /// `a = bitrev x`. (Unary)
    /// Type inferred from `x`.
    Bitrev,
    /// `a = clz x`. (Unary)
    /// Type inferred from `x`.
    Clz,
    /// `a = cls x`. (Unary)
    /// Type inferred from `x`.
    Cls,
    /// `a = ctz x`. (Unary)
    /// Type inferred from `x`.
    Ctz,
    /// `a = bswap x`. (Unary)
    /// Type inferred from `x`.
    Bswap,
    /// `a = popcnt x`. (Unary)
    /// Type inferred from `x`.
    Popcnt,
    /// `a = fcmp Cond, x, y`. (FloatCompare)
    /// Type inferred from `x`.
    Fcmp,
    /// `f = ffcmp x, y`. (Binary)
    /// Type inferred from `x`.
    Ffcmp,
    /// `a = fadd x, y`. (Binary)
    /// Type inferred from `x`.
    Fadd,
    /// `a = fsub x, y`. (Binary)
    /// Type inferred from `x`.
    Fsub,
    /// `a = fmul x, y`. (Binary)
    /// Type inferred from `x`.
    Fmul,
    /// `a = fdiv x, y`. (Binary)
    /// Type inferred from `x`.
    Fdiv,
    /// `a = sqrt x`. (Unary)
    /// Type inferred from `x`.
    Sqrt,
    /// `a = fma x, y, z`. (Ternary)
    /// Type inferred from `y`.
    Fma,
    /// `a = fneg x`. (Unary)
    /// Type inferred from `x`.
    Fneg,
    /// `a = fabs x`. (Unary)
    /// Type inferred from `x`.
    Fabs,
    /// `a = fcopysign x, y`. (Binary)
    /// Type inferred from `x`.
    Fcopysign,
    /// `a = fmin x, y`. (Binary)
    /// Type inferred from `x`.
    Fmin,
    /// `a = fmin_pseudo x, y`. (Binary)
    /// Type inferred from `x`.
    FminPseudo,
    /// `a = fmax x, y`. (Binary)
    /// Type inferred from `x`.
    Fmax,
    /// `a = fmax_pseudo x, y`. (Binary)
    /// Type inferred from `x`.
    FmaxPseudo,
    /// `a = ceil x`. (Unary)
    /// Type inferred from `x`.
    Ceil,
    /// `a = floor x`. (Unary)
    /// Type inferred from `x`.
    Floor,
    /// `a = trunc x`. (Unary)
    /// Type inferred from `x`.
    Trunc,
    /// `a = nearest x`. (Unary)
    /// Type inferred from `x`.
    Nearest,
    /// `a = is_null x`. (Unary)
    /// Type inferred from `x`.
    IsNull,
    /// `a = is_invalid x`. (Unary)
    /// Type inferred from `x`.
    IsInvalid,
    /// `a = bitcast MemFlags, x`. (LoadNoOffset)
    Bitcast,
    /// `a = scalar_to_vector s`. (Unary)
    ScalarToVector,
    /// `a = bmask x`. (Unary)
    Bmask,
    /// `a = ireduce x`. (Unary)
    Ireduce,
    /// `a = snarrow x, y`. (Binary)
    /// Type inferred from `x`.
    Snarrow,
    /// `a = unarrow x, y`. (Binary)
    /// Type inferred from `x`.
    Unarrow,
    /// `a = uunarrow x, y`. (Binary)
    /// Type inferred from `x`.
    Uunarrow,
    /// `a = swiden_low x`. (Unary)
    /// Type inferred from `x`.
    SwidenLow,
    /// `a = swiden_high x`. (Unary)
    /// Type inferred from `x`.
    SwidenHigh,
    /// `a = uwiden_low x`. (Unary)
    /// Type inferred from `x`.
    UwidenLow,
    /// `a = uwiden_high x`. (Unary)
    /// Type inferred from `x`.
    UwidenHigh,
    /// `a = iadd_pairwise x, y`. (Binary)
    /// Type inferred from `x`.
    IaddPairwise,
    /// `a = widening_pairwise_dot_product_s x, y`. (Binary)
    WideningPairwiseDotProductS,
    /// `a = uextend x`. (Unary)
    Uextend,
    /// `a = sextend x`. (Unary)
    Sextend,
    /// `a = fpromote x`. (Unary)
    Fpromote,
    /// `a = fdemote x`. (Unary)
    Fdemote,
    /// `a = fvdemote x`. (Unary)
    Fvdemote,
    /// `x = fvpromote_low a`. (Unary)
    FvpromoteLow,
    /// `a = fcvt_to_uint x`. (Unary)
    FcvtToUint,
    /// `a = fcvt_to_sint x`. (Unary)
    FcvtToSint,
    /// `a = fcvt_to_uint_sat x`. (Unary)
    FcvtToUintSat,
    /// `a = fcvt_to_sint_sat x`. (Unary)
    FcvtToSintSat,
    /// `a = fcvt_from_uint x`. (Unary)
    FcvtFromUint,
    /// `a = fcvt_from_sint x`. (Unary)
    FcvtFromSint,
    /// `a = fcvt_low_from_sint x`. (Unary)
    FcvtLowFromSint,
    /// `lo, hi = isplit x`. (Unary)
    /// Type inferred from `x`.
    Isplit,
    /// `a = iconcat lo, hi`. (Binary)
    /// Type inferred from `lo`.
    Iconcat,
    /// `a = atomic_rmw MemFlags, AtomicRmwOp, p, x`. (AtomicRmw)
    AtomicRmw,
    /// `a = atomic_cas MemFlags, p, e, x`. (AtomicCas)
    /// Type inferred from `x`.
    AtomicCas,
    /// `a = atomic_load MemFlags, p`. (LoadNoOffset)
    AtomicLoad,
    /// `atomic_store MemFlags, x, p`. (StoreNoOffset)
    /// Type inferred from `x`.
    AtomicStore,
    /// `fence`. (NullAry)
    Fence,
    /// `a = extract_vector x, y`. (BinaryImm8)
    /// Type inferred from `x`.
    ExtractVector,
}

impl Opcode {
    /// True for instructions that terminate the block
    pub fn is_terminator(self) -> bool {
        match self {
            Self::BrTable |
            Self::Jump |
            Self::Return |
            Self::Trap => {
                true
            }
            _ => {
                false
            }
        }
    }

    /// True for all branch or jump instructions.
    pub fn is_branch(self) -> bool {
        match self {
            Self::BrTable |
            Self::Brnz |
            Self::Brz |
            Self::Jump => {
                true
            }
            _ => {
                false
            }
        }
    }

    /// Is this a call instruction?
    pub fn is_call(self) -> bool {
        match self {
            Self::Call |
            Self::CallIndirect => {
                true
            }
            _ => {
                false
            }
        }
    }

    /// Is this a return instruction?
    pub fn is_return(self) -> bool {
        match self {
            Self::Return => {
                true
            }
            _ => {
                false
            }
        }
    }

    /// Can this instruction read from memory?
    pub fn can_load(self) -> bool {
        match self {
            Self::AtomicCas |
            Self::AtomicLoad |
            Self::AtomicRmw |
            Self::Debugtrap |
            Self::DynamicStackLoad |
            Self::HeapLoad |
            Self::Load |
            Self::Sload16 |
            Self::Sload16x4 |
            Self::Sload32 |
            Self::Sload32x2 |
            Self::Sload8 |
            Self::Sload8x8 |
            Self::StackLoad |
            Self::Uload16 |
            Self::Uload16x4 |
            Self::Uload32 |
            Self::Uload32x2 |
            Self::Uload8 |
            Self::Uload8x8 => {
                true
            }
            _ => {
                false
            }
        }
    }

    /// Can this instruction write to memory?
    pub fn can_store(self) -> bool {
        match self {
            Self::AtomicCas |
            Self::AtomicRmw |
            Self::AtomicStore |
            Self::Debugtrap |
            Self::DynamicStackStore |
            Self::HeapStore |
            Self::Istore16 |
            Self::Istore32 |
            Self::Istore8 |
            Self::StackStore |
            Self::Store => {
                true
            }
            _ => {
                false
            }
        }
    }

    /// Can this instruction cause a trap?
    pub fn can_trap(self) -> bool {
        match self {
            Self::FcvtToSint |
            Self::FcvtToUint |
            Self::HeapLoad |
            Self::HeapStore |
            Self::ResumableTrap |
            Self::ResumableTrapnz |
            Self::Sdiv |
            Self::Srem |
            Self::Trap |
            Self::Trapnz |
            Self::Trapz |
            Self::UaddOverflowTrap |
            Self::Udiv |
            Self::Urem => {
                true
            }
            _ => {
                false
            }
        }
    }

    /// Does this instruction have other side effects besides can_* flags?
    pub fn other_side_effects(self) -> bool {
        match self {
            Self::AtomicCas |
            Self::AtomicLoad |
            Self::AtomicRmw |
            Self::AtomicStore |
            Self::Debugtrap |
            Self::Fence |
            Self::GetPinnedReg |
            Self::SelectSpectreGuard |
            Self::SetPinnedReg => {
                true
            }
            _ => {
                false
            }
        }
    }

    /// Does this instruction write to CPU flags?
    pub fn writes_cpu_flags(self) -> bool {
        match self {
            Self::Ffcmp |
            Self::IaddIfcarry |
            Self::IaddIfcout |
            Self::Ifcmp |
            Self::IfcmpImm |
            Self::IsubIfborrow |
            Self::IsubIfbout => {
                true
            }
            _ => {
                false
            }
        }
    }

}

const OPCODE_FORMAT: [InstructionFormat; 195] = [
    InstructionFormat::Jump, // jump
    InstructionFormat::Branch, // brz
    InstructionFormat::Branch, // brnz
    InstructionFormat::BranchTable, // br_table
    InstructionFormat::NullAry, // debugtrap
    InstructionFormat::Trap, // trap
    InstructionFormat::CondTrap, // trapz
    InstructionFormat::Trap, // resumable_trap
    InstructionFormat::CondTrap, // trapnz
    InstructionFormat::CondTrap, // resumable_trapnz
    InstructionFormat::MultiAry, // return
    InstructionFormat::Call, // call
    InstructionFormat::CallIndirect, // call_indirect
    InstructionFormat::FuncAddr, // func_addr
    InstructionFormat::Unary, // splat
    InstructionFormat::Binary, // swizzle
    InstructionFormat::TernaryImm8, // insertlane
    InstructionFormat::BinaryImm8, // extractlane
    InstructionFormat::Binary, // smin
    InstructionFormat::Binary, // umin
    InstructionFormat::Binary, // smax
    InstructionFormat::Binary, // umax
    InstructionFormat::Binary, // avg_round
    InstructionFormat::Binary, // uadd_sat
    InstructionFormat::Binary, // sadd_sat
    InstructionFormat::Binary, // usub_sat
    InstructionFormat::Binary, // ssub_sat
    InstructionFormat::Load, // load
    InstructionFormat::Store, // store
    InstructionFormat::Load, // uload8
    InstructionFormat::Load, // sload8
    InstructionFormat::Store, // istore8
    InstructionFormat::Load, // uload16
    InstructionFormat::Load, // sload16
    InstructionFormat::Store, // istore16
    InstructionFormat::Load, // uload32
    InstructionFormat::Load, // sload32
    InstructionFormat::Store, // istore32
    InstructionFormat::Load, // uload8x8
    InstructionFormat::Load, // sload8x8
    InstructionFormat::Load, // uload16x4
    InstructionFormat::Load, // sload16x4
    InstructionFormat::Load, // uload32x2
    InstructionFormat::Load, // sload32x2
    InstructionFormat::StackLoad, // stack_load
    InstructionFormat::StackStore, // stack_store
    InstructionFormat::StackLoad, // stack_addr
    InstructionFormat::DynamicStackLoad, // dynamic_stack_load
    InstructionFormat::DynamicStackStore, // dynamic_stack_store
    InstructionFormat::DynamicStackLoad, // dynamic_stack_addr
    InstructionFormat::UnaryGlobalValue, // global_value
    InstructionFormat::UnaryGlobalValue, // symbol_value
    InstructionFormat::UnaryGlobalValue, // tls_value
    InstructionFormat::HeapAddr, // heap_addr
    InstructionFormat::HeapLoad, // heap_load
    InstructionFormat::HeapStore, // heap_store
    InstructionFormat::NullAry, // get_pinned_reg
    InstructionFormat::Unary, // set_pinned_reg
    InstructionFormat::NullAry, // get_frame_pointer
    InstructionFormat::NullAry, // get_stack_pointer
    InstructionFormat::NullAry, // get_return_address
    InstructionFormat::TableAddr, // table_addr
    InstructionFormat::UnaryImm, // iconst
    InstructionFormat::UnaryIeee32, // f32const
    InstructionFormat::UnaryIeee64, // f64const
    InstructionFormat::UnaryConst, // vconst
    InstructionFormat::Shuffle, // shuffle
    InstructionFormat::NullAry, // null
    InstructionFormat::NullAry, // nop
    InstructionFormat::Ternary, // select
    InstructionFormat::Ternary, // select_spectre_guard
    InstructionFormat::Ternary, // bitselect
    InstructionFormat::Unary, // vsplit
    InstructionFormat::Binary, // vconcat
    InstructionFormat::Ternary, // vselect
    InstructionFormat::Unary, // vany_true
    InstructionFormat::Unary, // vall_true
    InstructionFormat::Unary, // vhigh_bits
    InstructionFormat::IntCompare, // icmp
    InstructionFormat::IntCompareImm, // icmp_imm
    InstructionFormat::Binary, // ifcmp
    InstructionFormat::BinaryImm64, // ifcmp_imm
    InstructionFormat::Binary, // iadd
    InstructionFormat::Binary, // isub
    InstructionFormat::Unary, // ineg
    InstructionFormat::Unary, // iabs
    InstructionFormat::Binary, // imul
    InstructionFormat::Binary, // umulhi
    InstructionFormat::Binary, // smulhi
    InstructionFormat::Binary, // sqmul_round_sat
    InstructionFormat::Binary, // udiv
    InstructionFormat::Binary, // sdiv
    InstructionFormat::Binary, // urem
    InstructionFormat::Binary, // srem
    InstructionFormat::BinaryImm64, // iadd_imm
    InstructionFormat::BinaryImm64, // imul_imm
    InstructionFormat::BinaryImm64, // udiv_imm
    InstructionFormat::BinaryImm64, // sdiv_imm
    InstructionFormat::BinaryImm64, // urem_imm
    InstructionFormat::BinaryImm64, // srem_imm
    InstructionFormat::BinaryImm64, // irsub_imm
    InstructionFormat::Ternary, // iadd_cin
    InstructionFormat::Ternary, // iadd_ifcin
    InstructionFormat::Binary, // iadd_cout
    InstructionFormat::Binary, // iadd_ifcout
    InstructionFormat::Ternary, // iadd_carry
    InstructionFormat::Ternary, // iadd_ifcarry
    InstructionFormat::IntAddTrap, // uadd_overflow_trap
    InstructionFormat::Ternary, // isub_bin
    InstructionFormat::Ternary, // isub_ifbin
    InstructionFormat::Binary, // isub_bout
    InstructionFormat::Binary, // isub_ifbout
    InstructionFormat::Ternary, // isub_borrow
    InstructionFormat::Ternary, // isub_ifborrow
    InstructionFormat::Binary, // band
    InstructionFormat::Binary, // bor
    InstructionFormat::Binary, // bxor
    InstructionFormat::Unary, // bnot
    InstructionFormat::Binary, // band_not
    InstructionFormat::Binary, // bor_not
    InstructionFormat::Binary, // bxor_not
    InstructionFormat::BinaryImm64, // band_imm
    InstructionFormat::BinaryImm64, // bor_imm
    InstructionFormat::BinaryImm64, // bxor_imm
    InstructionFormat::Binary, // rotl
    InstructionFormat::Binary, // rotr
    InstructionFormat::BinaryImm64, // rotl_imm
    InstructionFormat::BinaryImm64, // rotr_imm
    InstructionFormat::Binary, // ishl
    InstructionFormat::Binary, // ushr
    InstructionFormat::Binary, // sshr
    InstructionFormat::BinaryImm64, // ishl_imm
    InstructionFormat::BinaryImm64, // ushr_imm
    InstructionFormat::BinaryImm64, // sshr_imm
    InstructionFormat::Unary, // bitrev
    InstructionFormat::Unary, // clz
    InstructionFormat::Unary, // cls
    InstructionFormat::Unary, // ctz
    InstructionFormat::Unary, // bswap
    InstructionFormat::Unary, // popcnt
    InstructionFormat::FloatCompare, // fcmp
    InstructionFormat::Binary, // ffcmp
    InstructionFormat::Binary, // fadd
    InstructionFormat::Binary, // fsub
    InstructionFormat::Binary, // fmul
    InstructionFormat::Binary, // fdiv
    InstructionFormat::Unary, // sqrt
    InstructionFormat::Ternary, // fma
    InstructionFormat::Unary, // fneg
    InstructionFormat::Unary, // fabs
    InstructionFormat::Binary, // fcopysign
    InstructionFormat::Binary, // fmin
    InstructionFormat::Binary, // fmin_pseudo
    InstructionFormat::Binary, // fmax
    InstructionFormat::Binary, // fmax_pseudo
    InstructionFormat::Unary, // ceil
    InstructionFormat::Unary, // floor
    InstructionFormat::Unary, // trunc
    InstructionFormat::Unary, // nearest
    InstructionFormat::Unary, // is_null
    InstructionFormat::Unary, // is_invalid
    InstructionFormat::LoadNoOffset, // bitcast
    InstructionFormat::Unary, // scalar_to_vector
    InstructionFormat::Unary, // bmask
    InstructionFormat::Unary, // ireduce
    InstructionFormat::Binary, // snarrow
    InstructionFormat::Binary, // unarrow
    InstructionFormat::Binary, // uunarrow
    InstructionFormat::Unary, // swiden_low
    InstructionFormat::Unary, // swiden_high
    InstructionFormat::Unary, // uwiden_low
    InstructionFormat::Unary, // uwiden_high
    InstructionFormat::Binary, // iadd_pairwise
    InstructionFormat::Binary, // widening_pairwise_dot_product_s
    InstructionFormat::Unary, // uextend
    InstructionFormat::Unary, // sextend
    InstructionFormat::Unary, // fpromote
    InstructionFormat::Unary, // fdemote
    InstructionFormat::Unary, // fvdemote
    InstructionFormat::Unary, // fvpromote_low
    InstructionFormat::Unary, // fcvt_to_uint
    InstructionFormat::Unary, // fcvt_to_sint
    InstructionFormat::Unary, // fcvt_to_uint_sat
    InstructionFormat::Unary, // fcvt_to_sint_sat
    InstructionFormat::Unary, // fcvt_from_uint
    InstructionFormat::Unary, // fcvt_from_sint
    InstructionFormat::Unary, // fcvt_low_from_sint
    InstructionFormat::Unary, // isplit
    InstructionFormat::Binary, // iconcat
    InstructionFormat::AtomicRmw, // atomic_rmw
    InstructionFormat::AtomicCas, // atomic_cas
    InstructionFormat::LoadNoOffset, // atomic_load
    InstructionFormat::StoreNoOffset, // atomic_store
    InstructionFormat::NullAry, // fence
    InstructionFormat::BinaryImm8, // extract_vector
];

fn opcode_name(opc: Opcode) -> &'static str {
    match opc {
        Opcode::AtomicCas => {
            "atomic_cas"
        }
        Opcode::AtomicLoad => {
            "atomic_load"
        }
        Opcode::AtomicRmw => {
            "atomic_rmw"
        }
        Opcode::AtomicStore => {
            "atomic_store"
        }
        Opcode::AvgRound => {
            "avg_round"
        }
        Opcode::Band => {
            "band"
        }
        Opcode::BandImm => {
            "band_imm"
        }
        Opcode::BandNot => {
            "band_not"
        }
        Opcode::Bitcast => {
            "bitcast"
        }
        Opcode::Bitrev => {
            "bitrev"
        }
        Opcode::Bitselect => {
            "bitselect"
        }
        Opcode::Bmask => {
            "bmask"
        }
        Opcode::Bnot => {
            "bnot"
        }
        Opcode::Bor => {
            "bor"
        }
        Opcode::BorImm => {
            "bor_imm"
        }
        Opcode::BorNot => {
            "bor_not"
        }
        Opcode::BrTable => {
            "br_table"
        }
        Opcode::Brnz => {
            "brnz"
        }
        Opcode::Brz => {
            "brz"
        }
        Opcode::Bswap => {
            "bswap"
        }
        Opcode::Bxor => {
            "bxor"
        }
        Opcode::BxorImm => {
            "bxor_imm"
        }
        Opcode::BxorNot => {
            "bxor_not"
        }
        Opcode::Call => {
            "call"
        }
        Opcode::CallIndirect => {
            "call_indirect"
        }
        Opcode::Ceil => {
            "ceil"
        }
        Opcode::Cls => {
            "cls"
        }
        Opcode::Clz => {
            "clz"
        }
        Opcode::Ctz => {
            "ctz"
        }
        Opcode::Debugtrap => {
            "debugtrap"
        }
        Opcode::DynamicStackAddr => {
            "dynamic_stack_addr"
        }
        Opcode::DynamicStackLoad => {
            "dynamic_stack_load"
        }
        Opcode::DynamicStackStore => {
            "dynamic_stack_store"
        }
        Opcode::ExtractVector => {
            "extract_vector"
        }
        Opcode::Extractlane => {
            "extractlane"
        }
        Opcode::F32const => {
            "f32const"
        }
        Opcode::F64const => {
            "f64const"
        }
        Opcode::Fabs => {
            "fabs"
        }
        Opcode::Fadd => {
            "fadd"
        }
        Opcode::Fcmp => {
            "fcmp"
        }
        Opcode::Fcopysign => {
            "fcopysign"
        }
        Opcode::FcvtFromSint => {
            "fcvt_from_sint"
        }
        Opcode::FcvtFromUint => {
            "fcvt_from_uint"
        }
        Opcode::FcvtLowFromSint => {
            "fcvt_low_from_sint"
        }
        Opcode::FcvtToSint => {
            "fcvt_to_sint"
        }
        Opcode::FcvtToSintSat => {
            "fcvt_to_sint_sat"
        }
        Opcode::FcvtToUint => {
            "fcvt_to_uint"
        }
        Opcode::FcvtToUintSat => {
            "fcvt_to_uint_sat"
        }
        Opcode::Fdemote => {
            "fdemote"
        }
        Opcode::Fdiv => {
            "fdiv"
        }
        Opcode::Fence => {
            "fence"
        }
        Opcode::Ffcmp => {
            "ffcmp"
        }
        Opcode::Floor => {
            "floor"
        }
        Opcode::Fma => {
            "fma"
        }
        Opcode::Fmax => {
            "fmax"
        }
        Opcode::FmaxPseudo => {
            "fmax_pseudo"
        }
        Opcode::Fmin => {
            "fmin"
        }
        Opcode::FminPseudo => {
            "fmin_pseudo"
        }
        Opcode::Fmul => {
            "fmul"
        }
        Opcode::Fneg => {
            "fneg"
        }
        Opcode::Fpromote => {
            "fpromote"
        }
        Opcode::Fsub => {
            "fsub"
        }
        Opcode::FuncAddr => {
            "func_addr"
        }
        Opcode::Fvdemote => {
            "fvdemote"
        }
        Opcode::FvpromoteLow => {
            "fvpromote_low"
        }
        Opcode::GetFramePointer => {
            "get_frame_pointer"
        }
        Opcode::GetPinnedReg => {
            "get_pinned_reg"
        }
        Opcode::GetReturnAddress => {
            "get_return_address"
        }
        Opcode::GetStackPointer => {
            "get_stack_pointer"
        }
        Opcode::GlobalValue => {
            "global_value"
        }
        Opcode::HeapAddr => {
            "heap_addr"
        }
        Opcode::HeapLoad => {
            "heap_load"
        }
        Opcode::HeapStore => {
            "heap_store"
        }
        Opcode::Iabs => {
            "iabs"
        }
        Opcode::Iadd => {
            "iadd"
        }
        Opcode::IaddCarry => {
            "iadd_carry"
        }
        Opcode::IaddCin => {
            "iadd_cin"
        }
        Opcode::IaddCout => {
            "iadd_cout"
        }
        Opcode::IaddIfcarry => {
            "iadd_ifcarry"
        }
        Opcode::IaddIfcin => {
            "iadd_ifcin"
        }
        Opcode::IaddIfcout => {
            "iadd_ifcout"
        }
        Opcode::IaddImm => {
            "iadd_imm"
        }
        Opcode::IaddPairwise => {
            "iadd_pairwise"
        }
        Opcode::Icmp => {
            "icmp"
        }
        Opcode::IcmpImm => {
            "icmp_imm"
        }
        Opcode::Iconcat => {
            "iconcat"
        }
        Opcode::Iconst => {
            "iconst"
        }
        Opcode::Ifcmp => {
            "ifcmp"
        }
        Opcode::IfcmpImm => {
            "ifcmp_imm"
        }
        Opcode::Imul => {
            "imul"
        }
        Opcode::ImulImm => {
            "imul_imm"
        }
        Opcode::Ineg => {
            "ineg"
        }
        Opcode::Insertlane => {
            "insertlane"
        }
        Opcode::Ireduce => {
            "ireduce"
        }
        Opcode::IrsubImm => {
            "irsub_imm"
        }
        Opcode::IsInvalid => {
            "is_invalid"
        }
        Opcode::IsNull => {
            "is_null"
        }
        Opcode::Ishl => {
            "ishl"
        }
        Opcode::IshlImm => {
            "ishl_imm"
        }
        Opcode::Isplit => {
            "isplit"
        }
        Opcode::Istore16 => {
            "istore16"
        }
        Opcode::Istore32 => {
            "istore32"
        }
        Opcode::Istore8 => {
            "istore8"
        }
        Opcode::Isub => {
            "isub"
        }
        Opcode::IsubBin => {
            "isub_bin"
        }
        Opcode::IsubBorrow => {
            "isub_borrow"
        }
        Opcode::IsubBout => {
            "isub_bout"
        }
        Opcode::IsubIfbin => {
            "isub_ifbin"
        }
        Opcode::IsubIfborrow => {
            "isub_ifborrow"
        }
        Opcode::IsubIfbout => {
            "isub_ifbout"
        }
        Opcode::Jump => {
            "jump"
        }
        Opcode::Load => {
            "load"
        }
        Opcode::Nearest => {
            "nearest"
        }
        Opcode::Nop => {
            "nop"
        }
        Opcode::Null => {
            "null"
        }
        Opcode::Popcnt => {
            "popcnt"
        }
        Opcode::ResumableTrap => {
            "resumable_trap"
        }
        Opcode::ResumableTrapnz => {
            "resumable_trapnz"
        }
        Opcode::Return => {
            "return"
        }
        Opcode::Rotl => {
            "rotl"
        }
        Opcode::RotlImm => {
            "rotl_imm"
        }
        Opcode::Rotr => {
            "rotr"
        }
        Opcode::RotrImm => {
            "rotr_imm"
        }
        Opcode::SaddSat => {
            "sadd_sat"
        }
        Opcode::ScalarToVector => {
            "scalar_to_vector"
        }
        Opcode::Sdiv => {
            "sdiv"
        }
        Opcode::SdivImm => {
            "sdiv_imm"
        }
        Opcode::Select => {
            "select"
        }
        Opcode::SelectSpectreGuard => {
            "select_spectre_guard"
        }
        Opcode::SetPinnedReg => {
            "set_pinned_reg"
        }
        Opcode::Sextend => {
            "sextend"
        }
        Opcode::Shuffle => {
            "shuffle"
        }
        Opcode::Sload16 => {
            "sload16"
        }
        Opcode::Sload16x4 => {
            "sload16x4"
        }
        Opcode::Sload32 => {
            "sload32"
        }
        Opcode::Sload32x2 => {
            "sload32x2"
        }
        Opcode::Sload8 => {
            "sload8"
        }
        Opcode::Sload8x8 => {
            "sload8x8"
        }
        Opcode::Smax => {
            "smax"
        }
        Opcode::Smin => {
            "smin"
        }
        Opcode::Smulhi => {
            "smulhi"
        }
        Opcode::Snarrow => {
            "snarrow"
        }
        Opcode::Splat => {
            "splat"
        }
        Opcode::SqmulRoundSat => {
            "sqmul_round_sat"
        }
        Opcode::Sqrt => {
            "sqrt"
        }
        Opcode::Srem => {
            "srem"
        }
        Opcode::SremImm => {
            "srem_imm"
        }
        Opcode::Sshr => {
            "sshr"
        }
        Opcode::SshrImm => {
            "sshr_imm"
        }
        Opcode::SsubSat => {
            "ssub_sat"
        }
        Opcode::StackAddr => {
            "stack_addr"
        }
        Opcode::StackLoad => {
            "stack_load"
        }
        Opcode::StackStore => {
            "stack_store"
        }
        Opcode::Store => {
            "store"
        }
        Opcode::SwidenHigh => {
            "swiden_high"
        }
        Opcode::SwidenLow => {
            "swiden_low"
        }
        Opcode::Swizzle => {
            "swizzle"
        }
        Opcode::SymbolValue => {
            "symbol_value"
        }
        Opcode::TableAddr => {
            "table_addr"
        }
        Opcode::TlsValue => {
            "tls_value"
        }
        Opcode::Trap => {
            "trap"
        }
        Opcode::Trapnz => {
            "trapnz"
        }
        Opcode::Trapz => {
            "trapz"
        }
        Opcode::Trunc => {
            "trunc"
        }
        Opcode::UaddOverflowTrap => {
            "uadd_overflow_trap"
        }
        Opcode::UaddSat => {
            "uadd_sat"
        }
        Opcode::Udiv => {
            "udiv"
        }
        Opcode::UdivImm => {
            "udiv_imm"
        }
        Opcode::Uextend => {
            "uextend"
        }
        Opcode::Uload16 => {
            "uload16"
        }
        Opcode::Uload16x4 => {
            "uload16x4"
        }
        Opcode::Uload32 => {
            "uload32"
        }
        Opcode::Uload32x2 => {
            "uload32x2"
        }
        Opcode::Uload8 => {
            "uload8"
        }
        Opcode::Uload8x8 => {
            "uload8x8"
        }
        Opcode::Umax => {
            "umax"
        }
        Opcode::Umin => {
            "umin"
        }
        Opcode::Umulhi => {
            "umulhi"
        }
        Opcode::Unarrow => {
            "unarrow"
        }
        Opcode::Urem => {
            "urem"
        }
        Opcode::UremImm => {
            "urem_imm"
        }
        Opcode::Ushr => {
            "ushr"
        }
        Opcode::UshrImm => {
            "ushr_imm"
        }
        Opcode::UsubSat => {
            "usub_sat"
        }
        Opcode::Uunarrow => {
            "uunarrow"
        }
        Opcode::UwidenHigh => {
            "uwiden_high"
        }
        Opcode::UwidenLow => {
            "uwiden_low"
        }
        Opcode::VallTrue => {
            "vall_true"
        }
        Opcode::VanyTrue => {
            "vany_true"
        }
        Opcode::Vconcat => {
            "vconcat"
        }
        Opcode::Vconst => {
            "vconst"
        }
        Opcode::VhighBits => {
            "vhigh_bits"
        }
        Opcode::Vselect => {
            "vselect"
        }
        Opcode::Vsplit => {
            "vsplit"
        }
        Opcode::WideningPairwiseDotProductS => {
            "widening_pairwise_dot_product_s"
        }
    }
}

const OPCODE_HASH_TABLE: [Option<Opcode>; 256] = [
    Some(Opcode::Imul),
    Some(Opcode::TlsValue),
    None,
    Some(Opcode::HeapAddr),
    Some(Opcode::Bswap),
    Some(Opcode::FcvtToSintSat),
    Some(Opcode::Fsub),
    Some(Opcode::Rotr),
    Some(Opcode::TableAddr),
    Some(Opcode::Iconst),
    Some(Opcode::Umin),
    Some(Opcode::Ifcmp),
    Some(Opcode::FcvtLowFromSint),
    Some(Opcode::Store),
    Some(Opcode::Brnz),
    Some(Opcode::GetFramePointer),
    Some(Opcode::Isub),
    Some(Opcode::UshrImm),
    Some(Opcode::Trap),
    Some(Opcode::Sdiv),
    Some(Opcode::Srem),
    Some(Opcode::SshrImm),
    Some(Opcode::Uunarrow),
    Some(Opcode::Urem),
    Some(Opcode::IsubIfborrow),
    Some(Opcode::IaddIfcarry),
    Some(Opcode::Bxor),
    Some(Opcode::Umax),
    Some(Opcode::SremImm),
    Some(Opcode::Insertlane),
    Some(Opcode::BxorNot),
    Some(Opcode::Swizzle),
    Some(Opcode::Load),
    Some(Opcode::Fadd),
    Some(Opcode::Jump),
    Some(Opcode::Null),
    Some(Opcode::Shuffle),
    Some(Opcode::Fneg),
    Some(Opcode::Umulhi),
    Some(Opcode::Ushr),
    None,
    Some(Opcode::UaddOverflowTrap),
    Some(Opcode::FcvtFromUint),
    Some(Opcode::VallTrue),
    Some(Opcode::Band),
    Some(Opcode::BxorImm),
    Some(Opcode::Fmax),
    Some(Opcode::Uload16x4),
    Some(Opcode::Ishl),
    None,
    Some(Opcode::Vconst),
    Some(Opcode::Call),
    Some(Opcode::ExtractVector),
    Some(Opcode::Sqrt),
    None,
    None,
    Some(Opcode::Ceil),
    Some(Opcode::Ineg),
    Some(Opcode::FuncAddr),
    Some(Opcode::SaddSat),
    Some(Opcode::Popcnt),
    None,
    Some(Opcode::Fabs),
    Some(Opcode::Fmin),
    None,
    Some(Opcode::GlobalValue),
    Some(Opcode::Bnot),
    Some(Opcode::FmaxPseudo),
    Some(Opcode::Isplit),
    Some(Opcode::Nearest),
    Some(Opcode::Trunc),
    None,
    Some(Opcode::RotlImm),
    Some(Opcode::Fcmp),
    Some(Opcode::SwidenHigh),
    Some(Opcode::IaddIfcin),
    Some(Opcode::Fmul),
    None,
    Some(Opcode::IsubBin),
    Some(Opcode::Uload8x8),
    Some(Opcode::FcvtToUint),
    Some(Opcode::ResumableTrapnz),
    Some(Opcode::Fdiv),
    Some(Opcode::FcvtToSint),
    None,
    Some(Opcode::Sextend),
    Some(Opcode::UremImm),
    Some(Opcode::AtomicLoad),
    None,
    Some(Opcode::Trapnz),
    Some(Opcode::Uload16),
    Some(Opcode::IaddImm),
    Some(Opcode::Uload32),
    Some(Opcode::HeapStore),
    Some(Opcode::Bitrev),
    Some(Opcode::IaddCarry),
    Some(Opcode::Smulhi),
    Some(Opcode::IsNull),
    Some(Opcode::Vsplit),
    None,
    None,
    None,
    None,
    Some(Opcode::BorNot),
    None,
    None,
    Some(Opcode::Sload8x8),
    Some(Opcode::IsubIfbout),
    Some(Opcode::FcvtFromSint),
    None,
    None,
    Some(Opcode::SetPinnedReg),
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::ImulImm),
    Some(Opcode::Ireduce),
    None,
    Some(Opcode::RotrImm),
    Some(Opcode::DynamicStackStore),
    Some(Opcode::StackStore),
    Some(Opcode::UwidenLow),
    Some(Opcode::Select),
    Some(Opcode::IaddCin),
    Some(Opcode::Istore32),
    Some(Opcode::FvpromoteLow),
    Some(Opcode::Istore16),
    None,
    Some(Opcode::Fdemote),
    Some(Opcode::BorImm),
    None,
    Some(Opcode::IcmpImm),
    Some(Opcode::Fvdemote),
    None,
    Some(Opcode::Sload16),
    Some(Opcode::Fcopysign),
    None,
    Some(Opcode::SdivImm),
    Some(Opcode::ResumableTrap),
    Some(Opcode::AvgRound),
    Some(Opcode::Sload32),
    Some(Opcode::Unarrow),
    None,
    Some(Opcode::Extractlane),
    Some(Opcode::StackAddr),
    None,
    Some(Opcode::BandImm),
    Some(Opcode::IsubBorrow),
    Some(Opcode::Return),
    None,
    Some(Opcode::Uload32x2),
    None,
    None,
    Some(Opcode::VanyTrue),
    Some(Opcode::IsubIfbin),
    Some(Opcode::UsubSat),
    None,
    None,
    None,
    None,
    Some(Opcode::DynamicStackLoad),
    Some(Opcode::Iconcat),
    Some(Opcode::Fence),
    None,
    None,
    None,
    None,
    Some(Opcode::Fma),
    Some(Opcode::Bitselect),
    Some(Opcode::Istore8),
    Some(Opcode::BrTable),
    Some(Opcode::F64const),
    Some(Opcode::Nop),
    Some(Opcode::StackLoad),
    Some(Opcode::IrsubImm),
    Some(Opcode::IaddIfcout),
    Some(Opcode::SqmulRoundSat),
    Some(Opcode::IsubBout),
    Some(Opcode::Debugtrap),
    Some(Opcode::Sload16x4),
    Some(Opcode::Bor),
    Some(Opcode::IshlImm),
    Some(Opcode::BandNot),
    Some(Opcode::Ctz),
    Some(Opcode::Cls),
    Some(Opcode::Ffcmp),
    Some(Opcode::UwidenHigh),
    Some(Opcode::Brz),
    None,
    Some(Opcode::Uextend),
    Some(Opcode::Floor),
    Some(Opcode::UaddSat),
    Some(Opcode::Sload32x2),
    Some(Opcode::Clz),
    None,
    Some(Opcode::SelectSpectreGuard),
    Some(Opcode::Fpromote),
    None,
    Some(Opcode::Bitcast),
    None,
    Some(Opcode::SymbolValue),
    Some(Opcode::DynamicStackAddr),
    Some(Opcode::Bmask),
    Some(Opcode::GetPinnedReg),
    Some(Opcode::SsubSat),
    Some(Opcode::Vselect),
    None,
    Some(Opcode::ScalarToVector),
    Some(Opcode::AtomicRmw),
    None,
    Some(Opcode::Uload8),
    Some(Opcode::FcvtToUintSat),
    None,
    Some(Opcode::Smin),
    Some(Opcode::Trapz),
    Some(Opcode::Iabs),
    Some(Opcode::Udiv),
    None,
    Some(Opcode::AtomicCas),
    Some(Opcode::GetReturnAddress),
    None,
    Some(Opcode::SwidenLow),
    None,
    Some(Opcode::Rotl),
    Some(Opcode::IaddPairwise),
    None,
    None,
    Some(Opcode::Smax),
    Some(Opcode::FminPseudo),
    None,
    Some(Opcode::F32const),
    Some(Opcode::HeapLoad),
    Some(Opcode::WideningPairwiseDotProductS),
    Some(Opcode::Splat),
    Some(Opcode::IaddCout),
    Some(Opcode::Snarrow),
    Some(Opcode::CallIndirect),
    Some(Opcode::Sload8),
    None,
    Some(Opcode::VhighBits),
    Some(Opcode::UdivImm),
    None,
    Some(Opcode::IfcmpImm),
    Some(Opcode::Icmp),
    None,
    Some(Opcode::IsInvalid),
    Some(Opcode::Iadd),
    None,
    Some(Opcode::GetStackPointer),
    Some(Opcode::Vconcat),
    None,
    Some(Opcode::Sshr),
    Some(Opcode::AtomicStore),
    None,
];


// Table of opcode constraints.
const OPCODE_CONSTRAINTS: [OpcodeConstraints; 195] = [
    // Jump: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // Brz: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // Brnz: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // BrTable: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Concrete(ir::types::I32)']
    OpcodeConstraints {
        flags: 0x20,
        typeset_offset: 255,
        constraint_offset: 3,
    },
    // Debugtrap: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // Trap: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // Trapz: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // ResumableTrap: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // Trapnz: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // ResumableTrapnz: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // Return: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // Call: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // CallIndirect: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // FuncAddr: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // Splat: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'LaneOf']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 2,
        constraint_offset: 4,
    },
    // Swizzle: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Concrete(ir::types::I8X16)', 'Concrete(ir::types::I8X16)']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x41,
        typeset_offset: 2,
        constraint_offset: 6,
    },
    // Insertlane: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'LaneOf']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 2,
        constraint_offset: 9,
    },
    // Extractlane: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['LaneOf', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 2,
        constraint_offset: 5,
    },
    // Smin: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 3,
        constraint_offset: 0,
    },
    // Umin: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 3,
        constraint_offset: 0,
    },
    // Smax: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 3,
        constraint_offset: 0,
    },
    // Umax: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 3,
        constraint_offset: 0,
    },
    // AvgRound: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // UaddSat: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // SaddSat: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // UsubSat: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // SsubSat: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // Load: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(1)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 5,
        constraint_offset: 12,
    },
    // Store: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['Same', 'Free(1)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x58,
        typeset_offset: 5,
        constraint_offset: 12,
    },
    // Uload8: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(1)']
    // Polymorphic over TypeSet(lanes={1}, ints={16, 32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 6,
        constraint_offset: 12,
    },
    // Sload8: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(1)']
    // Polymorphic over TypeSet(lanes={1}, ints={16, 32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 6,
        constraint_offset: 12,
    },
    // Istore8: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['Same', 'Free(1)']
    // Polymorphic over TypeSet(lanes={1}, ints={16, 32, 64})
    OpcodeConstraints {
        flags: 0x58,
        typeset_offset: 6,
        constraint_offset: 12,
    },
    // Uload16: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(1)']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 7,
        constraint_offset: 12,
    },
    // Sload16: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(1)']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 7,
        constraint_offset: 12,
    },
    // Istore16: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['Same', 'Free(1)']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64})
    OpcodeConstraints {
        flags: 0x58,
        typeset_offset: 7,
        constraint_offset: 12,
    },
    // Uload32: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I64)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 1,
        constraint_offset: 14,
    },
    // Sload32: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I64)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 1,
        constraint_offset: 14,
    },
    // Istore32: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['Concrete(ir::types::I64)', 'Free(1)']
    // Polymorphic over TypeSet(lanes={1}, ints={64})
    OpcodeConstraints {
        flags: 0x58,
        typeset_offset: 8,
        constraint_offset: 16,
    },
    // Uload8x8: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I16X8)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 1,
        constraint_offset: 18,
    },
    // Sload8x8: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I16X8)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 1,
        constraint_offset: 18,
    },
    // Uload16x4: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I32X4)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 1,
        constraint_offset: 20,
    },
    // Sload16x4: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I32X4)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 1,
        constraint_offset: 20,
    },
    // Uload32x2: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I64X2)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 1,
        constraint_offset: 22,
    },
    // Sload32x2: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I64X2)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 1,
        constraint_offset: 22,
    },
    // StackLoad: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 5,
        constraint_offset: 0,
    },
    // StackStore: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 5,
        constraint_offset: 0,
    },
    // StackAddr: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // DynamicStackLoad: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 5,
        constraint_offset: 0,
    },
    // DynamicStackStore: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 5,
        constraint_offset: 0,
    },
    // DynamicStackAddr: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // GlobalValue: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 5,
        constraint_offset: 0,
    },
    // SymbolValue: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 5,
        constraint_offset: 0,
    },
    // TlsValue: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 5,
        constraint_offset: 0,
    },
    // HeapAddr: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(7)']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 1,
        constraint_offset: 23,
    },
    // HeapLoad: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(7)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 5,
        constraint_offset: 23,
    },
    // HeapStore: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['Same', 'Free(5)']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64})
    OpcodeConstraints {
        flags: 0x58,
        typeset_offset: 7,
        constraint_offset: 25,
    },
    // GetPinnedReg: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // SetPinnedReg: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // GetFramePointer: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // GetStackPointer: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // GetReturnAddress: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // TableAddr: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(7)']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 1,
        constraint_offset: 23,
    },
    // Iconst: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 9,
        constraint_offset: 0,
    },
    // F32const: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Concrete(ir::types::F32)']
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 255,
        constraint_offset: 27,
    },
    // F64const: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Concrete(ir::types::F64)']
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 255,
        constraint_offset: 28,
    },
    // Vconst: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 10,
        constraint_offset: 0,
    },
    // Shuffle: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Concrete(ir::types::I8X16)', 'Concrete(ir::types::I8X16)', 'Concrete(ir::types::I8X16)']
    OpcodeConstraints {
        flags: 0x41,
        typeset_offset: 255,
        constraint_offset: 29,
    },
    // Null: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 11,
        constraint_offset: 0,
    },
    // Nop: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // Select: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Free(0)', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x69,
        typeset_offset: 12,
        constraint_offset: 32,
    },
    // SelectSpectreGuard: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Free(0)', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x69,
        typeset_offset: 12,
        constraint_offset: 32,
    },
    // Bitselect: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x69,
        typeset_offset: 12,
        constraint_offset: 34,
    },
    // Vsplit: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['HalfVector', 'HalfVector', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x3a,
        typeset_offset: 10,
        constraint_offset: 38,
    },
    // Vconcat: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['DoubleVector', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128}, ints={8, 16, 32, 64, 128}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x59,
        typeset_offset: 13,
        constraint_offset: 41,
    },
    // Vselect: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'AsBool', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x69,
        typeset_offset: 10,
        constraint_offset: 43,
    },
    // VanyTrue: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I8)', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 10,
        constraint_offset: 47,
    },
    // VallTrue: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I8)', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 10,
        constraint_offset: 47,
    },
    // VhighBits: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(10)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 14,
        constraint_offset: 48,
    },
    // Icmp: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['AsBool', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x59,
        typeset_offset: 14,
        constraint_offset: 44,
    },
    // IcmpImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I8)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 0,
        constraint_offset: 47,
    },
    // Ifcmp: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['Concrete(ir::types::IFLAGS)', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x59,
        typeset_offset: 0,
        constraint_offset: 50,
    },
    // IfcmpImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::IFLAGS)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 0,
        constraint_offset: 50,
    },
    // Iadd: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 14,
        constraint_offset: 0,
    },
    // Isub: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 14,
        constraint_offset: 0,
    },
    // Ineg: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 14,
        constraint_offset: 0,
    },
    // Iabs: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 14,
        constraint_offset: 0,
    },
    // Imul: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 14,
        constraint_offset: 0,
    },
    // Umulhi: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 14,
        constraint_offset: 0,
    },
    // Smulhi: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 14,
        constraint_offset: 0,
    },
    // SqmulRoundSat: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={4, 8}, ints={16, 32})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 15,
        constraint_offset: 0,
    },
    // Udiv: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // Sdiv: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // Urem: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // Srem: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // IaddImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // ImulImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // UdivImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // SdivImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // UremImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // SremImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // IrsubImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // IaddCin: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Same', 'Same', 'Concrete(ir::types::I8)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x69,
        typeset_offset: 0,
        constraint_offset: 51,
    },
    // IaddIfcin: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Same', 'Same', 'Concrete(ir::types::IFLAGS)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x69,
        typeset_offset: 0,
        constraint_offset: 55,
    },
    // IaddCout: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Concrete(ir::types::I8)', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x4a,
        typeset_offset: 0,
        constraint_offset: 53,
    },
    // IaddIfcout: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Concrete(ir::types::IFLAGS)', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x4a,
        typeset_offset: 0,
        constraint_offset: 57,
    },
    // IaddCarry: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Concrete(ir::types::I8)', 'Same', 'Same', 'Concrete(ir::types::I8)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x6a,
        typeset_offset: 0,
        constraint_offset: 60,
    },
    // IaddIfcarry: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Concrete(ir::types::IFLAGS)', 'Same', 'Same', 'Concrete(ir::types::IFLAGS)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x6a,
        typeset_offset: 0,
        constraint_offset: 65,
    },
    // UaddOverflowTrap: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 7,
        constraint_offset: 0,
    },
    // IsubBin: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Same', 'Same', 'Concrete(ir::types::I8)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x69,
        typeset_offset: 0,
        constraint_offset: 51,
    },
    // IsubIfbin: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Same', 'Same', 'Concrete(ir::types::IFLAGS)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x69,
        typeset_offset: 0,
        constraint_offset: 55,
    },
    // IsubBout: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Concrete(ir::types::I8)', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x4a,
        typeset_offset: 0,
        constraint_offset: 53,
    },
    // IsubIfbout: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Concrete(ir::types::IFLAGS)', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x4a,
        typeset_offset: 0,
        constraint_offset: 57,
    },
    // IsubBorrow: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Concrete(ir::types::I8)', 'Same', 'Same', 'Concrete(ir::types::I8)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x6a,
        typeset_offset: 0,
        constraint_offset: 60,
    },
    // IsubIfborrow: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Concrete(ir::types::IFLAGS)', 'Same', 'Same', 'Concrete(ir::types::IFLAGS)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x6a,
        typeset_offset: 0,
        constraint_offset: 65,
    },
    // Band: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // Bor: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // Bxor: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // Bnot: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // BandNot: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // BorNot: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // BxorNot: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // BandImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // BorImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // BxorImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // Rotl: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Free(0)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 14,
        constraint_offset: 70,
    },
    // Rotr: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Free(0)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 14,
        constraint_offset: 70,
    },
    // RotlImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 14,
        constraint_offset: 0,
    },
    // RotrImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 14,
        constraint_offset: 0,
    },
    // Ishl: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Free(0)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 14,
        constraint_offset: 70,
    },
    // Ushr: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Free(0)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 14,
        constraint_offset: 70,
    },
    // Sshr: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Free(0)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 14,
        constraint_offset: 70,
    },
    // IshlImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 14,
        constraint_offset: 0,
    },
    // UshrImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 14,
        constraint_offset: 0,
    },
    // SshrImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 14,
        constraint_offset: 0,
    },
    // Bitrev: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // Clz: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // Cls: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // Ctz: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // Bswap: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 17,
        constraint_offset: 0,
    },
    // Popcnt: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 14,
        constraint_offset: 0,
    },
    // Fcmp: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['AsBool', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x59,
        typeset_offset: 18,
        constraint_offset: 44,
    },
    // Ffcmp: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['Concrete(ir::types::FFLAGS)', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x59,
        typeset_offset: 18,
        constraint_offset: 73,
    },
    // Fadd: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 18,
        constraint_offset: 0,
    },
    // Fsub: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 18,
        constraint_offset: 0,
    },
    // Fmul: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 18,
        constraint_offset: 0,
    },
    // Fdiv: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 18,
        constraint_offset: 0,
    },
    // Sqrt: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 18,
        constraint_offset: 0,
    },
    // Fma: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x69,
        typeset_offset: 18,
        constraint_offset: 34,
    },
    // Fneg: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 18,
        constraint_offset: 0,
    },
    // Fabs: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 18,
        constraint_offset: 0,
    },
    // Fcopysign: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 18,
        constraint_offset: 0,
    },
    // Fmin: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 18,
        constraint_offset: 0,
    },
    // FminPseudo: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 18,
        constraint_offset: 0,
    },
    // Fmax: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 18,
        constraint_offset: 0,
    },
    // FmaxPseudo: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 18,
        constraint_offset: 0,
    },
    // Ceil: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 18,
        constraint_offset: 0,
    },
    // Floor: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 18,
        constraint_offset: 0,
    },
    // Trunc: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 18,
        constraint_offset: 0,
    },
    // Nearest: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 18,
        constraint_offset: 0,
    },
    // IsNull: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I8)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 11,
        constraint_offset: 47,
    },
    // IsInvalid: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I8)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 11,
        constraint_offset: 47,
    },
    // Bitcast: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(5)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 5,
        constraint_offset: 25,
    },
    // ScalarToVector: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'LaneOf']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 10,
        constraint_offset: 4,
    },
    // Bmask: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(3)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 3,
        constraint_offset: 75,
    },
    // Ireduce: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(0)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 0,
        constraint_offset: 32,
    },
    // Snarrow: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['SplitLanes', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8}, ints={16, 32, 64})
    OpcodeConstraints {
        flags: 0x59,
        typeset_offset: 19,
        constraint_offset: 77,
    },
    // Unarrow: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['SplitLanes', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8}, ints={16, 32, 64})
    OpcodeConstraints {
        flags: 0x59,
        typeset_offset: 19,
        constraint_offset: 77,
    },
    // Uunarrow: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['SplitLanes', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8}, ints={16, 32, 64})
    OpcodeConstraints {
        flags: 0x59,
        typeset_offset: 19,
        constraint_offset: 77,
    },
    // SwidenLow: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['MergeLanes', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16}, ints={8, 16, 32})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 20,
        constraint_offset: 80,
    },
    // SwidenHigh: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['MergeLanes', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16}, ints={8, 16, 32})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 20,
        constraint_offset: 80,
    },
    // UwidenLow: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['MergeLanes', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16}, ints={8, 16, 32})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 20,
        constraint_offset: 80,
    },
    // UwidenHigh: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['MergeLanes', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16}, ints={8, 16, 32})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 20,
        constraint_offset: 80,
    },
    // IaddPairwise: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16}, ints={8, 16, 32})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 20,
        constraint_offset: 0,
    },
    // WideningPairwiseDotProductS: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Concrete(ir::types::I32X4)', 'Concrete(ir::types::I16X8)', 'Concrete(ir::types::I16X8)']
    OpcodeConstraints {
        flags: 0x41,
        typeset_offset: 255,
        constraint_offset: 82,
    },
    // Uextend: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(0)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 3,
        constraint_offset: 32,
    },
    // Sextend: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(0)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 3,
        constraint_offset: 32,
    },
    // Fpromote: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(18)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 21,
        constraint_offset: 85,
    },
    // Fdemote: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(18)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 21,
        constraint_offset: 85,
    },
    // Fvdemote: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Concrete(ir::types::F32X4)', 'Concrete(ir::types::F64X2)']
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 255,
        constraint_offset: 87,
    },
    // FvpromoteLow: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Concrete(ir::types::F64X2)', 'Concrete(ir::types::F32X4)']
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 255,
        constraint_offset: 88,
    },
    // FcvtToUint: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(22)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 3,
        constraint_offset: 90,
    },
    // FcvtToSint: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(22)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 3,
        constraint_offset: 90,
    },
    // FcvtToUintSat: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(18)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 3,
        constraint_offset: 85,
    },
    // FcvtToSintSat: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(18)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 3,
        constraint_offset: 85,
    },
    // FcvtFromUint: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(3)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 21,
        constraint_offset: 75,
    },
    // FcvtFromSint: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(3)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 21,
        constraint_offset: 75,
    },
    // FcvtLowFromSint: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(3)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 21,
        constraint_offset: 75,
    },
    // Isplit: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['HalfWidth', 'HalfWidth', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x3a,
        typeset_offset: 23,
        constraint_offset: 92,
    },
    // Iconcat: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['DoubleWidth', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64})
    OpcodeConstraints {
        flags: 0x59,
        typeset_offset: 9,
        constraint_offset: 95,
    },
    // AtomicRmw: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Free(1)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64})
    OpcodeConstraints {
        flags: 0x41,
        typeset_offset: 24,
        constraint_offset: 97,
    },
    // AtomicCas: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Free(1)', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64})
    OpcodeConstraints {
        flags: 0x69,
        typeset_offset: 24,
        constraint_offset: 97,
    },
    // AtomicLoad: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(1)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 24,
        constraint_offset: 12,
    },
    // AtomicStore: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['Same', 'Free(1)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64})
    OpcodeConstraints {
        flags: 0x58,
        typeset_offset: 24,
        constraint_offset: 12,
    },
    // Fence: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // ExtractVector: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['DynamicToVector', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 25,
        constraint_offset: 101,
    },
];

// Table of value type sets.
const TYPE_SETS: [ir::instructions::ValueTypeSet; 26] = [
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
        lanes: BitSet::<u16>(1),
        dynamic_lanes: BitSet::<u16>(0),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
        lanes: BitSet::<u16>(1),
        dynamic_lanes: BitSet::<u16>(0),
        ints: BitSet::<u8>(96),
        floats: BitSet::<u8>(0),
        refs: BitSet::<u8>(96),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
        lanes: BitSet::<u16>(510),
        dynamic_lanes: BitSet::<u16>(510),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(96),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
        lanes: BitSet::<u16>(511),
        dynamic_lanes: BitSet::<u16>(0),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
        lanes: BitSet::<u16>(510),
        dynamic_lanes: BitSet::<u16>(0),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
        lanes: BitSet::<u16>(511),
        dynamic_lanes: BitSet::<u16>(510),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(96),
        refs: BitSet::<u8>(96),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1}, ints={16, 32, 64})
        lanes: BitSet::<u16>(1),
        dynamic_lanes: BitSet::<u16>(0),
        ints: BitSet::<u8>(112),
        floats: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1}, ints={32, 64})
        lanes: BitSet::<u16>(1),
        dynamic_lanes: BitSet::<u16>(0),
        ints: BitSet::<u8>(96),
        floats: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1}, ints={64})
        lanes: BitSet::<u16>(1),
        dynamic_lanes: BitSet::<u16>(0),
        ints: BitSet::<u8>(64),
        floats: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64})
        lanes: BitSet::<u16>(511),
        dynamic_lanes: BitSet::<u16>(510),
        ints: BitSet::<u8>(120),
        floats: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
        lanes: BitSet::<u16>(510),
        dynamic_lanes: BitSet::<u16>(0),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(96),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1}, refs={32, 64})
        lanes: BitSet::<u16>(1),
        dynamic_lanes: BitSet::<u16>(0),
        ints: BitSet::<u8>(0),
        floats: BitSet::<u8>(0),
        refs: BitSet::<u8>(96),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
        lanes: BitSet::<u16>(511),
        dynamic_lanes: BitSet::<u16>(0),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(96),
        refs: BitSet::<u8>(96),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128}, ints={8, 16, 32, 64, 128}, floats={32, 64})
        lanes: BitSet::<u16>(255),
        dynamic_lanes: BitSet::<u16>(0),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(96),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
        lanes: BitSet::<u16>(511),
        dynamic_lanes: BitSet::<u16>(510),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={4, 8}, ints={16, 32})
        lanes: BitSet::<u16>(12),
        dynamic_lanes: BitSet::<u16>(0),
        ints: BitSet::<u8>(48),
        floats: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64})
        lanes: BitSet::<u16>(511),
        dynamic_lanes: BitSet::<u16>(0),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(96),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1}, ints={16, 32, 64, 128})
        lanes: BitSet::<u16>(1),
        dynamic_lanes: BitSet::<u16>(0),
        ints: BitSet::<u8>(240),
        floats: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
        lanes: BitSet::<u16>(511),
        dynamic_lanes: BitSet::<u16>(510),
        ints: BitSet::<u8>(0),
        floats: BitSet::<u8>(96),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={2, 4, 8}, ints={16, 32, 64})
        lanes: BitSet::<u16>(14),
        dynamic_lanes: BitSet::<u16>(14),
        ints: BitSet::<u8>(112),
        floats: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={2, 4, 8, 16}, ints={8, 16, 32})
        lanes: BitSet::<u16>(30),
        dynamic_lanes: BitSet::<u16>(30),
        ints: BitSet::<u8>(56),
        floats: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
        lanes: BitSet::<u16>(511),
        dynamic_lanes: BitSet::<u16>(0),
        ints: BitSet::<u8>(0),
        floats: BitSet::<u8>(96),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1}, floats={32, 64})
        lanes: BitSet::<u16>(1),
        dynamic_lanes: BitSet::<u16>(0),
        ints: BitSet::<u8>(0),
        floats: BitSet::<u8>(96),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={16, 32, 64, 128})
        lanes: BitSet::<u16>(511),
        dynamic_lanes: BitSet::<u16>(0),
        ints: BitSet::<u8>(240),
        floats: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1}, ints={8, 16, 32, 64})
        lanes: BitSet::<u16>(1),
        dynamic_lanes: BitSet::<u16>(0),
        ints: BitSet::<u8>(120),
        floats: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1}, ints={8, 16, 32, 64, 128}, floats={32, 64})
        lanes: BitSet::<u16>(1),
        dynamic_lanes: BitSet::<u16>(510),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(96),
        refs: BitSet::<u8>(0),
    },
];

// Table of operand constraint sequences.
const OPERAND_CONSTRAINTS: [OperandConstraint; 103] = [
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::I32),
    OperandConstraint::Same,
    OperandConstraint::LaneOf,
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::I8X16),
    OperandConstraint::Concrete(ir::types::I8X16),
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::LaneOf,
    OperandConstraint::Same,
    OperandConstraint::Free(1),
    OperandConstraint::Concrete(ir::types::I64),
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::I64),
    OperandConstraint::Free(1),
    OperandConstraint::Concrete(ir::types::I16X8),
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::I32X4),
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::I64X2),
    OperandConstraint::Same,
    OperandConstraint::Free(7),
    OperandConstraint::Same,
    OperandConstraint::Free(5),
    OperandConstraint::Concrete(ir::types::F32),
    OperandConstraint::Concrete(ir::types::F64),
    OperandConstraint::Concrete(ir::types::I8X16),
    OperandConstraint::Concrete(ir::types::I8X16),
    OperandConstraint::Concrete(ir::types::I8X16),
    OperandConstraint::Same,
    OperandConstraint::Free(0),
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::HalfVector,
    OperandConstraint::HalfVector,
    OperandConstraint::Same,
    OperandConstraint::DoubleVector,
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::AsBool,
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::I8),
    OperandConstraint::Same,
    OperandConstraint::Free(10),
    OperandConstraint::Concrete(ir::types::IFLAGS),
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::I8),
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::IFLAGS),
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::I8),
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::I8),
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::IFLAGS),
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::IFLAGS),
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Free(0),
    OperandConstraint::Concrete(ir::types::FFLAGS),
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Free(3),
    OperandConstraint::SplitLanes,
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::MergeLanes,
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::I32X4),
    OperandConstraint::Concrete(ir::types::I16X8),
    OperandConstraint::Concrete(ir::types::I16X8),
    OperandConstraint::Same,
    OperandConstraint::Free(18),
    OperandConstraint::Concrete(ir::types::F32X4),
    OperandConstraint::Concrete(ir::types::F64X2),
    OperandConstraint::Concrete(ir::types::F32X4),
    OperandConstraint::Same,
    OperandConstraint::Free(22),
    OperandConstraint::HalfWidth,
    OperandConstraint::HalfWidth,
    OperandConstraint::Same,
    OperandConstraint::DoubleWidth,
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Free(1),
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::DynamicToVector,
    OperandConstraint::Same,
];
