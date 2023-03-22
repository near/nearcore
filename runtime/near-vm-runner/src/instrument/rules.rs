use parity_wasm::elements::Instruction;
use std::collections::HashMap as Map;
use std::num::NonZeroU32;
use std::str::FromStr;

pub struct UnknownInstruction;

/// An interface that describes instruction costs.
pub trait Rules {
    /// Returns the cost for the passed `instruction`.
    ///
    /// Returning `None` makes the gas instrumention end with an error. This is meant
    /// as a way to have a partial rule set where any instruction that is not specifed
    /// is considered as forbidden.
    fn instruction_cost(&self, instruction: &Instruction) -> Option<u32>;

    /// Returns the costs for growing the memory using the `memory.grow` instruction.
    ///
    /// Please note that these costs are in addition to the costs specified by `instruction_cost`
    /// for the `memory.grow` instruction. Specifying `None` leads to no additional charge.
    /// Those are meant as dynamic costs which take the amount of pages that the memory is
    /// grown by into consideration. This is not possible using `instruction_cost` because
    /// those costs depend on the stack and must be injected as code into the function calling
    /// `memory.grow`. Therefore returning `Some` comes with a performance cost.
    fn memory_grow_cost(&self) -> Option<MemoryGrowCost>;
}

/// Dynamic costs for memory growth.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum MemoryGrowCost {
    /// Charge the specified amount for each page that the memory is grown by.
    Linear(NonZeroU32),
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
#[allow(unused)]
pub enum Metering {
    Regular,
    Forbidden,
    Fixed(u32),
}

#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
pub enum InstructionType {
    Bit,
    Add,
    Mul,
    Div,
    Load,
    Store,
    Const,
    FloatConst,
    Local,
    Global,
    ControlFlow,
    IntegerComparison,
    FloatComparison,
    Float,
    Conversion,
    FloatConversion,
    Reinterpretation,
    Unreachable,
    Nop,
    CurrentMemory,
    GrowMemory,
}

impl FromStr for InstructionType {
    type Err = UnknownInstruction;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "bit" => Ok(InstructionType::Bit),
            "add" => Ok(InstructionType::Add),
            "mul" => Ok(InstructionType::Mul),
            "div" => Ok(InstructionType::Div),
            "load" => Ok(InstructionType::Load),
            "store" => Ok(InstructionType::Store),
            "const" => Ok(InstructionType::Const),
            "local" => Ok(InstructionType::Local),
            "global" => Ok(InstructionType::Global),
            "flow" => Ok(InstructionType::ControlFlow),
            "integer_comp" => Ok(InstructionType::IntegerComparison),
            "float_comp" => Ok(InstructionType::FloatComparison),
            "float" => Ok(InstructionType::Float),
            "conversion" => Ok(InstructionType::Conversion),
            "float_conversion" => Ok(InstructionType::FloatConversion),
            "reinterpret" => Ok(InstructionType::Reinterpretation),
            "unreachable" => Ok(InstructionType::Unreachable),
            "nop" => Ok(InstructionType::Nop),
            "current_mem" => Ok(InstructionType::CurrentMemory),
            "grow_mem" => Ok(InstructionType::GrowMemory),
            _ => Err(UnknownInstruction),
        }
    }
}

impl InstructionType {
    pub fn op(instruction: &Instruction) -> Self {
        use Instruction::*;

        match *instruction {
            Unreachable => InstructionType::Unreachable,
            Nop => InstructionType::Nop,
            Block(_) => InstructionType::ControlFlow,
            Loop(_) => InstructionType::ControlFlow,
            If(_) => InstructionType::ControlFlow,
            Else => InstructionType::ControlFlow,
            End => InstructionType::ControlFlow,
            Br(_) => InstructionType::ControlFlow,
            BrIf(_) => InstructionType::ControlFlow,
            BrTable(_) => InstructionType::ControlFlow,
            Return => InstructionType::ControlFlow,
            Call(_) => InstructionType::ControlFlow,
            CallIndirect(_, _) => InstructionType::ControlFlow,
            Drop => InstructionType::ControlFlow,
            Select => InstructionType::ControlFlow,

            GetLocal(_) => InstructionType::Local,
            SetLocal(_) => InstructionType::Local,
            TeeLocal(_) => InstructionType::Local,
            GetGlobal(_) => InstructionType::Global,
            SetGlobal(_) => InstructionType::Global,

            I32Load(_, _) => InstructionType::Load,
            I64Load(_, _) => InstructionType::Load,
            F32Load(_, _) => InstructionType::Load,
            F64Load(_, _) => InstructionType::Load,
            I32Load8S(_, _) => InstructionType::Load,
            I32Load8U(_, _) => InstructionType::Load,
            I32Load16S(_, _) => InstructionType::Load,
            I32Load16U(_, _) => InstructionType::Load,
            I64Load8S(_, _) => InstructionType::Load,
            I64Load8U(_, _) => InstructionType::Load,
            I64Load16S(_, _) => InstructionType::Load,
            I64Load16U(_, _) => InstructionType::Load,
            I64Load32S(_, _) => InstructionType::Load,
            I64Load32U(_, _) => InstructionType::Load,

            I32Store(_, _) => InstructionType::Store,
            I64Store(_, _) => InstructionType::Store,
            F32Store(_, _) => InstructionType::Store,
            F64Store(_, _) => InstructionType::Store,
            I32Store8(_, _) => InstructionType::Store,
            I32Store16(_, _) => InstructionType::Store,
            I64Store8(_, _) => InstructionType::Store,
            I64Store16(_, _) => InstructionType::Store,
            I64Store32(_, _) => InstructionType::Store,

            CurrentMemory(_) => InstructionType::CurrentMemory,
            GrowMemory(_) => InstructionType::GrowMemory,

            I32Const(_) => InstructionType::Const,
            I64Const(_) => InstructionType::Const,

            F32Const(_) => InstructionType::FloatConst,
            F64Const(_) => InstructionType::FloatConst,

            I32Eqz => InstructionType::IntegerComparison,
            I32Eq => InstructionType::IntegerComparison,
            I32Ne => InstructionType::IntegerComparison,
            I32LtS => InstructionType::IntegerComparison,
            I32LtU => InstructionType::IntegerComparison,
            I32GtS => InstructionType::IntegerComparison,
            I32GtU => InstructionType::IntegerComparison,
            I32LeS => InstructionType::IntegerComparison,
            I32LeU => InstructionType::IntegerComparison,
            I32GeS => InstructionType::IntegerComparison,
            I32GeU => InstructionType::IntegerComparison,

            I64Eqz => InstructionType::IntegerComparison,
            I64Eq => InstructionType::IntegerComparison,
            I64Ne => InstructionType::IntegerComparison,
            I64LtS => InstructionType::IntegerComparison,
            I64LtU => InstructionType::IntegerComparison,
            I64GtS => InstructionType::IntegerComparison,
            I64GtU => InstructionType::IntegerComparison,
            I64LeS => InstructionType::IntegerComparison,
            I64LeU => InstructionType::IntegerComparison,
            I64GeS => InstructionType::IntegerComparison,
            I64GeU => InstructionType::IntegerComparison,

            F32Eq => InstructionType::FloatComparison,
            F32Ne => InstructionType::FloatComparison,
            F32Lt => InstructionType::FloatComparison,
            F32Gt => InstructionType::FloatComparison,
            F32Le => InstructionType::FloatComparison,
            F32Ge => InstructionType::FloatComparison,

            F64Eq => InstructionType::FloatComparison,
            F64Ne => InstructionType::FloatComparison,
            F64Lt => InstructionType::FloatComparison,
            F64Gt => InstructionType::FloatComparison,
            F64Le => InstructionType::FloatComparison,
            F64Ge => InstructionType::FloatComparison,

            I32Clz => InstructionType::Bit,
            I32Ctz => InstructionType::Bit,
            I32Popcnt => InstructionType::Bit,
            I32Add => InstructionType::Add,
            I32Sub => InstructionType::Add,
            I32Mul => InstructionType::Mul,
            I32DivS => InstructionType::Div,
            I32DivU => InstructionType::Div,
            I32RemS => InstructionType::Div,
            I32RemU => InstructionType::Div,
            I32And => InstructionType::Bit,
            I32Or => InstructionType::Bit,
            I32Xor => InstructionType::Bit,
            I32Shl => InstructionType::Bit,
            I32ShrS => InstructionType::Bit,
            I32ShrU => InstructionType::Bit,
            I32Rotl => InstructionType::Bit,
            I32Rotr => InstructionType::Bit,

            I64Clz => InstructionType::Bit,
            I64Ctz => InstructionType::Bit,
            I64Popcnt => InstructionType::Bit,
            I64Add => InstructionType::Add,
            I64Sub => InstructionType::Add,
            I64Mul => InstructionType::Mul,
            I64DivS => InstructionType::Div,
            I64DivU => InstructionType::Div,
            I64RemS => InstructionType::Div,
            I64RemU => InstructionType::Div,
            I64And => InstructionType::Bit,
            I64Or => InstructionType::Bit,
            I64Xor => InstructionType::Bit,
            I64Shl => InstructionType::Bit,
            I64ShrS => InstructionType::Bit,
            I64ShrU => InstructionType::Bit,
            I64Rotl => InstructionType::Bit,
            I64Rotr => InstructionType::Bit,

            F32Abs => InstructionType::Float,
            F32Neg => InstructionType::Float,
            F32Ceil => InstructionType::Float,
            F32Floor => InstructionType::Float,
            F32Trunc => InstructionType::Float,
            F32Nearest => InstructionType::Float,
            F32Sqrt => InstructionType::Float,
            F32Add => InstructionType::Float,
            F32Sub => InstructionType::Float,
            F32Mul => InstructionType::Float,
            F32Div => InstructionType::Float,
            F32Min => InstructionType::Float,
            F32Max => InstructionType::Float,
            F32Copysign => InstructionType::Float,
            F64Abs => InstructionType::Float,
            F64Neg => InstructionType::Float,
            F64Ceil => InstructionType::Float,
            F64Floor => InstructionType::Float,
            F64Trunc => InstructionType::Float,
            F64Nearest => InstructionType::Float,
            F64Sqrt => InstructionType::Float,
            F64Add => InstructionType::Float,
            F64Sub => InstructionType::Float,
            F64Mul => InstructionType::Float,
            F64Div => InstructionType::Float,
            F64Min => InstructionType::Float,
            F64Max => InstructionType::Float,
            F64Copysign => InstructionType::Float,

            I32WrapI64 => InstructionType::Conversion,
            I64ExtendSI32 => InstructionType::Conversion,
            I64ExtendUI32 => InstructionType::Conversion,

            I32TruncSF32 => InstructionType::FloatConversion,
            I32TruncUF32 => InstructionType::FloatConversion,
            I32TruncSF64 => InstructionType::FloatConversion,
            I32TruncUF64 => InstructionType::FloatConversion,
            I64TruncSF32 => InstructionType::FloatConversion,
            I64TruncUF32 => InstructionType::FloatConversion,
            I64TruncSF64 => InstructionType::FloatConversion,
            I64TruncUF64 => InstructionType::FloatConversion,
            F32ConvertSI32 => InstructionType::FloatConversion,
            F32ConvertUI32 => InstructionType::FloatConversion,
            F32ConvertSI64 => InstructionType::FloatConversion,
            F32ConvertUI64 => InstructionType::FloatConversion,
            F32DemoteF64 => InstructionType::FloatConversion,
            F64ConvertSI32 => InstructionType::FloatConversion,
            F64ConvertUI32 => InstructionType::FloatConversion,
            F64ConvertSI64 => InstructionType::FloatConversion,
            F64ConvertUI64 => InstructionType::FloatConversion,
            F64PromoteF32 => InstructionType::FloatConversion,

            I32ReinterpretF32 => InstructionType::Reinterpretation,
            I64ReinterpretF64 => InstructionType::Reinterpretation,
            F32ReinterpretI32 => InstructionType::Reinterpretation,
            F64ReinterpretI64 => InstructionType::Reinterpretation,
        }
    }
}

#[derive(Debug)]
pub struct Set {
    regular: u32,
    entries: Map<InstructionType, Metering>,
    grow: u32,
}

impl Default for Set {
    fn default() -> Self {
        Set { regular: 1, entries: Map::new(), grow: 0 }
    }
}

impl Set {
    pub fn new(regular: u32, entries: Map<InstructionType, Metering>) -> Self {
        Set { regular, entries, grow: 0 }
    }

    pub fn with_grow_cost(mut self, val: u32) -> Self {
        self.grow = val;
        self
    }
}

impl Rules for Set {
    fn instruction_cost(&self, instruction: &Instruction) -> Option<u32> {
        match self.entries.get(&InstructionType::op(instruction)) {
            None | Some(Metering::Regular) => Some(self.regular),
            Some(Metering::Fixed(val)) => Some(*val),
            Some(Metering::Forbidden) => None,
        }
    }

    fn memory_grow_cost(&self) -> Option<MemoryGrowCost> {
        NonZeroU32::new(self.grow).map(MemoryGrowCost::Linear)
    }
}
