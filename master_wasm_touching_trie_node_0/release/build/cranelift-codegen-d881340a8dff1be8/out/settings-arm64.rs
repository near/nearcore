#[derive(Clone, Hash)]
/// Flags group `arm64`.
pub struct Flags {
    bytes: [u8; 1],
}
impl Flags {
    /// Create flags arm64 settings group.
    #[allow(unused_variables)]
    pub fn new(shared: &settings::Flags, builder: Builder) -> Self {
        let bvec = builder.state_for("arm64");
        let mut arm64 = Self { bytes: [0; 1] };
        debug_assert_eq!(bvec.len(), 1);
        arm64.bytes[0..1].copy_from_slice(&bvec);
        arm64
    }
}
impl Flags {
    /// Iterates the setting values.
    pub fn iter(&self) -> impl Iterator<Item = Value> {
        let mut bytes = [0; 1];
        bytes.copy_from_slice(&self.bytes[0..1]);
        DESCRIPTORS.iter().filter_map(move |d| {
            let values = match &d.detail {
                detail::Detail::Preset => return None,
                detail::Detail::Enum { last, enumerators } => Some(TEMPLATE.enums(*last, *enumerators)),
                _ => None
            };
            Some(Value{ name: d.name, detail: d.detail, values, value: bytes[d.offset as usize] })
        })
    }
}
/// User-defined settings.
#[allow(dead_code)]
impl Flags {
    /// Get a view of the boolean predicates.
    pub fn predicate_view(&self) -> crate::settings::PredicateView {
        crate::settings::PredicateView::new(&self.bytes[0..])
    }
    /// Dynamic numbered predicate getter.
    fn numbered_predicate(&self, p: usize) -> bool {
        self.bytes[0 + p / 8] & (1 << (p % 8)) != 0
    }
    /// Has Large System Extensions (FEAT_LSE) support.
    pub fn has_lse(&self) -> bool {
        self.numbered_predicate(0)
    }
    /// Has Pointer authentication (FEAT_PAuth) support; enables the use of non-HINT instructions, but does not have an effect on code generation by itself.
    pub fn has_pauth(&self) -> bool {
        self.numbered_predicate(1)
    }
    /// If function return address signing is enabled, then apply it to all functions; does not have an effect on code generation by itself.
    pub fn sign_return_address_all(&self) -> bool {
        self.numbered_predicate(2)
    }
    /// Use pointer authentication instructions to sign function return addresses; HINT-space instructions using the A key are generated and simple functions that do not use the stack are not affected unless overridden by other settings.
    pub fn sign_return_address(&self) -> bool {
        self.numbered_predicate(3)
    }
    /// Use the B key with pointer authentication instructions instead of the default A key; does not have an effect on code generation by itself. Some platform ABIs may require this, for example.
    pub fn sign_return_address_with_bkey(&self) -> bool {
        self.numbered_predicate(4)
    }
    /// Use Branch Target Identification (FEAT_BTI) instructions.
    pub fn use_bti(&self) -> bool {
        self.numbered_predicate(5)
    }
}
static DESCRIPTORS: [detail::Descriptor; 6] = [
    detail::Descriptor {
        name: "has_lse",
        description: "Has Large System Extensions (FEAT_LSE) support.",
        offset: 0,
        detail: detail::Detail::Bool { bit: 0 },
    },
    detail::Descriptor {
        name: "has_pauth",
        description: "Has Pointer authentication (FEAT_PAuth) support; enables the use of non-HINT instructions, but does not have an effect on code generation by itself.",
        offset: 0,
        detail: detail::Detail::Bool { bit: 1 },
    },
    detail::Descriptor {
        name: "sign_return_address_all",
        description: "If function return address signing is enabled, then apply it to all functions; does not have an effect on code generation by itself.",
        offset: 0,
        detail: detail::Detail::Bool { bit: 2 },
    },
    detail::Descriptor {
        name: "sign_return_address",
        description: "Use pointer authentication instructions to sign function return addresses; HINT-space instructions using the A key are generated and simple functions that do not use the stack are not affected unless overridden by other settings.",
        offset: 0,
        detail: detail::Detail::Bool { bit: 3 },
    },
    detail::Descriptor {
        name: "sign_return_address_with_bkey",
        description: "Use the B key with pointer authentication instructions instead of the default A key; does not have an effect on code generation by itself. Some platform ABIs may require this, for example.",
        offset: 0,
        detail: detail::Detail::Bool { bit: 4 },
    },
    detail::Descriptor {
        name: "use_bti",
        description: "Use Branch Target Identification (FEAT_BTI) instructions.",
        offset: 0,
        detail: detail::Detail::Bool { bit: 5 },
    },
];
static ENUMERATORS: [&str; 0] = [
];
static HASH_TABLE: [u16; 8] = [
    5,
    0xffff,
    0xffff,
    1,
    3,
    4,
    2,
    0,
];
static PRESETS: [(u8, u8); 0] = [
];
static TEMPLATE: detail::Template = detail::Template {
    name: "arm64",
    descriptors: &DESCRIPTORS,
    enumerators: &ENUMERATORS,
    hash_table: &HASH_TABLE,
    defaults: &[0x00],
    presets: &PRESETS,
};
/// Create a `settings::Builder` for the arm64 settings group.
pub fn builder() -> Builder {
    Builder::new(&TEMPLATE)
}
impl fmt::Display for Flags {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "[arm64]")?;
        for d in &DESCRIPTORS {
            if !d.detail.is_preset() {
                write!(f, "{} = ", d.name)?;
                TEMPLATE.format_toml_value(d.detail, self.bytes[d.offset as usize], f)?;
                writeln!(f)?;
            }
        }
        Ok(())
    }
}
