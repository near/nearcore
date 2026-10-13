use super::{LengthLimitExceeded, Replay, is_length_limit_exceeded};
use crate::network_protocol::bounded;
use crate::test_alloc::measure_allocated;
use borsh::error::ERROR_ZST_FORBIDDEN;
use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::{KIB, MIB};
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::hash::{BuildHasherDefault, DefaultHasher, Hash, RandomState};
use std::io::{self, IoSliceMut, Read};

const MAX: u32 = 4;

/// Hasher other than the default, to check that decoders keep the caller's.
type CustomHasher = BuildHasherDefault<DefaultHasher>;

/// Upper bound on the error values and other small allocations made while a
/// decode fails.
const SMALL_ALLOCATIONS: usize = 1024;

#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Small {
    id: u32,
    name: String,
}

thread_local! {
    static PROBE_DECODES: Cell<usize> = const { Cell::new(0) };
}

/// Counts how many times it is decoded on the current thread.
#[derive(BorshSerialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Probe(u64);

impl BorshDeserialize for Probe {
    fn deserialize_reader<R: Read>(reader: &mut R) -> io::Result<Self> {
        PROBE_DECODES.set(PROBE_DECODES.get() + 1);
        u64::deserialize_reader(reader).map(Probe)
    }
}

/// Decodes with `Read` methods that Borsh's own decoders never call.
#[derive(Debug, PartialEq)]
struct OtherReads {
    vectored: usize,
    rest: String,
}

impl BorshDeserialize for OtherReads {
    fn deserialize_reader<R: Read>(reader: &mut R) -> io::Result<Self> {
        let (mut first, mut second) = ([0; 1], [0; 1]);
        let vectored = reader
            .read_vectored(&mut [IoSliceMut::new(&mut first), IoSliceMut::new(&mut second)])?;
        let mut rest = String::new();
        reader.read_to_string(&mut rest)?;
        Ok(Self { vectored, rest })
    }
}

/// Records which `Read` methods are called on it.
#[derive(Default)]
struct CallLog(Vec<&'static str>);

impl Read for CallLog {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        self.0.push("read");
        Ok(0)
    }

    fn read_vectored(&mut self, _: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        self.0.push("read_vectored");
        Ok(0)
    }

    fn read_exact(&mut self, _: &mut [u8]) -> io::Result<()> {
        self.0.push("read_exact");
        Ok(())
    }

    fn read_to_end(&mut self, _: &mut Vec<u8>) -> io::Result<usize> {
        self.0.push("read_to_end");
        Ok(0)
    }

    fn read_to_string(&mut self, _: &mut String) -> io::Result<usize> {
        self.0.push("read_to_string");
        Ok(0)
    }
}

#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq)]
struct PlainMessage {
    head: u8,
    items: Vec<u32>,
    tail: u16,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq)]
struct BoundedMessage {
    head: u8,
    #[borsh(deserialize_with = "bounded::vec::<_, _, 3>")]
    items: Vec<u32>,
    tail: u16,
}

#[derive(BorshDeserialize, Debug)]
struct BoundedProbes {
    #[borsh(deserialize_with = "bounded::vec::<_, _, 2>")]
    _probes: Vec<Probe>,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq)]
struct BoundedCollections {
    #[borsh(deserialize_with = "bounded::vec::<_, _, 3>")]
    vec: Vec<u32>,
    #[borsh(deserialize_with = "bounded::hash_set::<_, _, _, 3>")]
    hash_set: HashSet<u32>,
    #[borsh(deserialize_with = "bounded::hash_map::<_, _, _, _, 3>")]
    hash_map: HashMap<u32, String, CustomHasher>,
    #[borsh(deserialize_with = "bounded::btree_set::<_, _, 3>")]
    btree_set: BTreeSet<u32>,
    #[borsh(deserialize_with = "bounded::btree_map::<_, _, _, 3>")]
    btree_map: BTreeMap<u32, String>,
}

/// Reader with configurable behavior, to check that decoders use the caller's
/// reader exactly as stock Borsh does.
#[derive(Clone, Copy)]
struct TestReader<'a> {
    data: &'a [u8],
    /// Number of `read` calls so far.
    reads: usize,
    /// Upper bound on the bytes returned by one `read`.
    max_read: usize,
    /// Return `Interrupted` from this `read` call, counting from 0.
    interrupted_read: Option<usize>,
    /// Fail a `read_exact` that `data` cannot fill without consuming anything.
    atomic_read_exact: bool,
    /// Fail reads once `data` is used up, instead of returning 0.
    fail_at_end: bool,
}

impl<'a> TestReader<'a> {
    fn variants(data: &'a [u8]) -> [Self; 8] {
        let plain = Self {
            data,
            reads: 0,
            max_read: usize::MAX,
            interrupted_read: None,
            atomic_read_exact: false,
            fail_at_end: false,
        };
        [
            plain,
            Self { max_read: 1, ..plain },
            Self { atomic_read_exact: true, ..plain },
            Self { fail_at_end: true, ..plain },
            // Interrupt the length prefix, then the first element, with
            // whole and with one-byte reads.
            Self { interrupted_read: Some(0), ..plain },
            Self { interrupted_read: Some(1), ..plain },
            Self { max_read: 1, interrupted_read: Some(2), ..plain },
            Self { max_read: 1, interrupted_read: Some(6), ..plain },
        ]
    }

    fn remaining(&self) -> usize {
        self.data.len()
    }
}

impl Read for TestReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let call = self.reads;
        self.reads += 1;
        if self.interrupted_read == Some(call) {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if self.fail_at_end && self.data.is_empty() && !buf.is_empty() {
            return Err(io::Error::other("reader failed"));
        }
        let len = buf.len().min(self.max_read);
        self.data.read(&mut buf[..len])
    }

    fn read_exact(&mut self, mut buf: &mut [u8]) -> io::Result<()> {
        if self.atomic_read_exact && buf.len() > self.data.len() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        // The default implementation, which an override cannot call.
        while !buf.is_empty() {
            match self.read(buf) {
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(read) => buf = &mut buf[read..],
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }
}

fn err_repr(err: &io::Error) -> (io::ErrorKind, String) {
    (err.kind(), err.to_string())
}

fn declared_len(bytes: &[u8]) -> Option<u32> {
    bytes.first_chunk().map(|prefix| u32::from_le_bytes(*prefix))
}

fn assert_same_result<T: PartialEq + Debug>(bounded: &io::Result<T>, stock: &io::Result<T>) {
    match (bounded, stock) {
        (Ok(bounded), Ok(stock)) => assert_eq!(bounded, stock),
        (Err(bounded), Err(stock)) => {
            assert!(!is_length_limit_exceeded(bounded), "unexpected limit error: {bounded}");
            assert_eq!(err_repr(bounded), err_repr(stock));
        }
        _ => panic!("bounded result {bounded:?} differs from stock result {stock:?}"),
    }
}

/// Decodes with `bounded` and with stock Borsh, each from its own copy of
/// `reader`, and asserts that both return the same value or error and leave
/// the same number of bytes unread.
fn assert_reader_parity<R: Read + Copy, T>(
    reader: R,
    remaining: impl Fn(&R) -> usize,
    bounded: impl FnOnce(&mut R) -> io::Result<T>,
) -> io::Result<T>
where
    T: BorshDeserialize + PartialEq + Debug,
{
    let mut bounded_reader = reader;
    let bounded_result = bounded(&mut bounded_reader);
    let mut stock_reader = reader;
    let stock_result = T::deserialize_reader(&mut stock_reader);
    assert_same_result(&bounded_result, &stock_result);
    assert_eq!(remaining(&bounded_reader), remaining(&stock_reader), "consumed byte counts differ");
    bounded_result
}

fn assert_parity<'a, T>(
    bytes: &'a [u8],
    bounded: impl FnOnce(&mut &'a [u8]) -> io::Result<T>,
) -> io::Result<T>
where
    T: BorshDeserialize + PartialEq + Debug,
{
    assert_reader_parity(bytes, |reader| reader.len(), bounded)
}

fn assert_test_reader_parity<'a, T>(
    reader: TestReader<'a>,
    bounded: impl FnOnce(&mut TestReader<'a>) -> io::Result<T>,
) where
    T: BorshDeserialize + PartialEq + Debug,
{
    let _ = assert_reader_parity(reader, TestReader::remaining, bounded);
}

fn assert_limit_error<T: Debug>(result: io::Result<T>, len: u32, max: u32) {
    let err = result.expect_err("expected a limit error");
    assert!(is_length_limit_exceeded(&err), "expected a limit error, got {err}");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    let inner = err.get_ref().unwrap().downcast_ref::<LengthLimitExceeded>().unwrap();
    assert_eq!((inner.len, inner.max), (len, max));
}

/// Asserts that `bounded` rejects `bytes` with a limit error after reading
/// only the length prefix, without decoding any `Probe`.
fn assert_rejected_before_elements<'a, T: Debug>(
    bytes: &'a [u8],
    max: u32,
    bounded: impl FnOnce(&mut &'a [u8]) -> io::Result<T>,
) {
    let len = declared_len(bytes).unwrap();
    let mut reader = bytes;
    PROBE_DECODES.set(0);
    let result = bounded(&mut reader);
    assert_eq!(PROBE_DECODES.get(), 0, "elements were decoded");
    assert_limit_error(result, len, max);
    assert_eq!(bytes.len() - reader.len(), 4, "read past the length prefix");
}

/// Asserts that `bounded` matches stock Borsh on `bytes`, unless the length
/// prefix exceeds `max`, in which case it must be rejected up front.
fn assert_parity_or_limit<'a, T>(
    bytes: &'a [u8],
    max: u32,
    bounded: impl FnOnce(&mut &'a [u8]) -> io::Result<T>,
) where
    T: BorshDeserialize + PartialEq + Debug,
{
    match declared_len(bytes) {
        Some(len) if len > max => assert_rejected_before_elements(bytes, max, bounded),
        _ => {
            let _ = assert_parity(bytes, bounded);
        }
    }
}

/// Runs `assert_parity_or_limit` for every decoder, with the default and a
/// custom hasher.
fn assert_all_decoders<const MAX: u32>(bytes: &[u8]) {
    assert_parity_or_limit(bytes, MAX, bounded::vec::<_, u32, MAX>);
    assert_parity_or_limit(bytes, MAX, bounded::hash_set::<_, u32, RandomState, MAX>);
    assert_parity_or_limit(bytes, MAX, bounded::hash_set::<_, u32, CustomHasher, MAX>);
    assert_parity_or_limit(bytes, MAX, bounded::btree_set::<_, u32, MAX>);
    assert_parity_or_limit(bytes, MAX, bounded::hash_map::<_, u32, u8, RandomState, MAX>);
    assert_parity_or_limit(bytes, MAX, bounded::hash_map::<_, u32, u8, CustomHasher, MAX>);
    assert_parity_or_limit(bytes, MAX, bounded::btree_map::<_, u32, u8, MAX>);
}

fn lengths_within(max: u32) -> impl Iterator<Item = u32> {
    [0, 1, max].into_iter().filter(move |&len| len <= max)
}

fn check_vec_parity<T, const MAX: u32>(make: impl Fn(u32) -> T)
where
    T: BorshSerialize + BorshDeserialize + PartialEq + Debug,
{
    for len in lengths_within(MAX) {
        let value: Vec<T> = (0..len).map(&make).collect();
        let bytes = borsh::to_vec(&value).unwrap();
        assert_eq!(assert_parity(&bytes, bounded::vec::<_, T, MAX>).unwrap(), value);
    }
}

fn check_set_parity<T, const MAX: u32>(make: impl Fn(u32) -> T)
where
    T: BorshSerialize + BorshDeserialize + Eq + Hash + Ord + Debug,
{
    for len in lengths_within(MAX) {
        let value: BTreeSet<T> = (0..len).map(&make).collect();
        let bytes = borsh::to_vec(&value).unwrap();
        let hash_set = assert_parity(&bytes, bounded::hash_set::<_, T, RandomState, MAX>).unwrap();
        let custom = assert_parity(&bytes, bounded::hash_set::<_, T, CustomHasher, MAX>).unwrap();
        let btree_set = assert_parity(&bytes, bounded::btree_set::<_, T, MAX>).unwrap();
        assert_eq!(btree_set, value);
        assert_eq!(hash_set.into_iter().collect::<BTreeSet<_>>(), value);
        assert_eq!(custom.into_iter().collect::<BTreeSet<_>>(), value);
    }
}

fn check_map_parity<K, V, const MAX: u32>(make: impl Fn(u32) -> (K, V))
where
    K: BorshSerialize + BorshDeserialize + Eq + Hash + Ord + Debug,
    V: BorshSerialize + BorshDeserialize + PartialEq + Debug,
{
    for len in lengths_within(MAX) {
        let value: BTreeMap<K, V> = (0..len).map(&make).collect();
        let bytes = borsh::to_vec(&value).unwrap();
        let hash_map =
            assert_parity(&bytes, bounded::hash_map::<_, K, V, RandomState, MAX>).unwrap();
        let custom =
            assert_parity(&bytes, bounded::hash_map::<_, K, V, CustomHasher, MAX>).unwrap();
        let btree_map = assert_parity(&bytes, bounded::btree_map::<_, K, V, MAX>).unwrap();
        assert_eq!(btree_map, value);
        assert_eq!(hash_map.into_iter().collect::<BTreeMap<_, _>>(), value);
        assert_eq!(custom.into_iter().collect::<BTreeMap<_, _>>(), value);
    }
}

#[test]
fn vec_matches_borsh_within_limit() {
    check_vec_parity::<u8, MAX>(|i| i as u8);
    check_vec_parity::<u64, MAX>(u64::from);
    check_vec_parity::<String, MAX>(|i| format!("element {i}"));
    check_vec_parity::<Vec<u32>, MAX>(|i| (0..i).collect());
    check_vec_parity::<Small, MAX>(|i| Small { id: i, name: i.to_string() });
    check_vec_parity::<u64, 0>(u64::from);
}

#[test]
fn sets_match_borsh_within_limit() {
    check_set_parity::<u8, MAX>(|i| i as u8);
    check_set_parity::<u64, MAX>(u64::from);
    check_set_parity::<String, MAX>(|i| format!("element {i}"));
    check_set_parity::<Vec<u32>, MAX>(|i| (0..i).collect());
    check_set_parity::<Small, MAX>(|i| Small { id: i, name: i.to_string() });
    check_set_parity::<u64, 0>(u64::from);
}

#[test]
fn maps_match_borsh_within_limit() {
    check_map_parity::<u8, u64, MAX>(|i| (i as u8, u64::from(i)));
    check_map_parity::<u64, String, MAX>(|i| (u64::from(i), format!("value {i}")));
    check_map_parity::<String, Vec<u32>, MAX>(|i| (i.to_string(), (0..i).collect()));
    check_map_parity::<Small, Small, MAX>(|i| {
        (Small { id: i, name: String::new() }, Small { id: i + 1, name: i.to_string() })
    });
    check_map_parity::<u64, u64, 0>(|i| (u64::from(i), u64::from(i)));
}

#[test]
fn sets_match_borsh_on_unsorted_and_duplicate_input() {
    for elements in [vec![3u64, 1, 2], vec![1, 1, 2], vec![2, 2, 2]] {
        // Sets share the wire layout of vectors.
        let bytes = borsh::to_vec(&elements).unwrap();
        assert_parity(&bytes, bounded::hash_set::<_, u64, RandomState, 3>).unwrap();
        assert_parity(&bytes, bounded::hash_set::<_, u64, CustomHasher, 3>).unwrap();
        assert_parity(&bytes, bounded::btree_set::<_, u64, 3>).unwrap();
        // The limit counts encoded elements, duplicates included.
        assert_limit_error(
            bounded::hash_set::<_, u64, RandomState, 2>(&mut bytes.as_slice()),
            3,
            2,
        );
        assert_limit_error(bounded::btree_set::<_, u64, 2>(&mut bytes.as_slice()), 3, 2);
    }
}

#[test]
fn duplicate_map_keys_count_towards_limit_and_last_value_wins() {
    // Serializing a map cannot produce duplicate keys, so encode the entries
    // as a vector of pairs, which has the same wire layout.
    let entries: Vec<(u64, String)> =
        vec![(2, "two".to_string()), (1, "first".to_string()), (1, "last".to_string())];
    let bytes = borsh::to_vec(&entries).unwrap();

    assert_limit_error(
        bounded::hash_map::<_, u64, String, RandomState, 2>(&mut bytes.as_slice()),
        3,
        2,
    );
    assert_limit_error(bounded::btree_map::<_, u64, String, 2>(&mut bytes.as_slice()), 3, 2);

    let expected = BTreeMap::from([(1, "last".to_string()), (2, "two".to_string())]);
    let hash_map =
        assert_parity(&bytes, bounded::hash_map::<_, u64, String, RandomState, 3>).unwrap();
    let custom =
        assert_parity(&bytes, bounded::hash_map::<_, u64, String, CustomHasher, 3>).unwrap();
    let btree_map = assert_parity(&bytes, bounded::btree_map::<_, u64, String, 3>).unwrap();
    assert_eq!(btree_map, expected);
    assert_eq!(hash_map.into_iter().collect::<BTreeMap<_, _>>(), expected);
    assert_eq!(custom.into_iter().collect::<BTreeMap<_, _>>(), expected);
}

#[test]
fn over_limit_is_rejected_before_any_element_is_decoded() {
    let probes: Vec<Probe> = (0..=u64::from(MAX)).map(Probe).collect();
    let pairs: Vec<(Probe, Probe)> = probes.iter().map(|p| (p.clone(), p.clone())).collect();
    let probe_bytes = borsh::to_vec(&probes).unwrap();
    let pair_bytes = borsh::to_vec(&pairs).unwrap();

    // The probe must count decodes, or the zero checks below prove nothing.
    PROBE_DECODES.set(0);
    bounded::vec::<_, Probe, { MAX + 1 }>(&mut probe_bytes.as_slice()).unwrap();
    assert_eq!(PROBE_DECODES.get(), probes.len());

    assert_rejected_before_elements(&probe_bytes, MAX, bounded::vec::<_, Probe, MAX>);
    assert_rejected_before_elements(
        &probe_bytes,
        MAX,
        bounded::hash_set::<_, Probe, RandomState, MAX>,
    );
    assert_rejected_before_elements(&probe_bytes, MAX, bounded::btree_set::<_, Probe, MAX>);
    assert_rejected_before_elements(
        &pair_bytes,
        MAX,
        bounded::hash_map::<_, Probe, Probe, RandomState, MAX>,
    );
    assert_rejected_before_elements(&pair_bytes, MAX, bounded::btree_map::<_, Probe, Probe, MAX>);

    // A limit of zero rejects any element.
    assert_rejected_before_elements(&probe_bytes[..12], 0, bounded::vec::<_, Probe, 0>);

    // A lying length with no elements behind it is rejected the same way.
    assert_rejected_before_elements(&u32::MAX.to_le_bytes(), MAX, bounded::vec::<_, Probe, MAX>);
}

#[test]
fn limit_error_survives_derive() {
    let probes: Vec<Probe> = (0..3).map(Probe).collect();
    let bytes = borsh::to_vec(&probes).unwrap();
    PROBE_DECODES.set(0);
    assert_limit_error(borsh::from_slice::<BoundedProbes>(&bytes), 3, 2);
    assert_eq!(PROBE_DECODES.get(), 0);
}

#[test]
fn deserialize_with_keeps_wire_format_and_enforces_limit() {
    let plain = PlainMessage { head: 7, items: vec![1, 2, 3], tail: 9 };
    let bounded = BoundedMessage { head: 7, items: vec![1, 2, 3], tail: 9 };
    let bytes = borsh::to_vec(&plain).unwrap();
    assert_eq!(borsh::to_vec(&bounded).unwrap(), bytes);
    assert_eq!(borsh::from_slice::<BoundedMessage>(&bytes).unwrap(), bounded);

    let too_long = borsh::to_vec(&PlainMessage { items: vec![1, 2, 3, 4], ..plain }).unwrap();
    assert_limit_error(borsh::from_slice::<BoundedMessage>(&too_long), 4, 3);
}

#[test]
fn deserialize_with_works_for_every_collection() {
    let value = BoundedCollections {
        vec: vec![3, 1, 2],
        hash_set: HashSet::from([3, 1, 2]),
        hash_map: HashMap::from_iter([(1, "a".to_string()), (2, "b".to_string())]),
        btree_set: BTreeSet::from([5]),
        btree_map: BTreeMap::new(),
    };
    let bytes = borsh::to_vec(&value).unwrap();
    assert_eq!(borsh::from_slice::<BoundedCollections>(&bytes).unwrap(), value);

    let too_long = BoundedCollections {
        hash_map: HashMap::from_iter((0..4).map(|i| (i, i.to_string()))),
        ..value
    };
    let bytes = borsh::to_vec(&too_long).unwrap();
    assert_limit_error(borsh::from_slice::<BoundedCollections>(&bytes), 4, 3);
}

#[test]
fn zero_sized_elements_and_keys_fail_like_borsh() {
    // Borsh refuses to encode these, so the inputs are built by hand. The
    // error must come first, even without a complete length prefix.
    let inputs: [&[u8]; 5] =
        [&[], &[1, 0], &0u32.to_le_bytes(), &MAX.to_le_bytes(), &u32::MAX.to_le_bytes()];
    for bytes in inputs {
        let errors = [
            assert_parity(bytes, bounded::vec::<_, (), MAX>).unwrap_err(),
            assert_parity(bytes, bounded::hash_set::<_, (), RandomState, MAX>).unwrap_err(),
            assert_parity(bytes, bounded::btree_set::<_, (), MAX>).unwrap_err(),
            assert_parity(bytes, bounded::hash_map::<_, (), u64, RandomState, MAX>).unwrap_err(),
            assert_parity(bytes, bounded::btree_map::<_, (), u64, MAX>).unwrap_err(),
        ];
        for err in errors {
            assert_eq!(err.to_string(), ERROR_ZST_FORBIDDEN);
        }
    }
}

#[test]
fn zero_sized_map_values_match_borsh() {
    for entries in [vec![], vec![(2u64, ())], vec![(2, ()), (1, ()), (2, ())]] {
        let bytes = borsh::to_vec(&entries).unwrap();
        assert_parity(&bytes, bounded::hash_map::<_, u64, (), RandomState, MAX>).unwrap();
        assert_parity(&bytes, bounded::btree_map::<_, u64, (), MAX>).unwrap();
    }
}

#[test]
fn malformed_input_fails_like_borsh() {
    let elements = borsh::to_vec(&vec![1u32, 2, 3]).unwrap();
    let pairs = borsh::to_vec(&vec![(1u32, 1u8), (2, 2)]).unwrap();

    // Truncated length prefix.
    assert_all_decoders::<{ u32::MAX }>(&[]);
    assert_all_decoders::<{ u32::MAX }>(&[3, 0, 0]);
    // Truncated inside an element and at an element boundary.
    assert_all_decoders::<{ u32::MAX }>(&elements[..elements.len() - 2]);
    assert_all_decoders::<{ u32::MAX }>(&elements[..elements.len() - 4]);
    assert_all_decoders::<{ u32::MAX }>(&pairs[..pairs.len() - 1]);
    // Valid input followed by trailing bytes, which are left unread.
    assert_all_decoders::<{ u32::MAX }>(&[elements.as_slice(), &[9, 9]].concat());
    assert_all_decoders::<{ u32::MAX }>(&[pairs.as_slice(), &[9]].concat());

    // Bytes take a separate path in Borsh.
    let bytes = borsh::to_vec(&vec![7u8; 10]).unwrap();
    assert_parity(&bytes[..bytes.len() - 1], bounded::vec::<_, u8, 16>).unwrap_err();

    // Element decoding errors.
    let mut invalid_utf8 = borsh::to_vec(&vec!["ab".to_string()]).unwrap();
    *invalid_utf8.last_mut().unwrap() = 0xff;
    assert_parity(&invalid_utf8, bounded::vec::<_, String, MAX>).unwrap_err();
    let invalid_bool = [1, 0, 0, 0, 2];
    assert_parity(&invalid_bool, bounded::vec::<_, bool, MAX>).unwrap_err();
}

#[test]
fn trailing_bytes_fail_like_borsh() {
    let mut bytes = borsh::to_vec(&PlainMessage { head: 1, items: vec![2], tail: 3 }).unwrap();
    bytes.push(0);
    let bounded = borsh::from_slice::<BoundedMessage>(&bytes).unwrap_err();
    let stock = borsh::from_slice::<PlainMessage>(&bytes).unwrap_err();
    assert!(!is_length_limit_exceeded(&bounded));
    assert_eq!(err_repr(&bounded), err_repr(&stock));
}

#[test]
fn unusual_readers_match_borsh() {
    let elements = borsh::to_vec(&vec![1u64, 2, 3]).unwrap();
    let pairs = borsh::to_vec(&vec![(1u64, 1u64), (2, 2)]).unwrap();
    let bytes = borsh::to_vec(&vec![7u8; 10]).unwrap();
    // Every truncation, so that the input runs out at every position.
    for len in 0..=elements.len() {
        for reader in TestReader::variants(&elements[..len]) {
            assert_test_reader_parity(reader, bounded::vec::<_, u64, MAX>);
            assert_test_reader_parity(reader, bounded::hash_set::<_, u64, RandomState, MAX>);
            assert_test_reader_parity(reader, bounded::btree_set::<_, u64, MAX>);
        }
    }
    for len in 0..=pairs.len() {
        for reader in TestReader::variants(&pairs[..len]) {
            assert_test_reader_parity(reader, bounded::hash_map::<_, u64, u64, RandomState, MAX>);
            assert_test_reader_parity(reader, bounded::btree_map::<_, u64, u64, MAX>);
        }
    }
    // Borsh reads bytes with `read` instead of `read_exact`, so it returns
    // `Interrupted` instead of retrying.
    for len in 0..=bytes.len() {
        for reader in TestReader::variants(&bytes[..len]) {
            assert_test_reader_parity(reader, bounded::vec::<_, u8, 16>);
        }
    }
}

#[test]
fn replay_serves_prefix_then_reader() {
    let mut rest: &[u8] = &[5, 6, 7];
    let mut replay = Replay { prefix: &[1, 2, 3, 4], reader: &mut rest };
    let mut buf = [0; 6];
    replay.read_exact(&mut buf).unwrap();
    assert_eq!(buf, [1, 2, 3, 4, 5, 6]);
    assert_eq!(rest, [7]);

    let mut rest: &[u8] = &[5, 6, 7];
    let mut replay = Replay { prefix: &[1, 2, 3, 4], reader: &mut rest };
    let mut buf = [0; 8];
    assert_eq!(replay.read(&mut buf).unwrap(), 4);
    assert_eq!(replay.read(&mut buf[4..]).unwrap(), 3);
    assert_eq!(buf, [1, 2, 3, 4, 5, 6, 7, 0]);

    let mut rest: &[u8] = &[5, 6, 7];
    let mut replay = Replay { prefix: &[1, 2, 3, 4], reader: &mut rest };
    let mut buf = vec![];
    assert_eq!(replay.read_to_end(&mut buf).unwrap(), 7);
    assert_eq!(buf, [1, 2, 3, 4, 5, 6, 7]);

    let mut rest: &[u8] = b"cd";
    let mut replay = Replay { prefix: b"ab", reader: &mut rest };
    let mut buf = String::new();
    assert_eq!(replay.read_to_string(&mut buf).unwrap(), 4);
    assert_eq!(buf, "abcd");
}

#[test]
fn replay_passes_every_read_method_through_after_the_prefix() {
    let mut log = CallLog::default();
    let mut replay = Replay { prefix: &[], reader: &mut log };
    assert_eq!(replay.read(&mut [0; 1]).unwrap(), 0);
    assert_eq!(replay.read_vectored(&mut [IoSliceMut::new(&mut [0; 1])]).unwrap(), 0);
    replay.read_exact(&mut [0; 1]).unwrap();
    assert_eq!(replay.read_to_end(&mut vec![]).unwrap(), 0);
    assert_eq!(replay.read_to_string(&mut String::new()).unwrap(), 0);
    assert_eq!(log.0, ["read", "read_vectored", "read_exact", "read_to_end", "read_to_string"]);
}

#[test]
fn element_decoders_use_the_callers_reader() {
    // A byte slice fills every buffer in `read_vectored`, unlike the default
    // implementation.
    let input = [1, 0, 0, 0, b'a', b'b', b'c'];
    let decoded = assert_parity(&input, bounded::vec::<_, OtherReads, 1>).unwrap();
    assert_eq!(decoded, [OtherReads { vectored: 2, rest: "c".to_string() }]);
    // It also leaves invalid UTF-8 unread in `read_to_string`.
    let input = [1, 0, 0, 0, b'a', b'b', 0xff];
    assert_parity(&input, bounded::vec::<_, OtherReads, 1>).unwrap_err();
}

/// The limit counts elements only. Borsh itself keeps preallocation small for
/// lengths within the limit, except for bytes.
#[test]
fn lying_length_allocations() {
    const LIMIT: u32 = 1_000_000;

    // Borsh preallocates at most 4 KiB of elements before reading any.
    let bytes = LIMIT.to_le_bytes();
    let (result, allocated) =
        measure_allocated(|| bounded::vec::<_, u64, LIMIT>(&mut bytes.as_slice()));
    assert!(!is_length_limit_exceeded(&result.unwrap_err()));
    assert!(allocated <= 4 * KIB as usize + SMALL_ALLOCATIONS, "allocated {allocated} bytes");

    // For bytes, Borsh's initial buffer is up to 1 MiB, filled with zeros.
    let (result, allocated) =
        measure_allocated(|| bounded::vec::<_, u8, LIMIT>(&mut bytes.as_slice()));
    assert!(!is_length_limit_exceeded(&result.unwrap_err()));
    let buffer = (LIMIT as usize).min(MIB as usize);
    assert!(allocated >= buffer, "allocated {allocated} bytes");
    assert!(allocated <= buffer + SMALL_ALLOCATIONS, "allocated {allocated} bytes");

    // Over the limit, only the error is allocated.
    let bytes = (LIMIT + 1).to_le_bytes();
    let (result, allocated) =
        measure_allocated(|| bounded::vec::<_, u8, LIMIT>(&mut bytes.as_slice()));
    assert_limit_error(result, LIMIT + 1, LIMIT);
    assert!(allocated <= SMALL_ALLOCATIONS, "allocated {allocated} bytes");
}

#[test]
fn vec_accepts_exactly_up_to_limit_fuzzer() {
    bolero::check!().with_generator(bolero::gen_with::<Vec<u32>>().len(0usize..16)).for_each(
        |elements: &Vec<u32>| {
            let bytes = borsh::to_vec(elements).unwrap();
            let mut reader = bytes.as_slice();
            let result = bounded::vec::<_, u32, 8>(&mut reader);
            if elements.len() <= 8 {
                assert_eq!(&result.unwrap(), elements);
                assert!(reader.is_empty());
            } else {
                assert_limit_error(result, elements.len() as u32, 8);
            }
        },
    );
}

#[test]
fn arbitrary_input_matches_borsh_or_hits_limit_fuzzer() {
    bolero::check!().for_each(|bytes: &[u8]| {
        assert_all_decoders::<8>(bytes);
        // A random length prefix is almost always over the limit, so also
        // decode the rest of the input behind a small one.
        if let Some((&len, payload)) = bytes.split_first() {
            let input = [&u32::from(len % 12).to_le_bytes()[..], payload].concat();
            assert_all_decoders::<8>(&input);
        }
    });
}
