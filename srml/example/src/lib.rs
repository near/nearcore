// Copyright 2017-2018 Parity Technologies (UK) Ltd.
// This file is part of Substrate.

// Substrate is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Substrate is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Substrate.  If not, see <http://www.gnu.org/licenses/>.

//! The Example: A simple example of a runtime module demonstrating
//! concepts, APIs and structures common to most runtime modules.

// Ensure we're `no_std` when compiling for Wasm.
#![cfg_attr(not(feature = "std"), no_std)]

// Assert macros used in tests.
extern crate sr_std;

// Needed for tests (`with_externalities`).
#[cfg(test)]
extern crate sr_io as runtime_io;

// Needed for the set of mock primitives used in our tests.
#[cfg(test)]
extern crate substrate_primitives;

// Needed for deriving `Serialize` and `Deserialize` for various types.
// We only implement the serde traits for std builds - they're unneeded
// in the wasm runtime.
#[cfg(feature = "std")]
#[macro_use]
extern crate serde_derive;

// Needed for deriving `Encode` and `Decode` for `RawEvent`.
#[macro_use]
extern crate parity_codec_derive;
extern crate parity_codec as codec;

// Needed for type-safe access to storage DB.
#[macro_use]
extern crate srml_support as runtime_support;

// Needed for various traits. In our case, `OnFinalise`.
extern crate sr_primitives as runtime_primitives;
// `system` module provides us with all sorts of useful stuff and macros
// depend on it being around.
extern crate srml_system as system;
// `balances` module is needed for our little example. It's not required in
// general (though if you want your module to be able to work with tokens, then you
// might find it useful).
extern crate srml_balances as balances;

use runtime_primitives::traits::OnFinalise;
use runtime_support::{StorageValue, dispatch::Result};
use system::ensure_signed;

/// Our module's configuration trait. All our types and consts go in here. If the
/// module is dependent on specific other modules, then their configuration traits
/// should be added to our implied traits list.
///
/// `system::Trait` should always be included in our implied traits.
pub trait Trait: balances::Trait {
	/// The overarching event type.
	type Event: From<Event<Self>> + Into<<Self as system::Trait>::Event>;
}

// The module declaration. This states the entry points that we handle. The
// macro takes care of the marshalling of arguments and dispatch.
//
// Anyone can have these functions execute by signing and submitting
// an extrinsic. Ensure that calls into each of these execute in a time, memory and
// using storage space proportional to any costs paid for by the caller or otherwise the
// difficulty of forcing the call to happen.
//
// Generally you'll want to split these into three groups:
// - Public calls that are signed by an external account.
// - Root calls that are allowed to be made only by the governance system.
// - Inherent calls that are allowed to be made only by the block authors and validators.
//
// Information about where this dispatch initiated from is provided as the first argument
// "origin". As such functions must always look like:
//
// `fn foo(origin, bar: Bar, baz: Baz) -> Result;`
//
// The `Result` is required as part of the syntax (and expands to the conventional dispatch
// result of `Result<(), &'static str>`).
//
// When you come to `impl` them later in the module, you must specify the full type for `origin`:
//
// `fn foo(origin: T::Origin, bar: Bar, baz: Baz) { ... }`
//
// There are three entries in the `system::Origin` enum that correspond
// to the above bullets: `::Signed(AccountId)`, `::Root` and `::Inherent`. You should always match
// against them as the first thing you do in your function. There are three convenience calls
// in system that do the matching for you and return a convenient result: `ensure_signed`,
// `ensure_root` and `ensure_inherent`.
decl_module! {
	// Simple declaration of the `Module` type. Lets the macro know what its working on.
	pub struct Module<T: Trait> for enum Call where origin: T::Origin {
		/// This is your public interface. Be extremely careful.
		/// This is just a simple example of how to interact with the module from the external
		/// world.
		fn accumulate_dummy(origin, increase_by: T::Balance) -> Result;

		/// A privileged call; in this case it resets our dummy value to something new.
		fn set_dummy(new_dummy: T::Balance) -> Result;
	}
}

/// An event in this module. Events are simple means of reporting specific conditions and
/// circumstances that have happened that users, Dapps and/or chain explorers would find
/// interesting and otherwise difficult to detect.
decl_event!(
	pub enum Event<T> where B = <T as balances::Trait>::Balance {
		// Just a normal `enum`, here's a dummy event to ensure it compiles.
		/// Dummy event, just here so there's a generic type that's used.
		Dummy(B),
	}
);

decl_storage! {
	// A macro for the Storage trait, and its implementation, for this module.
	// This allows for type-safe usage of the Substrate storage database, so you can
	// keep things around between blocks.
	trait Store for Module<T: Trait> as Example {
		// Any storage declarations of the form:
		//   `pub? Name get(getter_name)? [config()|config(myname)] [build(|_| {...})] : <type> (= <new_default_value>)?;`
		// where `<type>` is either:
		//   - `Type` (a basic value item); or
		//   - `map KeyType => ValueType` (a map item).
		//
		// Note that there are two optional modifiers for the storage type declaration.
		// - `Foo: Option<u32>`:
		//   - `Foo::put(1); Foo::get()` returns `Some(1)`;
		//   - `Foo::kill(); Foo::get()` returns `None`.
		// - `Foo: u32`:
		//   - `Foo::put(1); Foo::get()` returns `1`;
		//   - `Foo::kill(); Foo::get()` returns `0` (u32::default()).
		// e.g. Foo: u32;
		// e.g. pub Bar get(bar): map T::AccountId => Vec<(T::Balance, u64)>;
		//
		// For basic value items, you'll get a type which implements
		// `runtime_support::StorageValue`. For map items, you'll get a type which
		// implements `runtime_support::StorageMap`.
		//
		// If they have a getter (`get(getter_name)`), then your module will come
		// equipped with `fn getter_name() -> Type` for basic value items or
		// `fn getter_name(key: KeyType) -> ValueType` for map items.
		Dummy get(dummy) config(): Option<T::Balance>;

		// this one uses the default, we'll demonstrate the usage of 'mutate' API.
		Foo get(foo) config(): T::Balance;
	}
}

// The main implementation block for the module. Functions here fall into three broad
// categories:
// - Implementations of dispatch functions. The dispatch code generated by the module macro
// expects each of its functions to be implemented.
// - Public interface. These are functions that are `pub` and generally fall into inspector
// functions that do not write to storage and operation functions that do.
// - Private functions. These are your usual private utilities unavailable to other modules.
impl<T: Trait> Module<T> {
	/// Deposit one of this module's events.
	// TODO: move into `decl_module` macro.
	fn deposit_event(event: Event<T>) {
		<system::Module<T>>::deposit_event(<T as Trait>::Event::from(event).into());
	}

	// Implement Calls and add public immutables and private mutables.

	// Implement dispatched function `accumulate_dummy`. This just increases the value
	// of `Dummy` by `increase_by`.
	//
	// Since this is a dispatched function there are two extremely important things to
	// remember:
	//
	// - MUST NOT PANIC: Under no circumstances (save, perhaps, storage getting into an
	// irreparably damaged state) must this function panic.
	// - NO SIDE-EFFECTS ON ERROR: This function must either complete totally (and return
	// `Ok(())` or it must have no side-effects on storage and return `Err('Some reason')`.
	//
	// The first is relatively easy to audit for - just ensure all panickers are removed from
	// logic that executes in production (which you do anyway, right?!). To ensure the second
	// is followed, you should do all tests for validity at the top of your function. This
	// is stuff like checking the sender (`origin`) or that state is such that the operation
	// makes sense.
	//
	// Once you've determined that it's all good, then enact the operation and change storage.
	// If you can't be certain that the operation will succeed without substantial computation
	// then you have a classic blockchain attack scenario. The normal way of managing this is
	// to attach a bond to the operation. As the first major alteration of storage, reserve
	// some value from the sender's account (`Balances` module has a `reserve` function for
	// exactly this scenario). This amount should be enough to cover any costs of the
	// substantial execution in case it turns out that you can't proceed with the operation.
	//
	// If it eventually transpires that the operation is fine and, therefore, that the
	// expense of the checks should be borne by the network, then you can refund the reserved
	// deposit. If, however, the operation turns out to be invalid and the computation is
	// wasted, then you can burn it or repatriate elsewhere.
	//
	// Security bonds ensure that attackers can't game it by ensuring that anyone interacting
	// with the system either progresses it or pays for the trouble of faffing around with
	// no progress.
	//
	// If you don't respect these rules, it is likely that your chain will be attackable.
	fn accumulate_dummy(origin: T::Origin, increase_by: T::Balance) -> Result {
		// This is a public call, so we ensure that the origin is some signed account.
		let _sender = ensure_signed(origin)?;

		// Read the value of dummy from storage.
		// let dummy = Self::dummy();
		// Will also work using the `::get` on the storage item type itself:
		// let dummy = <Dummy<T>>::get();

		// Calculate the new value.
		// let new_dummy = dummy.map_or(increase_by, |dummy| dummy + increase_by);

		// Put the new value into storage.
		// <Dummy<T>>::put(new_dummy);
		// Will also work with a reference:
		// <Dummy<T>>::put(&new_dummy);

		// Here's the new one of read and then modify the value.
		<Dummy<T>>::mutate(|dummy| {
			let new_dummy = dummy.map_or(increase_by, |dummy| dummy + increase_by);
			*dummy = Some(new_dummy);
		});

		// Let's deposit an event to let the outside world know this happened.
		Self::deposit_event(RawEvent::Dummy(increase_by));

		// All good.
		Ok(())
	}

	fn accumulate_foo(origin: T::Origin, increase_by: T::Balance) -> Result {
		let _sender = ensure_signed(origin)?;

		// Because Foo has 'default', the type of 'foo' in closure is the raw type instead of an Option<> type.
		<Foo<T>>::mutate(|foo| *foo = *foo + increase_by);

		Ok(())
	}

	// Implementation of a privileged call. This doesn't have an `origin` parameter because
	// it's not (directly) from an extrinsic, but rather the system as a whole has decided
	// to execute it. Different runtimes have different reasons for allow privileged
	// calls to be executed - we don't need to care why. Because it's privileged, we can
	// assume it's a one-off operation and substantial processing/storage/memory can be used
	// without worrying about gameability or attack scenarios.
	fn set_dummy(new_value: T::Balance) -> Result {
		// Put the new value into storage.
		<Dummy<T>>::put(new_value);

		// All good.
		Ok(())
	}
}

// This trait expresses what should happen when the block is finalised.
impl<T: Trait> OnFinalise<T::BlockNumber> for Module<T> {
	fn on_finalise(_: T::BlockNumber) {
		// Anything that needs to be done at the end of the block.
		// We just kill our dummy storage item.
		<Dummy<T>>::kill();
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	use runtime_io::with_externalities;
	use substrate_primitives::{H256, Blake2Hasher};
	use runtime_primitives::BuildStorage;
	use runtime_primitives::traits::{BlakeTwo256};
	use runtime_primitives::testing::DigestItem;

	// The testing primitives are very useful for avoiding having to work with signatures
	// or public keys. `u64` is used as the `AccountId` and no `Signature`s are requried.
	use runtime_primitives::testing::{Digest, Header};

	impl_outer_origin! {
		pub enum Origin for Test {}
	}

	// For testing the module, we construct most of a mock runtime. This means
	// first constructing a configuration type (`Test`) which `impl`s each of the
	// configuration traits of modules we want to use.
	#[derive(Clone, Eq, PartialEq)]
	pub struct Test;
	impl system::Trait for Test {
		type Origin = Origin;
		type Index = u64;
		type BlockNumber = u64;
		type Hash = H256;
		type Hashing = BlakeTwo256;
		type Digest = Digest;
		type AccountId = u64;
		type Header = Header;
		type Event = ();
		type Log = DigestItem;
	}
	impl balances::Trait for Test {
		type Balance = u64;
		type AccountIndex = u64;
		type OnFreeBalanceZero = ();
		type EnsureAccountLiquid = ();
		type Event = ();
	}
	impl Trait for Test {
		type Event = ();
	}
	type Example = Module<Test>;

	// This function basically just builds a genesis storage key/value store according to
	// our desired mockup.
	fn new_test_ext() -> runtime_io::TestExternalities<Blake2Hasher> {
		let mut t = system::GenesisConfig::<Test>::default().build_storage().unwrap();
		// We use default for brevity, but you can configure as desired if needed.
		t.extend(balances::GenesisConfig::<Test>::default().build_storage().unwrap());
		t.extend(GenesisConfig::<Test>{
			dummy: 42,
			foo: 24,
		}.build_storage().unwrap());
		t.into()
	}

	#[test]
	fn it_works_for_optional_value() {
		with_externalities(&mut new_test_ext(), || {
			// Check that GenesisBuilder works properly.
			assert_eq!(Example::dummy(), Some(42));

			// Check that accumulate works when we have Some value in Dummy already.
			assert_ok!(Example::accumulate_dummy(Origin::signed(1), 27));
			assert_eq!(Example::dummy(), Some(69));

			// Check that finalising the block removes Dummy from storage.
			<Example as OnFinalise<u64>>::on_finalise(1);
			assert_eq!(Example::dummy(), None);

			// Check that accumulate works when we Dummy has None in it.
			assert_ok!(Example::accumulate_dummy(Origin::signed(1), 42));
			assert_eq!(Example::dummy(), Some(42));
		});
	}

	#[test]
	fn it_works_for_default_value() {
		with_externalities(&mut new_test_ext(), || {
			assert_eq!(Example::foo(), 24);
			assert_ok!(Example::accumulate_foo(Origin::signed(1), 1));
			assert_eq!(Example::foo(), 25);
		});
	}
}
