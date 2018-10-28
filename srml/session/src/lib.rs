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

//! Session manager: is told the validators and allows them to manage their session keys for the
//! consensus module.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "std")]
#[macro_use]
extern crate serde_derive;

extern crate sr_std as rstd;

#[macro_use]
extern crate srml_support as runtime_support;

#[macro_use]
extern crate parity_codec_derive;

#[cfg(test)]
extern crate substrate_primitives;
#[cfg(test)]
extern crate sr_io as runtime_io;
extern crate parity_codec as codec;
extern crate sr_primitives as primitives;
extern crate srml_consensus as consensus;
extern crate srml_system as system;
extern crate srml_timestamp as timestamp;

use rstd::prelude::*;
use primitives::traits::{As, Zero, One, Convert};
use codec::HasCompact;
use runtime_support::{StorageValue, StorageMap};
use runtime_support::dispatch::Result;
use system::ensure_signed;
use rstd::ops::Mul;

/// A session has changed.
pub trait OnSessionChange<T> {
	/// Session has changed.
	fn on_session_change(time_elapsed: T, should_reward: bool);
}

impl<T> OnSessionChange<T> for () {
	fn on_session_change(_: T, _: bool) {}
}

pub trait Trait: timestamp::Trait {
	type ConvertAccountIdToSessionKey: Convert<Self::AccountId, Self::SessionKey>;
	type OnSessionChange: OnSessionChange<Self::Moment>;
	type Event: From<Event<Self>> + Into<<Self as system::Trait>::Event>;
}

decl_module! {
	pub struct Module<T: Trait> for enum Call where origin: T::Origin {
		fn set_key(origin, key: T::SessionKey) -> Result;

		fn set_length(new: <T::BlockNumber as HasCompact>::Type) -> Result;
		fn force_new_session(apply_rewards: bool) -> Result;
		fn on_finalise(n: T::BlockNumber) {
			Self::check_rotate_session(n);
		}
	}
}

/// An event in this module.
decl_event!(
	pub enum Event<T> where <T as system::Trait>::BlockNumber {
		/// New session has happened. Note that the argument is the session index, not the block
		/// number as the type might suggest.
		NewSession(BlockNumber),
	}
);

decl_storage! {
	trait Store for Module<T: Trait> as Session {

		/// The current set of validators.
		pub Validators get(validators) config(): Vec<T::AccountId>;
		/// Current length of the session.
		pub SessionLength get(length) config(session_length): T::BlockNumber = T::BlockNumber::sa(1000);
		/// Current index of the session.
		pub CurrentIndex get(current_index) build(|_| T::BlockNumber::sa(0)): T::BlockNumber;
		/// Timestamp when current session started.
		pub CurrentStart get(current_start) build(|_| T::Moment::zero()): T::Moment;

		/// New session is being forced is this entry exists; in which case, the boolean value is whether
		/// the new session should be considered a normal rotation (rewardable) or exceptional (slashable).
		pub ForcingNewSession get(forcing_new_session): Option<bool>;
		/// Block at which the session length last changed.
		LastLengthChange: Option<T::BlockNumber>;
		/// The next key for a given validator.
		NextKeyFor: map T::AccountId => Option<T::SessionKey>;
		/// The next session length.
		NextSessionLength: Option<T::BlockNumber>;
	}
}

impl<T: Trait> Module<T> {

	/// Deposit one of this module's events.
	fn deposit_event(event: Event<T>) {
		<system::Module<T>>::deposit_event(<T as Trait>::Event::from(event).into());
	}

	/// The number of validators currently.
	pub fn validator_count() -> u32 {
		<Validators<T>>::get().len() as u32	// TODO: can probably optimised
	}

	/// The last length change, if there was one, zero if not.
	pub fn last_length_change() -> T::BlockNumber {
		<LastLengthChange<T>>::get().unwrap_or_else(T::BlockNumber::zero)
	}

	/// Sets the session key of `_validator` to `_key`. This doesn't take effect until the next
	/// session.
	fn set_key(origin: T::Origin, key: T::SessionKey) -> Result {
		let who = ensure_signed(origin)?;
		// set new value for next session
		<NextKeyFor<T>>::insert(who, key);
		Ok(())
	}

	/// Set a new session length. Won't kick in until the next session change (at current length).
	fn set_length(new: <T::BlockNumber as HasCompact>::Type) -> Result {
		<NextSessionLength<T>>::put(new.into());
		Ok(())
	}

	/// Forces a new session.
	pub fn force_new_session(apply_rewards: bool) -> Result {
		Self::apply_force_new_session(apply_rewards)
	}

	// INTERNAL API (available to other runtime modules)

	/// Forces a new session, no origin.
	pub fn apply_force_new_session(apply_rewards: bool) -> Result {
		<ForcingNewSession<T>>::put(apply_rewards);
		Ok(())
	}

	/// Set the current set of validators.
	///
	/// Called by `staking::new_era()` only. `next_session` should be called after this in order to
	/// update the session keys to the next validator set.
	pub fn set_validators(new: &[T::AccountId]) {
		<Validators<T>>::put(&new.to_vec());			// TODO: optimise.
		<consensus::Module<T>>::set_authorities(
			&new.iter().cloned().map(T::ConvertAccountIdToSessionKey::convert).collect::<Vec<_>>()
		);
	}

	/// Hook to be called after transaction processing.
	pub fn check_rotate_session(block_number: T::BlockNumber) {
		// do this last, after the staking system has had chance to switch out the authorities for the
		// new set.
		// check block number and call next_session if necessary.
		let is_final_block = ((block_number - Self::last_length_change()) % Self::length()).is_zero();
		let (should_end_session, apply_rewards) = <ForcingNewSession<T>>::take()
			.map_or((is_final_block, is_final_block), |apply_rewards| (true, apply_rewards));
		if should_end_session {
			Self::rotate_session(is_final_block, apply_rewards);
		}
	}

	/// Move onto next session: register the new authority set.
	pub fn rotate_session(is_final_block: bool, apply_rewards: bool) {
		let now = <timestamp::Module<T>>::get();
		let time_elapsed = now.clone() - Self::current_start();
		let session_index = <CurrentIndex<T>>::get() + One::one();

		Self::deposit_event(RawEvent::NewSession(session_index));

		// Increment current session index.
		<CurrentIndex<T>>::put(session_index);
		<CurrentStart<T>>::put(now);

		// Enact session length change.
		let len_changed = if let Some(next_len) = <NextSessionLength<T>>::take() {
			<SessionLength<T>>::put(next_len);
			true
		} else {
			false
		};
		if len_changed || !is_final_block {
			let block_number = <system::Module<T>>::block_number();
			<LastLengthChange<T>>::put(block_number);
		}

		T::OnSessionChange::on_session_change(time_elapsed, apply_rewards);

		// Update any changes in session keys.
		Self::validators().iter().enumerate().for_each(|(i, v)| {
			if let Some(n) = <NextKeyFor<T>>::take(v) {
				<consensus::Module<T>>::set_authority(i as u32, &n);
			}
		});
	}

	/// Get the time that should have elapsed over a session if everything was working perfectly.
	pub fn ideal_session_duration() -> T::Moment {
		let block_period: T::Moment = <timestamp::Module<T>>::block_period();
		let session_length: T::BlockNumber = Self::length();
		Mul::<T::BlockNumber>::mul(block_period, session_length)
	}

	/// Number of blocks remaining in this session, not counting this one. If the session is
	/// due to rotate at the end of this block, then it will return 0. If the just began, then
	/// it will return `Self::length() - 1`.
	pub fn blocks_remaining() -> T::BlockNumber {
		let length = Self::length();
		let length_minus_1 = length - One::one();
		let block_number = <system::Module<T>>::block_number();
		length_minus_1 - (block_number - Self::last_length_change() + length_minus_1) % length
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use runtime_io::with_externalities;
	use substrate_primitives::{H256, Blake2Hasher};
	use primitives::BuildStorage;
	use primitives::traits::{Identity, BlakeTwo256};
	use primitives::testing::{Digest, DigestItem, Header};

	impl_outer_origin!{
		pub enum Origin for Test {}
	}

	#[derive(Clone, Eq, PartialEq)]
	pub struct Test;
	impl consensus::Trait for Test {
		const NOTE_OFFLINE_POSITION: u32 = 1;
		type Log = DigestItem;
		type SessionKey = u64;
		type OnOfflineValidator = ();
	}
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
	impl timestamp::Trait for Test {
		const TIMESTAMP_SET_POSITION: u32 = 0;
		type Moment = u64;
	}
	impl Trait for Test {
		type ConvertAccountIdToSessionKey = Identity;
		type OnSessionChange = ();
		type Event = ();
	}

	type System = system::Module<Test>;
	type Consensus = consensus::Module<Test>;
	type Session = Module<Test>;

	fn new_test_ext() -> runtime_io::TestExternalities<Blake2Hasher> {
		let mut t = system::GenesisConfig::<Test>::default().build_storage().unwrap();
		t.extend(consensus::GenesisConfig::<Test>{
			code: vec![],
			authorities: vec![1, 2, 3],
		}.build_storage().unwrap());
		t.extend(timestamp::GenesisConfig::<Test>{
			period: 5,
		}.build_storage().unwrap());
		t.extend(GenesisConfig::<Test>{
			session_length: 2,
			validators: vec![1, 2, 3],
		}.build_storage().unwrap());
		runtime_io::TestExternalities::new(t)
	}

	#[test]
	fn simple_setup_should_work() {
		with_externalities(&mut new_test_ext(), || {
			assert_eq!(Consensus::authorities(), vec![1, 2, 3]);
			assert_eq!(Session::length(), 2);
			assert_eq!(Session::validators(), vec![1, 2, 3]);
		});
	}

	#[test]
	fn should_work_with_early_exit() {
		with_externalities(&mut new_test_ext(), || {
			System::set_block_number(1);
			assert_ok!(Session::set_length(10.into()));
			assert_eq!(Session::blocks_remaining(), 1);
			Session::check_rotate_session(1);

			System::set_block_number(2);
			assert_eq!(Session::blocks_remaining(), 0);
			Session::check_rotate_session(2);
			assert_eq!(Session::length(), 10);

			System::set_block_number(7);
			assert_eq!(Session::current_index(), 1);
			assert_eq!(Session::blocks_remaining(), 5);
			assert_ok!(Session::force_new_session(false));
			Session::check_rotate_session(7);

			System::set_block_number(8);
			assert_eq!(Session::current_index(), 2);
			assert_eq!(Session::blocks_remaining(), 9);
			Session::check_rotate_session(8);

			System::set_block_number(17);
			assert_eq!(Session::current_index(), 2);
			assert_eq!(Session::blocks_remaining(), 0);
			Session::check_rotate_session(17);

			System::set_block_number(18);
			assert_eq!(Session::current_index(), 3);
		});
	}

	#[test]
	fn session_length_change_should_work() {
		with_externalities(&mut new_test_ext(), || {
			// Block 1: Change to length 3; no visible change.
			System::set_block_number(1);
			assert_ok!(Session::set_length(3.into()));
			Session::check_rotate_session(1);
			assert_eq!(Session::length(), 2);
			assert_eq!(Session::current_index(), 0);

			// Block 2: Length now changed to 3. Index incremented.
			System::set_block_number(2);
			assert_ok!(Session::set_length(3.into()));
			Session::check_rotate_session(2);
			assert_eq!(Session::length(), 3);
			assert_eq!(Session::current_index(), 1);

			// Block 3: Length now changed to 3. Index incremented.
			System::set_block_number(3);
			Session::check_rotate_session(3);
			assert_eq!(Session::length(), 3);
			assert_eq!(Session::current_index(), 1);

			// Block 4: Change to length 2; no visible change.
			System::set_block_number(4);
			assert_ok!(Session::set_length(2.into()));
			Session::check_rotate_session(4);
			assert_eq!(Session::length(), 3);
			assert_eq!(Session::current_index(), 1);

			// Block 5: Length now changed to 2. Index incremented.
			System::set_block_number(5);
			Session::check_rotate_session(5);
			assert_eq!(Session::length(), 2);
			assert_eq!(Session::current_index(), 2);

			// Block 6: No change.
			System::set_block_number(6);
			Session::check_rotate_session(6);
			assert_eq!(Session::length(), 2);
			assert_eq!(Session::current_index(), 2);

			// Block 7: Next index.
			System::set_block_number(7);
			Session::check_rotate_session(7);
			assert_eq!(Session::length(), 2);
			assert_eq!(Session::current_index(), 3);
		});
	}

	#[test]
	fn session_change_should_work() {
		with_externalities(&mut new_test_ext(), || {
			// Block 1: No change
			System::set_block_number(1);
			Session::check_rotate_session(1);
			assert_eq!(Consensus::authorities(), vec![1, 2, 3]);

			// Block 2: Session rollover, but no change.
			System::set_block_number(2);
			Session::check_rotate_session(2);
			assert_eq!(Consensus::authorities(), vec![1, 2, 3]);

			// Block 3: Set new key for validator 2; no visible change.
			System::set_block_number(3);
			assert_ok!(Session::set_key(Origin::signed(2), 5));
			assert_eq!(Consensus::authorities(), vec![1, 2, 3]);

			Session::check_rotate_session(3);
			assert_eq!(Consensus::authorities(), vec![1, 2, 3]);

			// Block 4: Session rollover, authority 2 changes.
			System::set_block_number(4);
			Session::check_rotate_session(4);
			assert_eq!(Consensus::authorities(), vec![1, 5, 3]);
		});
	}
}
