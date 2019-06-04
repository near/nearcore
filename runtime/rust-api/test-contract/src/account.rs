use crate::asset::*;
use crate::rate::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops;

#[derive(PartialEq, Eq, PartialOrd, Hash, Clone, Copy, Serialize, Deserialize)]
pub struct Quantity(pub i32);

#[derive(Clone, Serialize, Deserialize)]
pub struct Account(pub HashMap<Asset, Quantity>);

pub enum Tranx {
    Approved(Account, Account),
    Denied(HashMap<Asset, Quantity>),
}

impl Account {
    pub fn quantity(&self, asset: &Asset) -> Quantity {
        match self.0.get(asset) {
            Some(quantity) => quantity.clone(),
            None => Quantity(0),
        }
    }

    pub fn exchange(rate: &Rate, quantity: Quantity, buyer: &Account, seller: &Account) -> Tranx {
        let credit = &Account(rate.credit.clone()) * quantity;
        let debit = &Account(rate.debit.clone()) * quantity;
        let (buyer, seller) = (&(buyer - &debit) + &credit, &(seller - &credit) + &debit);
        let mut success = true;
        let mut deficit = HashMap::new();
        {
            let Account(buyer) = &buyer;
            let Account(debit) = debit;
            for asset in debit.keys() {
                match buyer.get(asset) {
                    Some(Quantity(quantity)) if *quantity < 0 => {
                        success = false;
                        deficit.insert(asset.clone(), Quantity(*quantity));
                    }
                    _ => (),
                }
            }
        }
        if success {
            Tranx::Approved(buyer, seller)
        } else {
            Tranx::Denied(deficit)
        }
    }

    pub fn map(&self) -> &HashMap<Asset, Quantity> {
        let Account(map) = self;
        map
    }

    fn prime(&mut self, rhs: &Account) {
        let Account(lhs) = self;
        let Account(rhs) = rhs;
        for rhs_key in rhs.keys() {
            if !lhs.contains_key(rhs_key) {
                lhs.insert(rhs_key.clone(), Quantity(0));
            }
        }
    }

    fn op<F>(lhs: &Account, rhs: &Account, op: F) -> Account
    where
        F: Fn(&Quantity, &Quantity) -> Quantity,
    {
        let mut acc = HashMap::new();
        let mut lhs = lhs.clone();
        let mut rhs = rhs.clone();
        lhs.prime(&rhs);
        rhs.prime(&lhs);
        let Account(lhs) = lhs;
        let Account(rhs) = rhs;
        for key in lhs.keys() {
            let lhs_quantity = lhs.get(key).unwrap();
            let rhs_quantity = rhs.get(key).unwrap();
            let quantity = op(lhs_quantity, rhs_quantity);
            acc.insert(key.clone(), quantity.clone());
        }
        Account(acc)
    }
}

impl PartialEq for Account {
    fn eq(&self, rhs: &Account) -> bool {
        let mut lhs = self.clone();
        let mut rhs = rhs.clone();
        lhs.prime(&rhs);
        rhs.prime(&lhs);
        let Account(lhs) = lhs;
        let Account(rhs) = rhs;
        lhs == rhs
    }
}

impl<'a, 'b> ops::Add<&'a Account> for &'b Account {
    type Output = Account;

    fn add(self, rhs: &Account) -> Account {
        Account::op(self, rhs, |Quantity(lq), Quantity(rq)| Quantity(lq + rq))
    }
}

impl<'a, 'b> ops::Sub<&'a Account> for &'b Account {
    type Output = Account;

    fn sub(self, rhs: &Account) -> Account {
        Account::op(self, rhs, |Quantity(lq), Quantity(rq)| Quantity(lq - rq))
    }
}

impl<'a> ops::Mul<Quantity> for &'a Account {
    type Output = Account;

    fn mul(self, rhs: Quantity) -> Account {
        let Account(lhs) = self;
        let keys = lhs.keys();
        let mut lhs = lhs.clone();
        let Quantity(rhs_quantity) = rhs;
        for key in keys {
            let q = lhs.entry(key.clone()).or_insert(Quantity(0));
            let Quantity(lhs_quantity) = *q;
            *q = Quantity(lhs_quantity * rhs_quantity);
        }
        Account(lhs)
    }
}
