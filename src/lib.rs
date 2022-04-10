#![allow(dead_code)]

mod bptree;
mod kvtype;
#[cfg(test)]
mod tests {
    use std::borrow::BorrowMut;
    use std::cell::RefCell;
    use std::sync::Arc;
    use kvtype::KVType;
    use crate::{bptree, kvtype};
    use bptree::Bptree;

    impl KVType for i32{}
    impl KVType for &str {}
    #[test]
    fn it_works() {
        let mut bt:Bptree<i32, &str> = Bptree::new(9);
        for i in (0..100000) {
            bt.set(i, "hello");
        }


        for i in (0..100000) {

            if let Some(res) = bt.remove(&i){

            }
            else{
                println!("{}: error", i);
            }


        }


    }

}
