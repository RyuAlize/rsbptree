use std::cell::RefCell;
use std::option::Option;
use std::fmt::Debug;
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};
use crate::bptree::BtreeNode::inner;
use super::kvtype::KVType;

#[derive(Debug)]
pub struct Bptree<K, V> {
    mutex: Mutex<bool>,
    root: BtreeNode<K,V>,
    m: usize,
}

impl<K, V> Bptree<K, V>
    where K : Debug + Clone + Ord + KVType,
          V : Debug + Clone + Ord + KVType,
{
    pub fn new(m: usize) -> Self {
        Self{
            mutex: Mutex::new(true),
            root: BtreeNode::placehold,
            m,
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.root.get(key)
    }

    pub fn set(&mut self, key: K, val: V)  {
        self.mutex.lock();
        match self.root {
            BtreeNode::placehold => {
                let mut new_leaf = LeafNode::new(self.m-1);
                new_leaf.set(key,val);
                self.root = BtreeNode::leaf(Arc::new(Mutex::new(new_leaf)));
            },
            _ => {
                match self.root.set(key,val) {
                    None => {},
                    Some((split_key, mut new_btree_node)) => {
                        let left_child = self.root.clone();
                        let mut new_inner = InnerNode::new(self.m-1);
                        new_inner.keys.push(split_key);
                        new_inner.childNodeptrs.push(left_child);
                        new_inner.childNodeptrs.push(new_btree_node);

                        self.root = BtreeNode::inner((Arc::new(Mutex::new(new_inner))));

                    }
                }
            }
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.mutex.lock();
        if self.root.keys_len() == 0 {
            let root = self.root.clone();
            if let BtreeNode::inner(inner_node_arc) = root {
                let inner_node_content = inner_node_arc.lock().unwrap();
                let child = inner_node_content.childNodeptrs[0].clone();
                self.root = child;
            }
        }
        match self.root.remove(key, None, None) {
            (None, None, None) => {return None;}
            (_, _, Some(old_val)) => {

                return Some(old_val);
            }
            _ => {unreachable!()}
        }
    }


}

#[derive(Debug, Clone)]
pub enum BtreeNode<K, V> {
    inner(Arc<Mutex<InnerNode<K, V>>>),
    leaf(Arc<Mutex<LeafNode<K, V>>>),
    placehold,
}

impl<K, V> BtreeNode<K, V>
    where K : Debug + Clone + Ord + KVType,
          V : Debug + Clone + Ord + KVType,
{
    pub fn get(&self, key: &K) -> Option<V> {
        match self{
            Self::leaf(leaf_node_ref) =>{
                let leaf_node_content = leaf_node_ref.lock().unwrap();
                let res = leaf_node_content.get(key);
                return res;
            },
            Self::inner(inner_node_ref) =>{
                let inner_node_content = inner_node_ref.lock().unwrap();
                let res = inner_node_content.get(key);
                return res;
            },
            Self::placehold => {return None;}
        }
    }

    pub fn set(&mut self, key: K, val: V) -> Option<(K, BtreeNode<K, V>)> {
        match self{
            Self::leaf(leaf_node_ref) => {
                let mut leaf_node_content = leaf_node_ref.lock().unwrap();
                return leaf_node_content.set(key, val);
            },
            Self::inner(inner_node_ref) => {
                let mut inner_node_content = inner_node_ref.lock().unwrap();
                return inner_node_content.set(key, val);
            }
            Self::placehold => {return None;}
        }
    }

    pub fn remove(&mut self, key: &K, left_slibing: Option<BtreeNode<K,V>>,
                  right_slibing: Option<BtreeNode<K,V>>) -> (Option<K>, Option<K>, Option<V>) {
        match self{
            Self::leaf(leaf_node_ref) => {
                let mut leaf_node_content = leaf_node_ref.lock().unwrap();
                return leaf_node_content.remove(key, left_slibing, right_slibing);
            },
            Self::inner(inner_node_ref) => {
                let mut inner_node_content = inner_node_ref.lock().unwrap();
                return inner_node_content.remove(key, left_slibing, right_slibing);
            }
            Self::placehold => {return (None, None, None);}
        }
    }

    pub fn keys_len(&self) -> usize {
        match self{
            Self::leaf(leaf_node_ref) => {
                let mut leaf_node_content = leaf_node_ref.lock().unwrap();
                return leaf_node_content.keys.len();
            },
            Self::inner(inner_node_ref) => {
                let mut inner_node_content = inner_node_ref.lock().unwrap();
                return inner_node_content.keys.len();
            }
            Self::placehold => {0}
        }
    }
}

#[derive(Debug, Clone)]
pub struct InnerNode<K,V>{
    keys: Vec<K>,
    childNodeptrs: Vec<BtreeNode<K,V>>,
    max_key_count: usize,
}

impl<K,V> InnerNode<K,V>
    where K : Debug + Clone + Ord + KVType,
          V : Debug + Clone + Ord + KVType,
{
    pub fn new(max_key_count: usize) -> Self {
        Self{
            keys: Vec::with_capacity(max_key_count),
            childNodeptrs: Vec::with_capacity(max_key_count+1),
            max_key_count,
        }
    }

    pub fn from(keys: &[K], vals: &[BtreeNode<K, V>], max_key_count: usize) -> Self{
        Self{
            keys: keys.to_vec(),
            childNodeptrs: vals.to_vec(),
            max_key_count,
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let mut index = 0;
        match self.keys.binary_search(key){
            Err(i) =>{ index = i;},
            Ok(i) =>{index = i+1;},
        }
        self.childNodeptrs[index].get(key)
    }

    pub fn set(&mut self, key: K, val: V) -> Option<(K, BtreeNode<K, V>)> {
        let mut index = 0;
        match self.keys.binary_search(&key){
            Err(i) =>{ index = i;},
            Ok(i) =>{index = i+1;},
        }

        match self.childNodeptrs[index].set(key, val) {
            None =>{ return None;}
            Some((split_key, new_btree_node)) => {
                match self.keys.binary_search(&split_key) {
                    Ok(_) => unreachable!(),
                    Err(index) => {
                        self.keys.insert(index, split_key);
                        self.childNodeptrs.insert(index+1, new_btree_node);
                    }
                }

                match self.need_split() {
                    false => { return None; },
                    true => {
                        if let Some((split_key, new_inner_cell)) = self.split(self.split_at()){
                            let new_btree_node = BtreeNode::inner(new_inner_cell);
                            return Some((split_key, new_btree_node));
                        }
                        else{
                            unreachable!()
                        }
                    }
                }
            }
        }
    }

    pub fn remove(&mut self,
                  key: &K,
                  left: Option<BtreeNode<K,V>>,
                  right: Option<BtreeNode<K,V>>
    ) -> (Option<K>, Option<K>, Option<V>)
    {
        let mut index = 0;
        let mut old_val;
        let min_key = self.keys[0].clone();
        match self.keys.binary_search(key) {
            Err(i) => {index = i;}
            Ok(i) => {index = i+1;}
        }
        let left_slibing = self.left_slibing(index);
        let right_slibing = self.right_slibing(index);
        match self.childNodeptrs[index].remove(key, left_slibing, right_slibing) {
            (Some(old_key), Some(new_key), Some(val))  => {
                match self.keys.binary_search(&old_key) {
                    Err(_) =>{}
                    Ok(i) =>{self.keys[i] = new_key;}
                }
                old_val = val;
            },
            (Some(old_key), None, Some(val)) => {
                match self.keys.binary_search(&old_key) {
                    Err(_) =>{panic!("btree struct error!")}
                    Ok(i) =>{
                        self.keys.remove(i);
                        self.childNodeptrs.remove(i+1);
                    }
                }
                old_val = val;
            },
            (None, None, Some(old_val)) => {return (None,None,Some(old_val));},
            (None, None, None) => {return (None,None,None);},
            _ => {unreachable!()}
        }
        match self.need_merge() {
            false => {
                if self.keys[0] != min_key {
                    let new_min_key = self.keys[0].clone();
                    return (Some(min_key), Some(new_min_key), Some(old_val));
                }
                else{
                    return (None, None, Some(old_val));
                }
            }
            true => {
                if let Some(btree_node) = left {
                    match btree_node {
                        BtreeNode::placehold =>{}
                        BtreeNode::leaf(_) => {panic!("bptree struct error!");}
                        BtreeNode::inner(inner_node_cell) => {
                            let mut inner_node_content = inner_node_cell.lock().unwrap();
                            if inner_node_content.can_borrow(){
                                let last_index = inner_node_content.keys.len()-1;
                                let key = inner_node_content.keys.remove(last_index);
                                let childptr = inner_node_content.childNodeptrs.remove(last_index);

                                self.keys.insert(0, key.clone());
                                self.childNodeptrs.insert(1, childptr);// insert behind the placehold
                                return (Some(min_key), Some(key), Some(old_val));
                            }
                            else{
                                self.childNodeptrs.remove(0);//remove the placehold
                                inner_node_content.keys.append(&mut self.keys);
                                inner_node_content.childNodeptrs.append(&mut self.childNodeptrs);

                                return (Some(min_key), None, Some(old_val));
                            }
                        }
                    }
                }
                if let Some(btree_node) = right {
                    match btree_node {
                        BtreeNode::placehold =>{}
                        BtreeNode::leaf(_) => {panic!("bptree struct error!");},
                        BtreeNode::inner(inner_node_cell) => {
                            let mut inner_node_content = inner_node_cell.lock().unwrap();

                            if inner_node_content.can_borrow() {
                                let key = inner_node_content.keys.remove(0);
                                let childptr = inner_node_content.childNodeptrs.remove(1);
                                let old_key = key.clone();
                                let new_key = inner_node_content.keys[0].clone();
                                self.keys.push(key);
                                self.childNodeptrs.push(childptr);

                                return (Some(old_key), Some(new_key), Some(old_val));
                            }
                            else{
                                let old_key = inner_node_content.keys[0].clone();
                                inner_node_content.childNodeptrs.remove(0);
                                self.keys.append(&mut inner_node_content.keys);
                                self.childNodeptrs.append(&mut inner_node_content.childNodeptrs);

                                return (Some(old_key), None, Some(old_val));
                            }

                        }
                    }
                }
            }
        }
        (None,None,Some(old_val))
    }

    fn can_borrow(&self) -> bool {
        self.keys.len() > self.split_at()
    }

    fn left_slibing(&self, index: usize) -> Option<BtreeNode<K, V>> {
        if index > 0 {
            Some(self.childNodeptrs[index - 1].clone())
        }
        else {
            None
        }
    }

    fn right_slibing(&self, index: usize) -> Option<BtreeNode<K, V>> {
        if index < self.childNodeptrs.len()-1 {
            Some(self.childNodeptrs[index + 1].clone())
        }
        else{
            None
        }
    }

    fn need_split(&self) -> bool {
        self.keys.len() > self.max_key_count
    }

    fn need_merge(&self) -> bool {
        self.keys.len() < self.split_at()
    }

    fn split_at(&self) -> usize {
        ((self.max_key_count / 2) + (self.max_key_count % 2)) as usize
    }

    fn split(&mut self, split_at: usize) -> Option<(K, Arc<Mutex<InnerNode<K, V>>>)> {
        let split_key = self.keys[split_at].clone();
        let mut new_inner = InnerNode::from(self.keys[split_at..].as_ref(),
                                            self.childNodeptrs[split_at+1..].as_ref(),
                                            self.max_key_count);
        new_inner.childNodeptrs.insert(0, BtreeNode::placehold);

        self.keys.drain(split_at..);
        self.childNodeptrs.drain(split_at+1..);
        let new_btree_node = Arc::new(Mutex::new(new_inner));
        Some((split_key, new_btree_node))

    }
}


#[derive(Debug, Clone)]
pub struct LeafNode<K, V>{
    keys: Vec<K>,
    vals: Vec<V>,
    next: Option<Arc<Mutex<LeafNode<K, V>>>>,
    max_key_count: usize,
}

impl<K, V> LeafNode<K, V>
    where K : Debug + Clone + Ord + KVType,
          V : Debug + Clone + Ord + KVType,
{
    pub fn new(max_key_count: usize) -> Self{
        Self{
            keys: Vec::with_capacity(max_key_count),
            vals: Vec::with_capacity(max_key_count),
            next: Option::None,
            max_key_count,
        }
    }

    pub fn from(keys: &[K], vals: &[V], max_key_count: usize) -> Self{
        Self{
            keys: keys.to_vec(),
            vals: vals.to_vec(),
            next: Option::None,
            max_key_count,
        }
    }

    fn set_next(&mut self, next: Option<Arc<Mutex<LeafNode<K, V>>>>) {
        self.next = next;
    }

    pub fn get(&self, key: &K) -> Option<V> {
        match self.keys.binary_search(key){
            Ok(i)=>{Some(self.vals[i].clone())}
            Err(_) => None,
        }
    }

    pub fn set(&mut self, key: K, val: V) -> Option<(K, BtreeNode<K, V>)> {
        match self.keys.binary_search(&key){
            Ok(i) => {self.vals[i] = val;}
            Err(i) => {
                self.keys.insert(i, key);
                self.vals.insert(i, val);
            },
        }
        match self.need_split(){
            false => { return None; },
            true => {
                if let Some((split_key, new_leaf_arc)) = self.split(self.split_at()){
                    let new_btree_node = BtreeNode::leaf(new_leaf_arc);
                    return Some((split_key, new_btree_node));
                }
                else{
                    unreachable!()
                }
            }
        }
    }

    pub fn remove(&mut self, key:& K,  left: Option<BtreeNode<K,V>>,
                  right: Option<BtreeNode<K,V>>) -> (Option<K>, Option<K>, Option<V>) {
        let mut old_val = None;
        let min_key = self.keys[0].clone();
        match self.keys.binary_search(key) {
            Err(_) => {return (None, None, None);},
            Ok(i) => {
                let mut old_key = self.keys.remove(i);
                old_val = Some(self.vals.remove(i));

                match self.need_merge() {
                    false => {
                        if i == 0 {
                            let new_min_key = self.keys[0].clone();
                            return (Some(min_key), Some(new_min_key), old_val);
                        }
                        else{
                            return (None, None, old_val);
                        }
                    }
                    true => {
                        if let Some(btree_node) = left {
                            match btree_node {
                                BtreeNode::placehold =>{panic!("leaf node can not be placehold");}
                                BtreeNode::inner(_) => {panic!("bptree struct error!");}
                                BtreeNode::leaf(leaf_node_arc) => {
                                    let mut leaf_node_content = leaf_node_arc.lock().unwrap();
                                    if leaf_node_content.can_borrow(){
                                        let last_index = leaf_node_content.keys.len()-1;
                                        let key = leaf_node_content.keys.remove(last_index);
                                        let val = leaf_node_content.vals.remove(last_index);
                                        if i > 0 {
                                            old_key = self.keys[0].clone();
                                        }

                                        self.keys.insert(0, key.clone());
                                        self.vals.insert(0, val);

                                        return (Some(old_key), Some(key), old_val);
                                    }
                                    else{
                                        if i > 0 {
                                            old_key = self.keys[0].clone();
                                        }

                                        leaf_node_content.keys.append(&mut self.keys);
                                        leaf_node_content.vals.append(&mut self.vals);

                                        return (Some(old_key), None, old_val);
                                    }
                                }
                            }
                        }
                        if let Some(btree_node) = right {
                            match btree_node {
                                BtreeNode::placehold =>{panic!("leaf node can not be placehold");}
                                BtreeNode::inner(_) => {panic!("bptree struct error!");},
                                BtreeNode::leaf(leaf_node_arc) => {
                                    let mut leaf_node_content = leaf_node_arc.lock().unwrap();

                                    if leaf_node_content.can_borrow() {
                                        let key = leaf_node_content.keys.remove(0);
                                        let val = leaf_node_content.vals.remove(0);
                                        old_key = key.clone();
                                        let new_key = leaf_node_content.keys[0].clone();
                                        self.keys.push(key);
                                        self.vals.push(val);

                                        return (Some(old_key), Some(new_key), old_val);
                                    }
                                    else{
                                        old_key = leaf_node_content.keys[0].clone();

                                        self.keys.append(&mut leaf_node_content.keys);
                                        self.vals.append(&mut leaf_node_content.vals);

                                        return (Some(old_key), None, old_val);
                                    }

                                }
                            }
                        }
                    }
                }
            }
        }
        (None, None, old_val)
    }

    fn can_borrow(&self) -> bool {
        self.keys.len() > self.split_at()
    }

    fn need_split(&self) -> bool {
        self.keys.len() > self.max_key_count
    }

    fn need_merge(&self) -> bool {
        self.keys.len() < self.split_at()
    }

    fn split_at(&self) -> usize {
        ((self.max_key_count / 2) + (self.max_key_count % 2)) as usize
    }

    fn split(&mut self, split_at: usize) -> Option<(K, Arc<Mutex<LeafNode<K, V>>>)> {
        let split_key = self.keys[split_at].clone();
        let mut new_leaf = LeafNode::from(self.keys[split_at..].as_ref(),
                                          self.vals[split_at..].as_ref(),
                                          self.max_key_count);

        new_leaf.set_next(self.next.take());
        let new_leaf_arc = Arc::new(Mutex::new(new_leaf));
        self.set_next(Some(new_leaf_arc.clone()));
        self.keys.drain(split_at..);
        self.vals.drain(split_at..);

        Some((split_key,new_leaf_arc))
    }
}
