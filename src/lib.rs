#[macro_use]
extern crate queues;

use queues::*;

use rand::Rng;
use std::cell::RefCell;
use std::cmp;
use std::rc::{Rc, Weak};

#[derive(Debug, PartialEq)]
enum Color {
    Red,
    Black,
}

#[derive(Debug)]
struct RBNode {
    color: Color,
    left: Option<Rc<RefCell<RBNode>>>,
    right: Option<Rc<RefCell<RBNode>>>,
    parent: Option<Rc<RefCell<RBNode>>>,
    key: String,
    value: String,
}

impl RBNode {
    fn new(key: String, value: String, parent: Option<Rc<RefCell<RBNode>>>) -> RBNode {
        RBNode {
            color: Color::Black,
            left: None,
            right: None,
            parent: parent,
            key: key,
            value: value,
        }
    }
    fn insert(&mut self, node: Rc<RefCell<RBNode>>, parent_node: Rc<RefCell<RBNode>>) {
        match (self.left.as_ref(), self.right.as_ref()) {
            (None, _) => {
                node.borrow_mut().parent = Some(parent_node);

                self.left = Some(node);
            }
            (_, None) => {
                self.right = Some(node);
            }
            (Some(left), Some(right)) => {
                let mut rng = rand::thread_rng();

                match rng.gen_bool(0.5) {
                    false => {
                        left.borrow_mut().insert(node, Rc::clone(left));
                    }
                    true => {
                        right.borrow_mut().insert(node, Rc::clone(right));
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct RBTree {
    root_node: Option<Rc<RefCell<RBNode>>>,
    breath: Option<usize>,
}

fn print_node(node: &RBNode, num_spaces: usize) {
    println!("{:indent$}{}", "", node.value, indent = num_spaces);
    if let Some(v) = &node.left {
        println!("{:indent$}/", "", indent = num_spaces - 1);
        print_node(&v.borrow(), num_spaces - 2);
    }

    if let Some(v) = &node.right {
        println!("{:indent$}\\", "", indent = num_spaces + 1);
        print_node(&v.borrow(), num_spaces + 2);
    }
}

fn breath_helper(node: &RBNode, current_breath: usize) -> usize {
    match (&node.left, &node.right) {
        (Some(n_left), Some(n_right)) => {
            let breath_left = breath_helper(&n_left.borrow(), current_breath + 1);
            let breath_right = breath_helper(&n_right.borrow(), current_breath + 1);
            cmp::max(breath_left, breath_right)
        }
        (Some(n_left), None) => breath_helper(&n_left.borrow(), current_breath + 1),
        (None, Some(n_right)) => breath_helper(&n_right.borrow(), current_breath + 1),
        (None, None) => current_breath,
    }
}

fn get_helper(node: &RBNode, key: String) -> Result<String, String> {
    if node.key == key {
        Ok(node.value.clone())
    } else {
        match (&node.left, &node.right) {
            (Some(left), _) if key < node.key => get_helper(&left.borrow(), key),
            (_, Some(right)) if node.key < key => get_helper(&right.borrow(), key),
            (_, _) => Err("key not found".to_owned()),
        }
    }
}

impl RBTree {
    pub fn new() -> RBTree {
        RBTree {
            root_node: None,
            breath: None,
        }
    }
    pub fn insert(&mut self, key: String, value: String) {
        match &self.root_node {
            None => {
                self.root_node = Some(Rc::new(RefCell::new(RBNode::new(key, value, None))));
            }
            Some(root_node) => {
                let new_node = Rc::new(RefCell::new(RBNode::new(key, value, None)));
                root_node
                    .borrow_mut()
                    .insert(new_node, Rc::clone(&root_node));
            }
        }
    }
    pub fn print(&self) {
        let mut queue: Queue<(Rc<RefCell<RBNode>>, usize)> = queue![];
        // let root_node = Rc::clone(root_node);
        let mut current_level = 0;
        let mut num_nodes_in_level = 1;
        if let Some(root_node) = &self.root_node {
            queue.add((Rc::clone(root_node), current_level));
        }
        while queue.size() > 0 {
            let (node, level) = match queue.remove() {
                Ok((node, level)) => (node, level),
                Err(_) => {
                    panic!("Could not unpack result from queue");
                }
            };
            if level != current_level {
                current_level = level;
                println!("");
                for i in 1..num_nodes_in_level {
                    // println!(
                    //     "breath={}, i={}, num_nodes_in_level={}, current_level={}, sum=",
                    //     self.breath(),
                    //     i,
                    //     num_nodes_in_level,
                    //     current_level
                    // );
                    print!(
                        "{:indent$}/{:breath$}\\",
                        "",
                        "",
                        indent = self.breath() * 2 - 2 * current_level + 3,
                        breath = usize::pow(3, (self.breath() as u32) - (current_level as u32))
                    );
                }
                println!("");
                print!("");
                num_nodes_in_level = 1;
            }
            print!(
                "{:indent$}{} ",
                "",
                node.borrow().key,
                indent = self.breath() * 2 - current_level * 2 + 2
            );

            match (&node.borrow().left, &node.borrow().right) {
                (Some(left), Some(right)) => {
                    let left = Rc::clone(&left);
                    queue.add((left, current_level + 1));

                    let right = Rc::clone(&right);
                    queue.add((right, current_level + 1));
                }
                (Some(left), None) => {
                    let left = Rc::clone(&left);
                    queue.add((left, current_level + 1));

                    queue.add((
                        Rc::new(RefCell::new(RBNode::new(
                            "".to_owned(),
                            "".to_owned(),
                            None,
                        ))),
                        current_level + 1,
                    ));
                }
                (None, Some(right)) => {
                    queue.add((
                        Rc::new(RefCell::new(RBNode::new(
                            "".to_owned(),
                            "".to_owned(),
                            None,
                        ))),
                        current_level + 1,
                    ));
                    let right = Rc::clone(&right);
                    queue.add((right, current_level + 1));
                }
                (None, None) => {}
            }

            num_nodes_in_level += 1;
            // println!("{:indent$}\\", "", indent = self.breath() * 2 + 1);
        }
        // let num_spaces = 2 * self.breath();
        // println!("tree has breath of {} ", self.breath());
        // print_node(&self.root_node.borrow(), num_spaces);
        // println!("root_node: {}", self.root_node.borrow().data);
    }

    fn breath(&self) -> usize {
        match self.breath {
            Some(v) => v,
            None => match &self.root_node {
                None => 0,
                Some(root_node) => breath_helper(&root_node.borrow(), 0),
            },
        }
    }
    pub fn get(&self, key: String) -> Result<String, String> {
        match &self.root_node {
            Some(root_node) => get_helper(&root_node.borrow(), key),
            None => Err("Tree is empty".to_owned()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn insert_node() {
        let mut rb_tree = RBTree::new();
        rb_tree.insert("a".to_owned(), "value1".to_owned());

        rb_tree.insert("b".to_owned(), "value2".to_owned());

        let mut expected_rb_tree = RBTree::new();
        expected_rb_tree.insert("a".to_owned(), "value1".to_owned());
        expected_rb_tree.root_node.unwrap().borrow_mut().right = Some(Rc::new(RefCell::new(
            RBNode::new("b".to_owned(), "value2".to_owned(), None),
        )));

        rb_tree.print();
    }
}
