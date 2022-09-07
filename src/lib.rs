extern crate queues;

use queues::*;

use std::cell::RefCell;
use std::cmp;
use std::rc::Rc;

#[derive(Debug, PartialEq, Copy, Clone)]
enum Color {
    Red,
    Black,
}

#[derive(Debug)]
pub struct RBNode {
    color: Color,
    pub left: node,
    pub right: node,
    pub parent: node,
    key: String,
    value: String,
}

type node = Option<Rc<RefCell<RBNode>>>;

impl RBNode {
    pub fn new(key: String, value: String, parent: node) -> RBNode {
        RBNode {
            color: Color::Red,
            left: None,
            right: None,
            parent: parent,
            key: key,
            value: value,
        }
    }

    fn insert(&mut self, mut node: RBNode, parent_node: Rc<RefCell<RBNode>>) {
        match (self.left.as_ref(), self.right.as_ref()) {
            (None, _) if node.key < self.key => {
                node.parent = Some(parent_node);
                self.left = Some(Rc::new(RefCell::new(node)));
            }

            (Some(left), _) if node.key < self.key => {
                left.borrow_mut().insert(node, Rc::clone(left));
            }

            (_, None) if node.key > self.key => {
                self.right = Some(Rc::new(RefCell::new(node)));
            }

            (_, Some(right)) if node.key > self.key => {
                right.borrow_mut().insert(node, Rc::clone(right));
            }
            (_, _) if node.key == self.key => {
                self.value = node.value.clone();
            }
            (_, _) => {
                // will never happen
                panic!("the impossible has happened")
            }
        }
    }

    fn father_color(&self) -> Option<Color> {
        match &self.parent {
            Some(parent) => Some(parent.borrow().color),
            None => None,
        }
    }

    fn grand_father(&self) -> node {
        match &self.parent {
            Some(parent) => match &parent.borrow().parent {
                Some(grand_parent) => Some(Rc::clone(grand_parent)),
                None => None,
            },
            None => None,
        }
    }
    fn grand_father_color(&self) -> Option<Color> {
        match &self.grand_father() {
            Some(grand_father) => Some(grand_father.borrow().color),
            None => None,
        }
    }
    fn uncle(&self) -> node {
        match &self.parent {
            Some(parent) => match &parent.borrow().parent {
                Some(grand_parent) if parent.borrow().key < grand_parent.borrow().key => {
                    match &grand_parent.borrow().right {
                        Some(uncle) => Some(Rc::clone(uncle)),
                        None => None,
                    }
                }

                Some(grand_parent) if parent.borrow().key > grand_parent.borrow().key => {
                    match &grand_parent.borrow().left {
                        Some(uncle) => Some(Rc::clone(uncle)),
                        None => None,
                    }
                }
                None => None,
                _ => None,
            },
            None => None,
        }
    }
    fn uncle_color(&self) -> Option<Color> {
        match self.uncle() {
            Some(uncle) => Some(uncle.borrow().color),
            None => None,
        }
    }
}

#[derive(Debug)]
pub struct RBTree {
    pub root_node: node,
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

fn balance_helper(node: Rc<RefCell<RBNode>>) {
    match node.borrow().parent {
        None => node.borrow_mut().color = Color::Black,
        Some(_) => {
            if node.borrow().father_color().unwrap() == Color::Red {
                if node.borrow().uncle_color().unwrap() == Color::Red {
                    node.borrow().uncle().unwrap().borrow_mut().color = Color::Black;
                    // node.borrow().grand_father().unwrap().color = Color::Red;
                }
            }
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
        let new_node = RBNode::new(key, value, None);
        match &self.root_node {
            None => {
                self.root_node = Some(Rc::new(RefCell::new(new_node)));
            }
            Some(root_node) => {
                root_node
                    .borrow_mut()
                    .insert(new_node, Rc::clone(root_node));
            }
        }
        self.balance()
    }

    fn balance(&mut self) {
        match &self.root_node {
            Some(root_node) => balance_helper(Rc::clone(root_node)),
            None => {}
        }
    }

    pub fn rotate_left(&mut self, x: Rc<RefCell<RBNode>>) {
        /*
                 x                           y
               /   \                       /   \
              a     y         =>          x     c
                  /   \                 /  \
                 b     c               a    b
        */
        // let x = Rc::new(RefCell::new("a".to_owned()));
        // let x = node;
        match &x.borrow().right {
            Some(y) => {
                match y.borrow().left.as_ref() {
                    Some(y_left) => {
                        x.borrow_mut().right = Some(Rc::clone(y_left));
                        y_left.borrow_mut().parent = Some(Rc::clone(&x));
                    }
                    None => {
                        x.borrow_mut().right = None;
                    }
                }
                y.borrow_mut().parent = Some(Rc::clone(x.borrow().parent.as_ref().unwrap()));
                if x.borrow().parent.is_some() {
                    if x.borrow().key
                        == x.borrow()
                            .parent
                            .as_ref()
                            .unwrap()
                            .borrow()
                            .left
                            .as_ref()
                            .unwrap()
                            .borrow()
                            .key
                    {
                        x.borrow().parent.as_ref().unwrap().borrow_mut().left = Some(Rc::clone(y));
                    } else {
                        x.borrow().parent.as_ref().unwrap().borrow_mut().right = Some(Rc::clone(y));
                    }
                } else {
                    self.root_node = Some(Rc::clone(y));
                }
                y.borrow_mut().left = Some(Rc::clone(&x));
                x.borrow_mut().parent = Some(Rc::clone(y));
            }
            None => {}
        }
        // println!("{}",x.borrow().key);
        // let y = x.borrow().right.as_ref().unwrap();
        // x.borrow_mut().right = Some(Rc::clone(y));
        // x.borrow_mut().right = Some(Rc::clone(y.unwrap().borrow().left.as_ref().unwrap()));
    }

    pub fn print(&self) {
        let mut queue: Queue<(Rc<RefCell<RBNode>>, usize)> = queue![];
        // let root_node = Rc::clone(root_node);
        let mut current_level = 0;
        let mut num_nodes_in_level = 1;
        if let Some(root_node) = &self.root_node {
            queue.add((Rc::clone(root_node), current_level));
        }
        while queue.size() > 0 && current_level <= self.breath() {
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
                    if self.breath() >= current_level {
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
                }
                println!("");
                print!("");
                num_nodes_in_level = 1;
            }
            if self.breath() >= current_level {
                print!(
                    "{:indent$}{} ",
                    "",
                    node.borrow().key,
                    indent = self.breath() * 2 - current_level * 2 + 2
                );
            }

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
                (None, None) => {
                    queue.add((
                        Rc::new(RefCell::new(RBNode::new(
                            "".to_owned(),
                            "".to_owned(),
                            None,
                        ))),
                        current_level + 1,
                    ));
                    queue.add((
                        Rc::new(RefCell::new(RBNode::new(
                            "".to_owned(),
                            "".to_owned(),
                            None,
                        ))),
                        current_level + 1,
                    ));
                }
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
    // #[test]
    // fn insert_node() {
    //     let mut rb_tree = RBTree::new();
    //     rb_tree.insert("a".to_owned(), "value1".to_owned());

    //     rb_tree.insert("b".to_owned(), "value2".to_owned());

    //     let mut expected_rb_tree = RBTree::new();
    //     expected_rb_tree.insert("a".to_owned(), "value1".to_owned());
    //     expected_rb_tree.root_node.unwrap().borrow_mut().right = Some(Rc::new(RefCell::new(
    //         RBNode::new("b".to_owned(), "value2".to_owned(), None),
    //     )));

    //     rb_tree.print();
    // }
    #[test]
    fn left_rotate() {
        let mut rb_tree = RBTree::new();
        let x_node = Rc::new(RefCell::new(RBNode::new(
            "x".to_owned(),
            "val1".to_owned(),
            None,
        )));
        let a_node = RBNode::new("A".to_owned(), "val2".to_owned(), Some(Rc::clone(&x_node)));
        x_node.borrow_mut().left = Some(Rc::new(RefCell::new(a_node)));

        let y_node = Rc::new(RefCell::new(RBNode::new(
            "y".to_owned(),
            "asd".to_owned(),
            Some(Rc::clone(&x_node)),
        )));

        let b_node = Rc::new(RefCell::new(RBNode::new(
            "b".to_owned(),
            "asd".to_owned(),
            Some(Rc::clone(&y_node)),
        )));
        let c_node = Rc::new(RefCell::new(RBNode::new(
            "c".to_owned(),
            "asd".to_owned(),
            Some(Rc::clone(&y_node)),
        )));

        y_node.borrow_mut().left = Some(b_node);
        y_node.borrow_mut().right = Some(c_node);
        x_node.borrow_mut().right = Some(y_node);

        rb_tree.root_node = Some(x_node);

        rb_tree.print();
        rb_tree.rotate_left(Rc::clone(rb_tree.root_node.as_ref().unwrap()));
    }
}
