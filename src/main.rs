#![allow(dead_code)]
// use rust_kv_store::TreeNode;
use rust_kv_store::RBNode;
use rust_kv_store::RBTree;
use std::cell::RefCell;
use std::cmp;
use std::collections::HashMap;
use std::io::stdin;
use std::io::{Error, ErrorKind};
use std::rc::Rc;

#[derive(Copy, Clone)]
enum InputCommandType {
    Get,
    Set,
}

struct InputCommand {
    command: InputCommandType,
    key: String,
    value: String,
}
fn get_input() -> Result<InputCommand, Error> {
    let mut input_key = String::new();
    stdin().read_line(&mut input_key)?;
    let input_key = input_key.to_lowercase();
    let mut iter = input_key.split_whitespace();
    let command = match iter.next() {
        Some("set") => InputCommandType::Set,
        Some("get") => InputCommandType::Get,
        other => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "Could not parse command. Expected 'set' or 'get' but got '{0}'",
                    other.unwrap_or_default()
                ),
            ))
        }
    };

    let key = match iter.next() {
        Some(v) => v.to_string(),
        None => return Err(Error::new(ErrorKind::InvalidInput, "Could not parse input")),
    };
    let value = match (command, iter.next()) {
        (InputCommandType::Set, Some(v)) => v.to_string(),
        (InputCommandType::Get, other) => other.unwrap_or_default().to_string(),
        (_, _) => return Err(Error::new(ErrorKind::InvalidInput, "Could not parse input")),
    };

    Ok(InputCommand {
        command,
        key,
        value,
    })
}

fn main() {
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

    rb_tree.print();
    // let mut key_value_store: HashMap<String, String> = HashMap::new();
    let mut rb_tree = RBTree::new();
    rb_tree.insert("a".to_owned(), "value_1".to_owned());
    // // print!("{}",rb_tree.root_node.deref());
    rb_tree.insert(String::from("g"), "value_g".to_owned());
    rb_tree.insert(String::from("h"), "value_h".to_owned());
    rb_tree.insert(String::from("i"), "value_i".to_owned());
    rb_tree.insert(String::from("j"), "value_j".to_owned());
    rb_tree.insert(String::from("k"), "value_k".to_owned());
    rb_tree.insert(String::from("b"), "value_b".to_owned());
    rb_tree.insert(String::from("c"), "value_c".to_owned());
    rb_tree.insert(String::from("d"), "value_d".to_owned());
    rb_tree.insert(String::from("e"), "value_e".to_owned());
    rb_tree.insert(String::from("f"), "value_f".to_owned());
    rb_tree.insert(String::from("c"), "value_f".to_owned());

    rb_tree.print();
    let value = rb_tree.get("d".to_owned());
    println!("{}", value.unwrap())
    // loop {
    //     let input_command = match get_input() {
    //         Ok(v) => v,
    //         Err(error) => {
    //             println!("{}\n", error);
    //             continue;
    //         }
    //     };

    //     match input_command.command {
    //         InputCommandType::Set => {
    //             key_value_store.insert(input_command.key, input_command.value);
    //         }
    //         InputCommandType::Get => match key_value_store.get(&input_command.key) {
    //             Some(value) => {
    //                 println!(
    //                     "\n{{\n  key:{}  \n  value:{}\n}}\n",
    //                     input_command.key, value
    //                 )
    //             }
    //             None => println!("could not find key:{}\n", input_command.key),
    //         },
    //     };
    // }
}
