use std::collections::HashMap;
use std::io::stdin;
use std::io::{Error, ErrorKind};

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
    let command = iter.next();
    let key = iter.next();
    let value = iter.next();

    match command {
        Some("set") => match (key, value) {
            (Some(key), Some(value)) => Ok(InputCommand {
                command: InputCommandType::Set,
                key: key.to_string(),
                value: value.to_string(),
            }),
            (_, _) => Err(Error::new(ErrorKind::InvalidInput, "Could not parse input")),
        },
        Some("get") => match key {
            Some(key) => Ok(InputCommand {
                command: InputCommandType::Get,
                key: key.to_string(),
                value: value.unwrap_or_default().to_string(),
            }),
            None => Err(Error::new(ErrorKind::InvalidInput, "Could not parse input")),
        },
        _ => Err(Error::new(
            ErrorKind::InvalidInput,
            format!(
                "Could not parse command. Expected 'set' or 'get' but got '{0}'",
                command.unwrap_or_default()
            ),
        )),
    }
}

fn main() {
    let mut key_value_store: HashMap<String, String> = HashMap::new();
    loop {
        let input_command = get_input();
        let input_command = match input_command {
            Ok(v) => v,
            Err(error) => {
                println!("{}\n", error);
                continue;
            }
        };

        match input_command.command {
            InputCommandType::Set => {
                key_value_store.insert(input_command.key, input_command.value);
            }
            InputCommandType::Get => match key_value_store.get(&input_command.key) {
                Some(value) => println!("key:{}  value:{}\n", input_command.key, value),
                None => println!("could not find key:{}\n", input_command.key),
            },
        };
    }
}
