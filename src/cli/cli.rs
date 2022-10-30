use std::io::stdin;
use std::io::{Error, ErrorKind};

#[derive(Copy, Clone)]
enum InputCommandType {
    Get,
    Set,
}

pub struct InputCommand {
    command: InputCommandType,
    key: String,
    value: String,
}
pub fn get_input() -> Result<InputCommand, Error> {
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
