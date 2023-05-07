use std::io::{StdoutLock, Write};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct MessageBody {
    msg_id: usize,
    in_reply_to: Option<usize>,
    #[serde(rename = "type")]
    #[serde(flatten)]
    message: Payload,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct Message {
    src: String,
    dest: String,
    body: MessageBody,
}

impl Message {
    pub fn send(&self, payload: Payload, out: &mut StdoutLock) {
        let output = Message {
            src: self.dest.clone(),
            dest: self.src.clone(),
            body: MessageBody {
                msg_id: &self.body.msg_id + 1,
                in_reply_to: Some(self.body.msg_id),
                message: payload,
            },
        };
        writeln!(
            out,
            "{}",
            serde_json::to_string(&output)
                .expect("Failed to convert message (to be sent) to string.")
        )
        .expect("Failed to write to stdout");
        out.flush().expect("Failed to flush the stdout");
    }
}

struct Node<'a> {
    pub out: StdoutLock<'a>,
}

impl Node<'_> {
    fn recv(&mut self, msg: Message) {
        use Payload::*;

        match &msg.body.message {
            Init { .. } => {
                msg.send(Payload::InitOk, &mut self.out);
            }
            Echo { echo } => {
                msg.send(
                    EchoOk {
                        echo: echo.to_owned(),
                    },
                    &mut self.out,
                );
            }
            _ => {}
        }
    }
}

fn main() {
    let stdin = std::io::stdin();
    let out = std::io::stdout().lock();
    let mut node = Node { out };
    for line in stdin.lines() {
        let line = line.expect("Unable to read the line from standard input");
        // We are just using the `.expect` function here because this is for testing with
        // Malestrong and we'd rather fail and crash than properly handle this issue and log it
        let message =
            serde_json::from_str::<Message>(&line).expect("Unable to parse message from the line");
        node.recv(message);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_echo_serde() {
        use serde_json::*;
        let message = Message {
            src: "n1".to_string(),
            dest: "c1".to_string(),
            body: MessageBody {
                msg_id: 1,
                in_reply_to: Some(usize::MIN + 1),
                message: Payload::EchoOk {
                    echo: "Please echo 35".to_string(),
                },
            },
        };
        let expected: Message = from_value(json! ({
          "src": "n1",
          "dest": "c1",
          "body": {
            "type": "echo_ok",
            "msg_id": 1,
            "in_reply_to": 1,
            "echo": "Please echo 35"
          }
        }))
        .expect("Failed to parse the expected message");
        assert_eq!(message, expected, "Failed to to match the serialied values");
        assert_eq!(
            to_string(&message).expect("Could not deserialize the message"),
            to_string(&expected).expect("Could not deserialize the expected message"),
            "Failed to match the deserialied values"
        );
    }
}
