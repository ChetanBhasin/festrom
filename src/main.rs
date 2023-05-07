use std::{
    collections::{HashMap, HashSet},
    io::{StdoutLock, Write},
};

use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum Payload {
    // Maelstrom init payloads
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,

    // Echo (probably mainly used for heartbeat)
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },

    // Used for generating unique IDs
    Generate,
    GenerateOk {
        id: String,
    },

    // Used for topology management
    Topology {
        topology: HashMap<String, HashSet<String>>,
    },
    TopologyOk,

    // Used for broadcasting messages
    Broadcast {
        message: usize,
    },
    BroadcastOk,

    // Used for reading messages
    Read,
    ReadOk {
        messages: HashSet<usize>,
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
    node_id: Option<String>,
    topology: HashMap<String, HashSet<String>>,
    messages: HashSet<usize>,
    out: StdoutLock<'a>,
}

impl Node<'_> {
    fn update_node_id(&mut self, node_id: String) {
        self.node_id = Some(node_id);
    }

    fn recv(&mut self, msg: Message) {
        use Payload::*;

        match &msg.body.message {
            Init { node_id, .. } => {
                self.update_node_id(node_id.to_owned());
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
            Generate => {
                let id = Ulid::new().to_string();
                let node = self
                    .node_id
                    .clone()
                    .unwrap_or_else(|| panic!("Uninitizlied node got a generate message"));
                let id = format!("{}-{}", node, id);
                msg.send(GenerateOk { id }, &mut self.out);
            }
            Topology { topology } => {
                self.topology = topology.to_owned();
                msg.send(TopologyOk, &mut self.out);
            }
            Broadcast { message } => {
                self.messages.extend(vec![*message]);
                msg.send(BroadcastOk, &mut self.out);
            }
            Read => {
                msg.send(
                    ReadOk {
                        messages: self.messages.clone(),
                    },
                    &mut self.out,
                );
            }

            InitOk
            | EchoOk { .. }
            | GenerateOk { .. }
            | TopologyOk
            | BroadcastOk
            | ReadOk { .. } => {}
        }
    }
}

fn main() {
    let stdin = std::io::stdin();
    let out = std::io::stdout().lock();
    let mut node = Node {
        out,
        node_id: None,
        topology: HashMap::new(),
        messages: HashSet::new(),
    };
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
