use std::{
    collections::{HashMap, HashSet},
    io::{StdoutLock, Write},
};

use rand::prelude::IteratorRandom;

use tokio::io::{self, AsyncBufReadExt, BufReader};
use tokio::time::{self, Duration};
use tokio_stream::wrappers::{IntervalStream, LinesStream};
use tokio_stream::StreamExt;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Standard IO Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Serde JSON error: {0}")]
    SerdeError(#[from] serde_json::Error),
}

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

    // Used for Gossiping with other nodes
    Gossip {
        has_seen: HashSet<usize>,
    },
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct MessageBody {
    msg_id: Option<usize>,
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
    pub fn send_message(src: String, dest: String, message: MessageBody, out: &mut StdoutLock) {
        let output = Message {
            src,
            dest,
            body: message,
        };
        eprintln!("Writing a message: {:?}", output);
        writeln!(
            out,
            "{}",
            serde_json::to_string(&output)
                .expect("Failed to convert message (to be sent) to string.")
        )
        .expect("Failed to write to stdout");
        out.flush().expect("Failed to flush the stdout");
    }

    pub fn send(&self, payload: Payload, out: &mut StdoutLock) {
        Self::send_message(
            self.dest.clone(),
            self.src.clone(),
            MessageBody {
                msg_id: self.body.msg_id.map(|id| id + 1),
                in_reply_to: self.body.msg_id,
                message: payload,
            },
            out,
        );
    }
}

struct Node<'a> {
    node_id: Option<String>,
    topology: HashMap<String, HashSet<String>>,
    messages: HashSet<usize>,
    out: StdoutLock<'a>,
    node_has_seen: HashMap<String, HashSet<usize>>,
}

impl Node<'_> {
    fn find_gossip_messages(&self, has_seen: HashSet<usize>) -> HashSet<usize> {
        let (seen, mut unseen): (HashSet<_>, HashSet<_>) =
            self.messages.iter().partition(|msg| has_seen.contains(msg));
        // I'd only like to send 20% extra gossip - I haven't done much tuning on this though
        let random_select = (seen.len() as f32 * 0.2) as usize;
        let mut rng = rand::thread_rng();
        unseen.extend(seen.iter().choose_multiple(&mut rng, random_select));
        return unseen;
    }

    pub fn gossip(&mut self) {
        if let Some(node_id) = &self.node_id {
            if let Some(nodes) = self.topology.get(node_id) {
                for node in nodes {
                    let messages = self
                        .node_has_seen
                        .get(node)
                        .map(|has_seen| self.find_gossip_messages(has_seen.clone()))
                        .unwrap_or(self.messages.clone());
                    if messages.len() == 0 {
                        // There is no point sending empty messages
                        return;
                    }
                    Message::send_message(
                        node_id.clone(),
                        node.clone(),
                        MessageBody {
                            msg_id: None,
                            in_reply_to: None,
                            message: Payload::Gossip {
                                has_seen: messages.clone(),
                            },
                        },
                        &mut self.out,
                    );
                }
            }
        }
    }

    pub fn recv(&mut self, msg: Message) {
        use Payload::*;

        match &msg.body.message {
            Init { node_id, .. } => {
                self.node_id = Some(node_id.to_owned());
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
            Gossip { has_seen } => {
                self.messages.extend(has_seen);
                self.node_has_seen.insert(msg.src.clone(), has_seen.clone());
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

pub enum Event {
    TransportMessage(Message),
    Gossip,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let out = std::io::stdout().lock();
    let mut node = Node {
        out,
        node_id: None,
        topology: HashMap::new(),
        messages: HashSet::new(),
        node_has_seen: HashMap::new(),
    };

    let stdin = io::stdin();
    let reader = BufReader::new(stdin);
    let messages = LinesStream::new(reader.lines()).map(|line| {
        Ok::<Event, Error>(Event::TransportMessage(
            serde_json::from_str::<Message>(&line.expect("Failed to read the line"))
                .expect("Failed to read the message"),
        ))
    });
    let gossip_interval = IntervalStream::new(time::interval(Duration::from_millis(300)))
        .map(|_| Ok::<Event, Error>(Event::Gossip));

    let mut events = messages.merge(gossip_interval);

    //
    while let Some(Ok(action)) = events.next().await {
        match action {
            Event::TransportMessage(msg) => {
                eprintln!("Got a message: {:?}", msg);
                node.recv(msg);
            }
            Event::Gossip => node.gossip(),
        }
    }

    Ok(())
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
                msg_id: Some(1),
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
