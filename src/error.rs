use std::fmt;

pub type NodeError = Box<dyn std::error::Error + Send + Sync>;

pub type NodeResult = Result<(), NodeError>;

#[derive(Debug)]
pub enum Error {
    Config(&'static str),
    DurationExceeded {
        elapsed: std::time::Duration,
        limit: std::time::Duration,
    },
    NodePanicked {
        node: String,
        reason: String,
    },
    NodeReturned {
        node: String,
        source: NodeError,
    },
    NoProgress {
        steps: u64,
        limit: u64,
    },
    UnknownNode {
        name: String,
    },
    Unreachable {
        from: String,
        to: String,
    },
    DuplicateNode {
        name: String,
    },
    Io(String),
    Join(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(msg) => write!(f, "config error: {msg}"),
            Self::DurationExceeded { elapsed, limit } => {
                write!(f, "duration exceeded: {elapsed:?} > {limit:?}")
            }
            Self::NodePanicked { node, reason } => {
                write!(f, "node '{node}' panicked: {reason}")
            }
            Self::NodeReturned { node, source } => {
                write!(f, "node '{node}' returned error: {source}")
            }
            Self::NoProgress { steps, limit } => {
                write!(f, "no progress for {steps} steps (limit: {limit})")
            }
            Self::UnknownNode { name } => write!(f, "unknown node: '{name}'"),
            Self::Unreachable { from, to } => {
                write!(f, "unreachable: '{from}' -> '{to}'")
            }
            Self::DuplicateNode { name } => {
                write!(f, "duplicate node: '{name}'")
            }
            Self::Io(msg) => write!(f, "I/O error: {msg}"),
            Self::Join(msg) => write!(f, "join error: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NodeReturned { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}
