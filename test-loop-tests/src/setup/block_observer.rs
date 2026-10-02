use crate::setup::state::NodeExecutionData;
use crate::utils::node::TestLoopNode;
use itertools::Itertools;
use near_async::test_loop::data::TestLoopData;
use near_primitives::types::BlockHeight;
use std::ops::ControlFlow;

/// Node whose head height decides when a `BlockObserver` is called.
pub enum BlockSource {
    /// The first node with the lowest head height.
    SlowestNode,
    /// The node at this index in `TestLoopEnv::node_datas`.
    Node(usize),
}

impl BlockSource {
    fn select<'n, 'a>(&self, nodes: &'n [TestLoopNode<'a>]) -> &'n TestLoopNode<'a> {
        match self {
            BlockSource::SlowestNode => nodes.iter().min_by_key(|node| node.head().height).unwrap(),
            BlockSource::Node(index) => &nodes[*index],
        }
    }
}

/// The new block a `BlockObserver` is called for.
pub struct ObservedBlock<'a> {
    /// The node selected by the observer's `BlockSource`.
    pub observed_node: &'a TestLoopNode<'a>,
    /// All nodes, in `TestLoopEnv::node_datas` order.
    pub nodes: &'a [TestLoopNode<'a>],
}

impl ObservedBlock<'_> {
    /// Head height of the observed node.
    pub fn height(&self) -> BlockHeight {
        self.observed_node.head().height
    }
}

/// Code that runs once per new head height of its `BlockSource`, while a `NodeRunner` runs the
/// test loop. Returning `Break` removes the observer.
pub type BlockObserver = Box<dyn FnMut(&ObservedBlock<'_>) -> ControlFlow<()>>;

struct BlockObserverEntry {
    source: BlockSource,
    last_height: Option<BlockHeight>,
    observer: BlockObserver,
}

/// Observers of a `TestLoopEnv`, called in registration order.
#[derive(Default)]
pub(crate) struct BlockObservers {
    entries: Vec<BlockObserverEntry>,
}

impl BlockObservers {
    pub(crate) fn add(&mut self, source: BlockSource, observer: BlockObserver) {
        self.entries.push(BlockObserverEntry { source, last_height: None, observer });
    }

    /// Calls each observer whose source node's head height differs from the height at its
    /// previous call. Removes observers that return `Break`.
    pub(crate) fn call_on_new_blocks(
        &mut self,
        data: &TestLoopData,
        node_datas: &[NodeExecutionData],
    ) {
        if self.entries.is_empty() {
            return;
        }
        let nodes =
            node_datas.iter().map(|node_data| TestLoopNode { data, node_data }).collect_vec();
        self.entries.retain_mut(|entry| {
            let observed_node = entry.source.select(&nodes);
            let height = observed_node.head().height;
            if entry.last_height == Some(height) {
                return true;
            }
            entry.last_height = Some(height);
            (entry.observer)(&ObservedBlock { observed_node, nodes: &nodes }).is_continue()
        });
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
    }
}
