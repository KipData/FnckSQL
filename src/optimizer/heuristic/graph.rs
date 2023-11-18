use crate::optimizer::core::opt_expr::OptExprNodeId;
use crate::optimizer::heuristic::batch::HepMatchOrder;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use itertools::Itertools;
use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use petgraph::visit::{Bfs, EdgeRef};
use std::mem;

/// HepNodeId is used in optimizer to identify a node.
pub type HepNodeId = NodeIndex<OptExprNodeId>;

#[derive(Debug)]
pub struct HepGraph {
    graph: StableDiGraph<Operator, usize, usize>,
    root_index: HepNodeId,
    pub version: usize,
}

impl HepGraph {
    pub fn new(root: LogicalPlan) -> Self {
        fn graph_filling(
            graph: &mut StableDiGraph<Operator, usize, usize>,
            LogicalPlan {
                operator,
                childrens,
            }: LogicalPlan,
        ) -> HepNodeId {
            let index = graph.add_node(operator);

            for (order, child) in childrens.into_iter().enumerate() {
                let child_index = graph_filling(graph, child);
                let _ = graph.add_edge(index, child_index, order);
            }

            index
        }

        let mut graph = StableDiGraph::<Operator, usize, usize>::default();

        let root_index = graph_filling(&mut graph, root);

        HepGraph {
            graph,
            root_index,
            version: 0,
        }
    }

    pub fn parent_id(&self, node_id: HepNodeId) -> Option<HepNodeId> {
        self.graph
            .neighbors_directed(node_id, petgraph::Direction::Incoming)
            .next()
    }

    #[allow(dead_code)]
    pub fn add_root(&mut self, new_node: Operator) {
        let old_root_id = mem::replace(&mut self.root_index, self.graph.add_node(new_node));

        self.graph.add_edge(self.root_index, old_root_id, 0);
        self.version += 1;
    }

    pub fn add_node(
        &mut self,
        source_id: HepNodeId,
        children_option: Option<HepNodeId>,
        new_node: Operator,
    ) {
        let new_index = self.graph.add_node(new_node);
        let mut order = self.graph.edges(source_id).count();

        if let Some((children_id, old_edge_id)) = children_option.and_then(|children_id| {
            self.graph
                .find_edge(source_id, children_id)
                .map(|old_edge_id| (children_id, old_edge_id))
        }) {
            order = self.graph.remove_edge(old_edge_id).unwrap_or(0);

            self.graph.add_edge(new_index, children_id, 0);
        }

        self.graph.add_edge(source_id, new_index, order);
        self.version += 1;
    }

    pub fn replace_node(&mut self, source_id: HepNodeId, new_node: Operator) {
        self.graph[source_id] = new_node;
        self.version += 1;
    }

    pub fn swap_node(&mut self, a: HepNodeId, b: HepNodeId) {
        let tmp = self.graph[a].clone();

        self.graph[a] = mem::replace(&mut self.graph[b], tmp);
        self.version += 1;
    }

    pub fn remove_node(&mut self, source_id: HepNodeId, with_childrens: bool) -> Option<Operator> {
        if !with_childrens {
            let children_ids = self
                .graph
                .edges(source_id)
                .sorted_by_key(|edge_ref| edge_ref.weight())
                .map(|edge_ref| edge_ref.target())
                .collect_vec();

            if let Some(parent_id) = self.parent_id(source_id) {
                if let Some(edge) = self.graph.find_edge(parent_id, source_id) {
                    let weight = *self.graph.edge_weight(edge).unwrap_or(&0);

                    for (order, children_id) in children_ids.into_iter().enumerate() {
                        let _ = self.graph.add_edge(parent_id, children_id, weight + order);
                    }
                }
            } else {
                assert!(children_ids.len() < 2);
                self.root_index = children_ids[0];
            }
        }

        self.version += 1;
        self.graph.remove_node(source_id)
    }

    /// Traverse the graph in BFS order.
    fn bfs(&self, start: HepNodeId) -> Vec<HepNodeId> {
        let mut ids = Vec::with_capacity(self.graph.node_count());
        let mut iter = Bfs::new(&self.graph, start);
        while let Some(node_id) = iter.next(&self.graph) {
            ids.push(node_id);
        }
        ids
    }

    /// Use bfs to traverse the graph and return node ids
    pub fn nodes_iter(
        &self,
        order: HepMatchOrder,
        start_option: Option<HepNodeId>,
    ) -> Box<dyn Iterator<Item = HepNodeId>> {
        let ids = self.bfs(start_option.unwrap_or(self.root_index));
        match order {
            HepMatchOrder::TopDown => Box::new(ids.into_iter()),
            HepMatchOrder::BottomUp => Box::new(ids.into_iter().rev()),
        }
    }

    #[allow(dead_code)]
    pub fn node(&self, node_id: HepNodeId) -> Option<&Operator> {
        self.graph.node_weight(node_id)
    }

    pub fn operator(&self, node_id: HepNodeId) -> &Operator {
        &self.graph[node_id]
    }

    pub fn operator_mut(&mut self, node_id: HepNodeId) -> &mut Operator {
        &mut self.graph[node_id]
    }

    pub fn to_plan(&self) -> LogicalPlan {
        self.to_plan_with_index(self.root_index)
    }

    /// If input node is join, we use the edge weight to control the join chilren order.
    pub fn children_at(&self, id: HepNodeId) -> Vec<HepNodeId> {
        self.graph
            .edges(id)
            .sorted_by_key(|edge| edge.weight())
            .map(|edge| edge.target())
            .collect_vec()
    }

    pub fn to_plan_with_index(&self, start_index: HepNodeId) -> LogicalPlan {
        let mut root_plan = LogicalPlan {
            operator: self.operator(start_index).clone(),
            childrens: vec![],
        };

        self.build_childrens(&mut root_plan, start_index);

        root_plan
    }

    fn build_childrens(&self, plan: &mut LogicalPlan, start: HepNodeId) {
        for child_id in self.children_at(start) {
            let mut child_plan = LogicalPlan {
                operator: self.operator(child_id).clone(),
                childrens: vec![],
            };

            self.build_childrens(&mut child_plan, child_id);
            plan.childrens.push(child_plan);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::test::select_sql_run;
    use crate::execution::ExecutorError;
    use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
    use crate::planner::operator::Operator;
    use petgraph::stable_graph::{EdgeIndex, NodeIndex};

    #[tokio::test]
    async fn test_graph_for_plan() -> Result<(), ExecutorError> {
        let plan = select_sql_run("select * from t1 left join t2 on c1 = c3").await?;
        let graph = HepGraph::new(plan);

        assert!(graph
            .graph
            .contains_edge(NodeIndex::new(1), NodeIndex::new(2)));
        assert!(graph
            .graph
            .contains_edge(NodeIndex::new(1), NodeIndex::new(3)));
        assert!(graph
            .graph
            .contains_edge(NodeIndex::new(0), NodeIndex::new(1)));

        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(0)), Some(&0));
        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(1)), Some(&1));
        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(2)), Some(&0));

        Ok(())
    }

    #[tokio::test]
    async fn test_graph_add_node() -> Result<(), ExecutorError> {
        let plan = select_sql_run("select * from t1 left join t2 on c1 = c3").await?;
        let mut graph = HepGraph::new(plan);

        graph.add_node(HepNodeId::new(1), None, Operator::Dummy);

        graph.add_node(HepNodeId::new(1), Some(HepNodeId::new(4)), Operator::Dummy);

        graph.add_node(HepNodeId::new(5), None, Operator::Dummy);

        assert!(graph
            .graph
            .contains_edge(NodeIndex::new(5), NodeIndex::new(4)));
        assert!(graph
            .graph
            .contains_edge(NodeIndex::new(1), NodeIndex::new(5)));
        assert!(graph
            .graph
            .contains_edge(NodeIndex::new(5), NodeIndex::new(6)));

        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(3)), Some(&0));
        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(4)), Some(&2));
        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(5)), Some(&1));

        Ok(())
    }

    #[tokio::test]
    async fn test_graph_replace_node() -> Result<(), ExecutorError> {
        let plan = select_sql_run("select * from t1 left join t2 on c1 = c3").await?;
        let mut graph = HepGraph::new(plan);

        graph.replace_node(HepNodeId::new(1), Operator::Dummy);

        assert!(matches!(graph.operator(HepNodeId::new(1)), Operator::Dummy));

        Ok(())
    }

    #[tokio::test]
    async fn test_graph_remove_middle_node_by_single() -> Result<(), ExecutorError> {
        let plan = select_sql_run("select * from t1 left join t2 on c1 = c3").await?;
        let mut graph = HepGraph::new(plan);

        graph.remove_node(HepNodeId::new(1), false);

        assert_eq!(graph.graph.edge_count(), 2);

        assert!(graph
            .graph
            .contains_edge(NodeIndex::new(0), NodeIndex::new(2)));
        assert!(graph
            .graph
            .contains_edge(NodeIndex::new(0), NodeIndex::new(3)));

        Ok(())
    }

    #[tokio::test]
    async fn test_graph_remove_middle_node_with_childrens() -> Result<(), ExecutorError> {
        let plan = select_sql_run("select * from t1 left join t2 on c1 = c3").await?;
        let mut graph = HepGraph::new(plan);

        graph.remove_node(HepNodeId::new(1), true);

        assert_eq!(graph.graph.edge_count(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_graph_swap_node() -> Result<(), ExecutorError> {
        let plan = select_sql_run("select * from t1 left join t2 on c1 = c3").await?;
        let mut graph = HepGraph::new(plan);

        let before_op_0 = graph.operator(HepNodeId::new(0)).clone();
        let before_op_1 = graph.operator(HepNodeId::new(1)).clone();

        graph.swap_node(HepNodeId::new(0), HepNodeId::new(1));

        let op_0 = graph.operator(HepNodeId::new(0));
        let op_1 = graph.operator(HepNodeId::new(1));

        assert_eq!(op_0, &before_op_1);
        assert_eq!(op_1, &before_op_0);

        Ok(())
    }

    #[tokio::test]
    async fn test_graph_add_root() -> Result<(), ExecutorError> {
        let plan = select_sql_run("select * from t1 left join t2 on c1 = c3").await?;
        let mut graph = HepGraph::new(plan);

        graph.add_root(Operator::Dummy);

        assert_eq!(graph.graph.edge_count(), 4);
        assert!(graph
            .graph
            .contains_edge(NodeIndex::new(4), NodeIndex::new(0)));
        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(3)), Some(&0));

        Ok(())
    }

    #[tokio::test]
    async fn test_graph_to_plan() -> Result<(), ExecutorError> {
        let plan = select_sql_run("select * from t1 left join t2 on c1 = c3").await?;
        let graph = HepGraph::new(plan.clone());

        let plan_for_graph = graph.to_plan();

        assert_eq!(plan, plan_for_graph);

        let plan_by_index = graph.to_plan_with_index(HepNodeId::new(1));

        assert_eq!(plan.childrens[0], plan_by_index);

        Ok(())
    }
}
