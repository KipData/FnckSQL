use itertools::Itertools;
use petgraph::data::DataMap;
use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use petgraph::visit::{Bfs, EdgeRef, IntoEdges};
use crate::optimizer::core::opt_expr::{OptExpr, OptExprNode, OptExprNodeId};
use crate::optimizer::heuristic::matcher::HepMatchOrder;
use crate::planner::LogicalPlan;
use crate::planner::operator::Operator;

/// HepNodeId is used in optimizer to identify a node.
pub type HepNodeId = NodeIndex<OptExprNodeId>;

#[derive(Debug, PartialEq)]
pub(crate) struct HepNode {
    node: OptExprNode,
    source_id: Option<HepNodeId>
}

#[derive(Debug)]
pub(crate) struct HepGraph {
    graph: StableDiGraph<HepNode, usize, usize>,
    root_index: HepNodeId,
}

impl HepGraph {
    pub(crate) fn new(root: LogicalPlan) -> Self {
        fn graph_filling(
            graph: &mut StableDiGraph<HepNode, usize, usize>,
            LogicalPlan{ operator, childrens }: LogicalPlan,
            source_id: Option<HepNodeId>
        ) -> HepNodeId {
            let index = graph.add_node(HepNode {
                node: OptExprNode::OperatorRef(operator),
                source_id,
            });
            for (order, child) in childrens.into_iter().enumerate() {
                let child_index = graph_filling(graph, child, Some(index));
                let _ = graph.add_edge(index, child_index, order);
            }

            index
        }

        let mut graph = StableDiGraph::<HepNode, usize, usize>::default();

        let root_index = graph_filling(
            &mut graph,
            root,
            None
        );

        HepGraph {
            graph,
            root_index,
        }
    }

    pub(crate) fn add_node(&mut self, source_id: HepNodeId, children_option: Option<HepNodeId>, new_node: OptExprNode) {
        let new_index = self.graph.add_node(HepNode {
            node: new_node,
            source_id: Some(source_id),
        });

        let mut order = self.graph
            .edges(source_id)
            .count();

        if let Some(children_id) = children_option {
            self.graph.find_edge(source_id, children_id)
                .map(|old_edge_id| {
                    order = self.graph
                        .remove_edge(old_edge_id)
                        .unwrap_or(0);

                    self.graph[children_id].source_id = Some(new_index);
                    self.graph.add_edge(new_index, children_id, 0);
                });
        }

        self.graph.add_edge(source_id, new_index, order);
    }

    pub(crate) fn replace_node(&mut self, source_id: HepNodeId, new_node: OptExprNode) {
        let node = &self.graph[source_id];

        self.graph[source_id] = HepNode {
            node: new_node,
            source_id: node.source_id,
        }
    }

    pub(crate) fn remove_node(&mut self, source_id: HepNodeId, with_childrens: bool) {
        let children_ids = self.graph.edges(source_id)
            .map(|edge_ref| edge_ref.target())
            .collect_vec();

        if let Some(source) = self.graph.remove_node(source_id) {
            if with_childrens { return; }
            if source.source_id.is_none() {
                assert!(children_ids.len() < 2);
                self.root_index = children_ids[0];

                return;
            }

            for (order, children_id) in children_ids.into_iter().enumerate() {
                if let Some(parent_id) = source.source_id {
                    let _ = self.graph.add_edge(parent_id, children_id, order);
                }
            }
        }
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

    /// Use bfs to traverse the graph and return node ids. If the node is a join, the children order
    /// is unstable. Maybe `left, right` or `right, left`.
    pub fn nodes_iter(&self, order: HepMatchOrder, start_option: Option<HepNodeId>) -> Box<dyn Iterator<Item = HepNodeId>> {
        let ids = self.bfs(start_option.unwrap_or(self.root_index));
        match order {
            HepMatchOrder::TopDown => Box::new(ids.into_iter()),
            HepMatchOrder::BottomUp => Box::new(ids.into_iter().rev()),
        }
    }

    pub(crate) fn node(&self, node_id: HepNodeId) -> Option<&HepNode> {
        self.graph.node_weight(node_id)
    }

    pub fn operator(&self, node_id: HepNodeId) -> &Operator {
        match &self.graph[node_id].node {
            OptExprNode::OperatorRef(op) => op,
            OptExprNode::OptExpr(node_id) => self.operator(HepNodeId::new(*node_id)),
        }
    }

    pub(crate) fn to_plan(&self) -> LogicalPlan {
        self.to_plan_with_index(self.root_index)
    }

    /// If input node is join, we use the edge weight to control the join chilren order.
    pub fn children_at(&self, id: HepNodeId) -> Vec<HepNodeId> {
        self
            .graph
            .edges(id)
            .sorted_by_key(|edge| edge.weight())
            .map(|edge| edge.target())
            .collect_vec()
    }

    pub fn to_opt_expr(&self, start: HepNodeId) -> OptExpr {
        let children = self
            .children_at(start)
            .iter()
            .map(|&id| self.to_opt_expr(id))
            .collect::<Vec<_>>();
        OptExpr::new(
            OptExprNode::OperatorRef(self.operator(start).clone()),
            children,
        )
    }

    pub(crate) fn to_plan_with_index(&self, start_index: HepNodeId) -> LogicalPlan {
        let mut root_plan = LogicalPlan {
            operator: self.operator(start_index).clone(),
            childrens: vec![],
        };

        self.build_childrens(&mut root_plan, start_index);

        root_plan
    }

    fn build_childrens(
        &self,
        plan: &mut LogicalPlan,
        start: HepNodeId,
    ) {
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
    use anyhow::Result;
    use petgraph::stable_graph::{EdgeIndex, IndexType, NodeIndex};
    use crate::binder::test::select_sql_run;
    use crate::optimizer::core::opt_expr::{OptExprNode, OptExprNodeId};
    use crate::optimizer::heuristic::graph::{HepGraph, HepNode, HepNodeId};
    use crate::planner::operator::Operator;

    #[test]
    fn test_graph() -> Result<()> {
        let plan = select_sql_run("select * from t1 left join t2 on c1 = c3")?;
        let mut graph = HepGraph::new(plan.clone());

        graph.add_node(
            HepNodeId::new(1),
            None,
            OptExprNode::OperatorRef(Operator::Dummy)
        );

        graph.add_node(
            HepNodeId::new(1),
            Some(HepNodeId::new(4)),
            OptExprNode::OperatorRef(Operator::Dummy)
        );

        graph.add_node(
            HepNodeId::new(5),
            None,
            OptExprNode::OperatorRef(Operator::Dummy)
        );

        graph.replace_node(HepNodeId::new(5), OptExprNode::OptExpr(OptExprNodeId::new(5)));

        assert!(graph.graph.contains_edge(NodeIndex::new(1), NodeIndex::new(2)));
        assert!(graph.graph.contains_edge(NodeIndex::new(1), NodeIndex::new(3)));
        assert!(graph.graph.contains_edge(NodeIndex::new(0), NodeIndex::new(1)));
        assert!(graph.graph.contains_edge(NodeIndex::new(5), NodeIndex::new(4)));
        assert!(graph.graph.contains_edge(NodeIndex::new(1), NodeIndex::new(5)));
        assert!(graph.graph.contains_edge(NodeIndex::new(5), NodeIndex::new(6)));
        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(0)), Some(&0));
        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(1)), Some(&1));
        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(2)), Some(&0));
        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(3)), Some(&0));
        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(4)), Some(&2));
        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(5)), Some(&1));
        assert_eq!(
            graph.graph.node_weight(NodeIndex::new(5)),
            Some(&HepNode { node: OptExprNode::OptExpr(5), source_id: Some(NodeIndex::new(1)) })
        );
        assert_eq!(graph.root_index, NodeIndex::new(0));

        graph.remove_node(
            HepNodeId::new(0),
            false
        );

        graph.remove_node(
            HepNodeId::new(5),
            true
        );

        assert!(graph.graph.contains_edge(NodeIndex::new(1), NodeIndex::new(2)));
        assert!(graph.graph.contains_edge(NodeIndex::new(1), NodeIndex::new(3)));
        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(0)), Some(&0));
        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(1)), Some(&1));
        assert_eq!(graph.root_index, NodeIndex::new(1));

        let final_plan = graph.to_plan();

        match final_plan.operator {
            Operator::Join(_) => (),
            _ => unreachable!("Should be a join operator"),
        }

        assert_eq!(final_plan.childrens.len(), 2);

        let part_plan = graph.to_plan_with_index(HepNodeId::new(3));

        match part_plan.operator {
            Operator::Scan(_) => (),
            _ => unreachable!("Should be a scan operator"),
        }

        assert_eq!(part_plan.childrens.len(), 0);

        let root_expr = graph.to_opt_expr(HepNodeId::new(1));

        match root_expr.root.get_operator() {
            Operator::Join(_) => (),
            _ => unreachable!("Should be a join operator"),
        }

        assert_eq!(root_expr.childrens.len(), 2);

        let part_expr = graph.to_opt_expr(HepNodeId::new(3));

        match part_expr.root.get_operator() {
            Operator::Scan(_) => (),
            _ => unreachable!("Should be a scan operator"),
        }

        assert_eq!(part_expr.childrens.len(), 0);

        Ok(())
    }
}