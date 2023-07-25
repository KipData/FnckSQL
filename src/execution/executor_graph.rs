use std::{collections::HashMap, sync::Arc};

use petgraph::{
    stable_graph::{NodeIndex, StableGraph},
    visit::EdgeRef,
    Direction,
};

use super::{
    executor::ExecutionQueue,
    parallel::{
        meta_pipeline::MetaPipeline,
        pipeline_event::{PipelineEvent, PipelineEventStack},
    },
};
use anyhow::Result;

pub struct ExecutorGraph {
    graph: StableGraph<Arc<PipelineEvent>, ()>,
}

impl ExecutorGraph {
    pub fn from_pipeline(mut meta_pipeline: MetaPipeline) -> Result<ExecutorGraph> {
        let mut graph: StableGraph<Arc<PipelineEvent>, ()> = StableGraph::new();
        Self::init_graph(&mut meta_pipeline, &mut graph)?;
        Ok(ExecutorGraph { graph })
    }

    fn init_graph(
        meta_pipeline: &mut MetaPipeline,
        graph: &mut StableGraph<Arc<PipelineEvent>, ()>,
    ) -> Result<()> {
        // Get the all pipelines.
        let all_pipelines = meta_pipeline.get_pipelines(true)?;

        let to_schedue = meta_pipeline.get_meta_pipelines(true, true)?;

        // Value -> initialize_event_id, running_event_id, finish_event_id,
        // complete_event_id.
        let mut pipeline_graph_mapping: HashMap<usize, PipelineEventStack> = HashMap::new();

        for meta_pipe in to_schedue {
            // let mut pipes_edges: Vec<Vec<Edge>> = Vec::new();
            let base_pipeline = Arc::new(meta_pipe.get_base_pipeline()?);
            let base_initialize_event = Arc::new(PipelineEvent::create_initialize_event(
                base_pipeline.clone(),
            ));
            let base_running_event =
                Arc::new(PipelineEvent::create_running_event(base_pipeline.clone()));
            let base_finish_event =
                Arc::new(PipelineEvent::create_finish_event(base_pipeline.clone()));
            let base_complete_event = Arc::new(PipelineEvent::create_complete_event(
                true,
                base_pipeline.clone(),
            ));

            let base_init_event_node_id = graph.add_node(base_initialize_event);
            let base_running_event_node_id = graph.add_node(base_running_event);
            let base_finish_event_node_id = graph.add_node(base_finish_event);
            let base_complete_event_node_id = graph.add_node(base_complete_event);

            // Add base stack.
            pipeline_graph_mapping.insert(
                base_pipeline.get_pipeline_id(),
                PipelineEventStack {
                    pipeline_initialize_event: base_init_event_node_id,
                    pipeline_event: base_running_event_node_id,
                    pipeline_finish_event: base_finish_event_node_id,
                    pipeline_complete_event: base_complete_event_node_id,
                },
            );

            // Dependencies: initialize -> running -> finish -> complete.
            graph.add_edge(base_init_event_node_id, base_running_event_node_id, ());
            graph.add_edge(base_running_event_node_id, base_finish_event_node_id, ());
            graph.add_edge(base_finish_event_node_id, base_complete_event_node_id, ());

            let pipelines = meta_pipe.get_pipelines(false)?;
            for idx in 1..pipelines.len() {
                let pipeline = &pipelines[idx];

                let pipeline_running_event = Arc::new(PipelineEvent::create_running_event(
                    Arc::from(pipeline.clone()),
                ));

                let running_event_node_id = graph.add_node(pipeline_running_event);

                match meta_pipe.get_finish_group(pipeline.get_pipeline_id()) {
                    Some(finish_group) => {
                        let mapping_stack = pipeline_graph_mapping.get(finish_group).unwrap();
                        let stack = PipelineEventStack {
                            pipeline_initialize_event: base_init_event_node_id,
                            pipeline_event: base_running_event_node_id,
                            pipeline_finish_event: mapping_stack.pipeline_finish_event,
                            pipeline_complete_event: base_complete_event_node_id,
                        };

                        // Dependencies: base_finish -> pipeline_event -> group_finish
                        graph.add_edge(base_running_event_node_id, running_event_node_id, ());
                        graph.add_edge(
                            running_event_node_id,
                            mapping_stack.pipeline_finish_event,
                            (),
                        );

                        pipeline_graph_mapping.insert(pipeline.get_pipeline_id(), stack);
                    }
                    None => match meta_pipe.has_finish_event(pipeline.get_pipeline_id()) {
                        true => {
                            // Dependencies: base_finish -> pipeline_event ->
                            // pipeline_finish -> base_complete
                            let pipeline_finish_event = Arc::new(
                                PipelineEvent::create_finish_event(Arc::from(pipeline.clone())),
                            );

                            let finish_event_node_id = graph.add_node(pipeline_finish_event);

                            graph.add_edge(base_finish_event_node_id, running_event_node_id, ());
                            graph.add_edge(running_event_node_id, finish_event_node_id, ());

                            pipeline_graph_mapping.insert(
                                pipeline.get_pipeline_id(),
                                PipelineEventStack {
                                    pipeline_initialize_event: base_init_event_node_id,
                                    pipeline_event: running_event_node_id,
                                    pipeline_finish_event: finish_event_node_id,
                                    pipeline_complete_event: base_complete_event_node_id,
                                },
                            );
                        }
                        false => {
                            // Dependencies: base_initialize -> pipeline_event -> base_finish.
                            graph.add_edge(base_init_event_node_id, running_event_node_id, ());

                            pipeline_graph_mapping.insert(
                                pipeline.get_pipeline_id(),
                                PipelineEventStack {
                                    pipeline_initialize_event: base_init_event_node_id,
                                    pipeline_event: running_event_node_id,
                                    pipeline_finish_event: base_finish_event_node_id,
                                    pipeline_complete_event: base_complete_event_node_id,
                                },
                            );
                        }
                    },
                }
            }

            // Set up the dependencies within this `MetaPipeline`.
            for pipeline in pipelines.iter() {
                if let Some(_source) = pipeline.get_source() {
                    //         	if (source->type ==
                    // PhysicalOperatorType::TABLE_SCAN) { //
                    // we have to reset the source here (in the main thread),
                    // because some of our clients (looking at you, R)
                    // // do not like it when threads other than the main thread
                    // call into R, for e.g., arrow scans
                    // pipeline->ResetSource(true);
                    // }
                }

                match meta_pipe.find_dependencies(pipeline.get_pipeline_id())? {
                    Some(dep_pipes) => dep_pipes.iter().for_each(|dep_pipe| {
                        if let Some(dependency_stack) = pipeline_graph_mapping.get(dep_pipe) {
                            let stack = pipeline_graph_mapping
                                .get(&pipeline.get_pipeline_id())
                                .unwrap();
                            graph.add_edge(
                                dependency_stack.pipeline_event,
                                stack.pipeline_event,
                                (),
                            );
                        }
                    }),
                    None => continue,
                }
            }
        }

        // Set up the dependencies across `MetaPipeline`.
        for pipe in all_pipelines.iter() {
            for pipe_ix in pipe.get_dependencies().iter() {
                let from = pipeline_graph_mapping
                    .get(pipe_ix)
                    .unwrap()
                    .pipeline_complete_event;
                let to = pipeline_graph_mapping
                    .get(&pipe.get_pipeline_id())
                    .unwrap()
                    .pipeline_event;
                if !graph.contains_edge(from, to) {
                    graph.add_edge(from, to, ());
                }
            }
        }
        Ok(())
    }

    pub async fn init_schedule_event_queue(&self, global_execution_queue: &mut ExecutionQueue) {
        // Schedule source node.
        for node_idx in self.graph.externals(Direction::Outgoing) {
            let node = self.get_node_by_index(node_idx);
            // todo: fix deref pointer.
            let pipeline_event =
                unsafe { &mut *(node.as_ref() as *const PipelineEvent as *mut PipelineEvent) };
            pipeline_event.schedule(global_execution_queue).await;
        }
    }

    fn get_node_by_index(&self, index: NodeIndex) -> Arc<PipelineEvent> {
        self.graph[index].clone()
    }

    #[allow(dead_code)]
    fn get_prev_nodes(&self, index: NodeIndex) -> Vec<Arc<PipelineEvent>> {
        let mut prev_nodes = vec![];
        for edge in self.graph.edges_directed(index, Direction::Incoming) {
            prev_nodes.push(self.graph[edge.source()].clone());
        }
        prev_nodes
    }

    fn get_next_nodes(&self, index: NodeIndex) -> Vec<Arc<PipelineEvent>> {
        let mut next_nodes = vec![];
        for edge in self.graph.edges_directed(index, Direction::Outgoing) {
            next_nodes.push(self.graph[edge.target()].clone());
        }
        next_nodes
    }

    fn get_all_nodes(&self) -> Vec<Arc<PipelineEvent>> {
        self.graph.node_weights().cloned().collect()
    }

    fn get_last_node(&self) -> Arc<PipelineEvent> {
        let mut last_node = None;
        for node in self.graph.node_indices() {
            if self
                .graph
                .edges_directed(node, petgraph::Direction::Outgoing)
                .count()
                == 0
            {
                last_node = Some(self.graph[node].clone());
            }
        }
        last_node.unwrap()
    }
}
