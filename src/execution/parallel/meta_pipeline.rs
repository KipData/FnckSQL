use std::collections::HashMap;

use crate::execution::executor::Executor;
use crate::execution::physical::{PhysicalOperator, PhysicalOperatorRef};

use super::pipeline::Pipeline;
use anyhow::Result;

pub type PipelineIx = usize;

/// MetaPipeline represents a set of pipelines that have the same sink.
#[derive(Clone)]
pub struct MetaPipeline {
    /// The executor for all MetaPipeline in the query plan.
    // pub executor: Executor,

    /// The sink of all pipelines within this MetaPipeline.
    pub sink: Option<PhysicalOperatorRef>,

    /// All pipelines with the different source, but the same sink.
    pub pipelines: Vec<Pipeline>,

    /// Dependencies within this MetaPipeline.
    pub dependencies: HashMap<PipelineIx, Vec<PipelineIx>>,

    /// Other MetaPipelines that this MetaPipeline depends on.
    pub children: Vec<MetaPipeline>,

    /// Pipelines (other than the base pipeline) that need their own
    /// PipelineFinishEvent (e.g., for IEJoin)
    pub finish_pipelines: Vec<PipelineIx>,

    /// Next batch index.
    pub next_batch_index: u64,

    /// Mapping from pipeline(e.g., child or union) to finish pipeline.
    pub finish_map: HashMap<PipelineIx, PipelineIx>,
}

pub const BATCH_INCREMENT: u64 = 10000000000000;

impl MetaPipeline {
    pub fn create(sink: Option<PhysicalOperatorRef>) -> Self {
        MetaPipeline {
            // executor: todo!(),
            sink,
            pipelines: Vec::new(),
            dependencies: HashMap::new(),
            children: Vec::new(),
            finish_pipelines: Vec::new(),
            next_batch_index: 0,
            finish_map: HashMap::new(),
        }
    }

    /// Returns the Executor for this MetaPipeline.
    pub fn get_executor(&self) -> Result<Executor> {
        todo!()
    }

    /// Returns the sink operator for this MetaPipeline.
    pub fn get_sink(&self) -> Result<PhysicalOperator> {
        todo!()
    }

    pub fn find_dependencies(&self, pipeline_ix: PipelineIx) -> Result<Option<Vec<PipelineIx>>> {
        Ok(self.dependencies.get(&pipeline_ix).cloned())
    }

    /// Returns the initial pipeline of this MetaPipeline.
    pub fn get_base_pipeline(&self) -> Result<Pipeline> {
        Ok(self.pipelines[0].clone())
    }

    /// Returns the pipelines for this MetaPipeline.
    pub fn get_pipelines(&self, recursive: bool) -> Result<Vec<Pipeline>> {
        let mut pipelines = vec![];
        pipelines.extend_from_slice(&self.pipelines);

        if recursive {
            for child in &self.children {
                pipelines.extend_from_slice(&child.get_pipelines(recursive)?);
            }
        }
        Ok(pipelines)
    }

    /// Returns the MetaPipeline children of this MetaPipeline.
    pub fn get_meta_pipelines(&self, recursive: bool, skip: bool) -> Result<Vec<MetaPipeline>> {
        let mut meta_pipelines = vec![];
        if !skip {
            meta_pipelines.push(self.clone());
        }

        if recursive {
            for child in &self.children {
                child.get_meta_pipelines(true, false)?;
            }
        }
        Ok(meta_pipelines)
    }

    /// Create an empty pipeline within this MetaPipeline.
    pub fn create_pipeline(&mut self, pipeline_id: PipelineIx) -> Result<PipelineIx> {
        let mut pipeline = Pipeline::new(pipeline_id);
        pipeline.sink = self.sink.clone();
        pipeline.base_batch_index = BATCH_INCREMENT * self.next_batch_index;
        self.next_batch_index += 1;
        let ix = self.pipelines.len();
        self.pipelines.push(pipeline);
        Ok(ix)
    }

    /// Create a union pipeline.
    pub fn create_union_pipeline(&self, pipeline_id: PipelineIx) -> Result<Pipeline> {
        todo!()
    }

    /// Create a child pipeline operator `starting` at `operator`.
    /// where 'last_pipeline' is the last pipeline added before building out
    /// 'current'.
    pub fn create_child_pipeline(&self, pipeline_id: PipelineIx) {}

    /// Create a MetaPipeline child that `current` deponds on.
    pub fn create_child_meta_pipeline(
        &mut self,
        pipeline_id: PipelineIx,
        current: usize,
        operator: PhysicalOperatorRef,
    ) -> Result<&mut MetaPipeline> {
        let mut child_meta_pipe = MetaPipeline::create(Some(operator));
        child_meta_pipe.create_pipeline(pipeline_id)?;
        self.children.push(child_meta_pipe.clone());

        self.pipelines[current].dependencies.push(pipeline_id);
        Ok(self.children.last_mut().unwrap())
    }

    pub fn get_finish_group(&self, pipeline_ix: PipelineIx) -> Option<&PipelineIx> {
        self.finish_map.get(&pipeline_ix)
    }

    pub fn has_finish_event(&self, pipeline_ix: PipelineIx) -> bool {
        self.finish_pipelines.contains(&pipeline_ix)
    }
}
