use super::meta_pipeline::{MetaPipeline, PipelineIx};
use crate::executor::physical::{PhysicalOperator, PhysicalOperatorRef};
use anyhow::Result;

pub struct PipelineBuilder {
    current_pipeline: usize,
    last_pipeline_id: PipelineIx,
}

impl PipelineBuilder {
    pub fn new() -> PipelineBuilder {
        PipelineBuilder {
            current_pipeline: 0,
            last_pipeline_id: 0,
        }
    }

    pub fn finalize(&mut self, plan: PhysicalOperatorRef) -> Result<MetaPipeline> {
        let mut meta_pipeline = MetaPipeline::create(None);
        let pipe_ix = meta_pipeline.create_pipeline(self.get_next_pipeline_id())?;
        self.current_pipeline = pipe_ix;
        self.build_meta_pipeline(&mut meta_pipeline, plan)?;
        Ok(meta_pipeline)
    }

    fn get_next_pipeline_id(&mut self) -> PipelineIx {
        let ix = self.last_pipeline_id;
        self.last_pipeline_id += 1;
        ix
    }

    fn build_meta_pipeline(
        &mut self,
        meta_pipeline: &mut MetaPipeline,
        plan: PhysicalOperatorRef,
    ) -> Result<()> {
        match plan.as_ref() {
            PhysicalOperator::Join => self.build_join_pipeline(meta_pipeline, plan)?,
            _ => {
                self.build_pipeline(meta_pipeline, plan)?;
            }
        }
        Ok(())
    }

    fn build_pipeline(
        &mut self,
        meta_pipeline: &mut MetaPipeline,
        plan: PhysicalOperatorRef,
    ) -> Result<()> {
        let current = self.current_pipeline;
        if plan.is_sink() {
            // Operator is a sink, build a pipeline.
            // meta_pipeline.pipelines[current].sink = None;
            assert!(plan.children().len() == 1);

            meta_pipeline.pipelines[current].source = Some(plan.clone());

            // Create a new pipeline starting from the child.
            let child_meta_pipe = meta_pipeline.create_child_meta_pipeline(
                self.get_next_pipeline_id(),
                current,
                plan.clone(),
            )?;

            self.build_meta_pipeline(child_meta_pipe, plan.children()[0].clone())?;
        } else {
            // Operator is not a sink, recurse in children.
            let children = plan.children();
            if children.is_empty() {
                meta_pipeline.pipelines[current].source = Some(plan);
            } else {
                assert!(children.len() == 1);

                let children = plan.children();
                meta_pipeline.pipelines[current].operators.push(plan);

                self.build_pipeline(meta_pipeline, children[0].clone())?;
            }
        }

        Ok(())
    }

    // todo
    fn build_join_pipeline(
        &mut self,
        meta_pipeline: &mut MetaPipeline,
        plan: PhysicalOperatorRef,
    ) -> Result<()> {
        assert!(plan.children().len() == 2);

        // current is the probe pipeline: add this operator.
        let current = self.current_pipeline;
        meta_pipeline.pipelines[current]
            .operators
            .push(plan.clone());

        let _pipelines_so_far = meta_pipeline.get_pipelines(false)?;

        // Build side(RHS).
        let child_meta_pipeline = meta_pipeline.create_child_meta_pipeline(
            self.get_next_pipeline_id(),
            current,
            plan.clone(),
        )?;
        self.build_meta_pipeline(child_meta_pipeline, plan.children()[1].clone())?;

        // Build probe side(LHS).
        self.build_pipeline(meta_pipeline, plan.children()[0].clone())?;

        // match plan.as_ref() {
        //     PhysicalOperator::Join => {
        //         todo!()
        //     }
        //     _ => return Ok(()),
        // }

        Ok(())
    }
}

#[cfg(test)]
mod pipeline_builder_tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        executor::physical::{
            physical_filter::PhysicalFilter, physical_limit::PhysicalLimit,
            physical_projection::PhysicalProjection, physical_scan::PhysicalTableScan,
            physical_sort::PhysicalSort,
        },
        expression::ScalarExpression,
        planner::operator::sort::SortField,
        types::value::DataValue,
    };

    fn build_simple_physical_op_tree() -> PhysicalOperatorRef {
        let scan = PhysicalTableScan { plan_id: 1 };
        let filter = PhysicalFilter {
            plan_id: 2,
            input: Arc::new(PhysicalOperator::TableScan(scan)),
            predicates: ScalarExpression::Constant(DataValue::Null),
        };

        let sort = PhysicalSort {
            plan_id: 3,
            input: Arc::new(PhysicalOperator::Filter(filter)),
            order_by: vec![SortField::new(
                ScalarExpression::Constant(DataValue::Null),
                true,
                true,
            )],
            limit: None,
        };

        let sort_project = PhysicalProjection {
            plan_id: 4,
            input: Arc::new(PhysicalOperator::Sort(sort)),
        };

        let limit = PhysicalLimit {
            plan_id: 5,
            input: Arc::new(PhysicalOperator::Prjection(sort_project)),
            limit: 1,
            offset: 1,
        };

        let project = PhysicalProjection {
            plan_id: 6,
            input: Arc::new(PhysicalOperator::Limit(limit)),
        };

        Arc::new(PhysicalOperator::Prjection(project))
    }

    #[test]
    fn test_build_pipeline() {
        let mut builder = PipelineBuilder::new();

        let plan = build_simple_physical_op_tree();

        let meta_pipe = builder.finalize(plan).unwrap();

        let pipelines = meta_pipe.get_pipelines(true).unwrap();
        for pipeline in pipelines {
            println!("{:#?}", pipeline);
        }
    }
}
