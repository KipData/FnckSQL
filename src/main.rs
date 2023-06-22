use std::io;

use kip_sql::planner::{display::display_plan, logical_plan_builder::PlanBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(":) Welcome to the KIPSQL, Please input sql.");

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    let output = handle_query(input);

    println!("KIPSQL Output: {output}");

    Ok(())
}

fn handle_query(sql: String) -> String {
    let builder = PlanBuilder::new();
    let plan = builder.build_sql(&sql).unwrap();

    let logical_graph = display_plan(&plan);
    tracing::info!("logical plan {}", logical_graph);

    // todo optimize.
    // let mut optimize = Optimizer::new();
    // let _optmized_plan = optimize.optimize(&plan).unwrap();

    // todo executor.
    logical_graph
}
