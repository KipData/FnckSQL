use std::io;

use kip_sql::planner::{display::display_plan, logical_plan_builder::PlanBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let builder = PlanBuilder::new();

    loop {
        println!(":) Welcome to the KIPSQL, Please input sql.");
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let output = handle_query(input, &builder);
        println!("KIPSQL Output: {output}");
    }
}

fn handle_query(sql: String, builder: &PlanBuilder) -> String {
    let plan = builder.build_sql(&sql);
    if plan.is_err() {
        return plan.err().unwrap().to_string() + " ERROR";
    }
    return "OK".to_string();

    //let logical_graph = display_plan(&plan);
    //tracing::info!("logical plan {}", logical_graph);

    // todo optimize.
    // let mut optimize = Optimizer::new();
    // let _optmized_plan = optimize.optimize(&plan).unwrap();

    // todo executor.
    //let logical_graph = display_plan(&plan);
}
