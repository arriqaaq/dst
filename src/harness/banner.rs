use crate::sim::history::RunSummary;

use super::scenario::Scenario;

pub fn format_banner(scenario: &Scenario) -> String {
    let mut lines = Vec::new();
    lines.push("╔══════════════════════════════════════════════════╗".to_string());
    lines.push("║            DST SIMULATION RUN                   ║".to_string());
    lines.push("╠══════════════════════════════════════════════════╣".to_string());
    lines.push(format!("║  SEED:          {:<33}║", scenario.seed));
    lines.push(format!(
        "║  MAX DURATION:  {:<33}║",
        format!("{:?}", scenario.max_simulated_time)
    ));
    if !scenario.label.is_empty() {
        lines.push(format!("║  LABEL:         {:<33}║", scenario.label));
    }
    lines.push("╚══════════════════════════════════════════════════╝".to_string());
    lines.join("\n")
}

pub fn format_failure_repro(scenario: &Scenario, summary: &RunSummary) -> String {
    let mut lines = Vec::new();
    lines.push("──── DST FAILURE REPRO ────".to_string());
    lines.push(format!("  SEED:   {}", scenario.seed));
    lines.push(format!("  STEPS:  {}", summary.steps));
    lines.push(format!("  ELAPSED: {:?}", summary.final_elapsed));
    lines.push(format!(
        "  HASH:   {}",
        summary
            .history_hash
            .iter()
            .take(8)
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    ));
    lines.push(format!(
        "  REPRO:  {}",
        super::scenario::repro_command_line(scenario)
    ));
    lines.push("───────────────────────────".to_string());
    lines.join("\n")
}
