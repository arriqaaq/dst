use std::time::Duration;

use crate::sim::history::RunSummary;

#[derive(Debug, Clone)]
pub struct SeedRunResult {
    pub seed: u64,
    pub summary: RunSummary,
    pub error: Option<String>,
}

#[derive(Debug)]
pub struct SummaryTable {
    pub results: Vec<SeedRunResult>,
}

impl SummaryTable {
    pub fn new() -> Self {
        Self {
            results: Vec::new(),
        }
    }

    pub fn push(&mut self, result: SeedRunResult) {
        self.results.push(result);
    }

    pub fn passed(&self) -> usize {
        self.results.iter().filter(|r| r.error.is_none()).count()
    }

    pub fn failed(&self) -> usize {
        self.results.iter().filter(|r| r.error.is_some()).count()
    }

    pub fn total(&self) -> usize {
        self.results.len()
    }

    pub fn failures(&self) -> impl Iterator<Item = &SeedRunResult> {
        self.results.iter().filter(|r| r.error.is_some())
    }
}

impl Default for SummaryTable {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SummaryTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Seed sweep: {}/{} passed", self.passed(), self.total())?;
        for r in self.failures() {
            writeln!(
                f,
                "  FAIL seed={} steps={} error={}",
                r.seed,
                r.summary.steps,
                r.error.as_deref().unwrap_or("?"),
            )?;
        }
        Ok(())
    }
}

pub fn run_seed_sweep<F>(seeds: impl IntoIterator<Item = u64>, mut factory: F) -> SummaryTable
where
    F: FnMut(u64) -> Result<RunSummary, String>,
{
    let mut table = SummaryTable::new();
    for seed in seeds {
        match factory(seed) {
            Ok(summary) => {
                table.push(SeedRunResult {
                    seed,
                    summary,
                    error: None,
                });
            }
            Err(e) => {
                table.push(SeedRunResult {
                    seed,
                    summary: RunSummary {
                        steps: 0,
                        final_elapsed: Duration::ZERO,
                        history_hash: [0; 32],
                        clients_ok: false,
                        total_events: 0,
                    },
                    error: Some(e),
                });
            }
        }
    }
    table
}
