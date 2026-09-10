//! Diagnostic comparison with DataFusion's own executor, without the Go bridge.
#![cfg(feature = "test-sqllogictest")]

use std::path::{Path, PathBuf};

use datafusion_sqllogictest::{
    DataFusion, TestContext, df_value_validator, setup_scratch_dir, value_normalizer,
};

#[test]
#[ignore = "requires the pinned datasets; set DFGO_SQLLOGICTEST_SOURCE and DFGO_SQLLOGICTEST_FILE"]
fn upstream_oracle() -> Result<(), Box<dyn std::error::Error>> {
    let source = PathBuf::from(std::env::var("DFGO_SQLLOGICTEST_SOURCE")?);
    let file = std::env::var("DFGO_SQLLOGICTEST_FILE")?;
    // This diagnostic changes the working directory. Select it by name when
    // invoking ignored tests to keep the upstream convention isolated.
    std::env::set_current_dir(source.join("datafusion/sqllogictest"))?;
    let path = Path::new(&file);
    setup_scratch_dir(path)?;
    tokio::runtime::Runtime::new()?.block_on(async {
        let fixture = TestContext::try_new_for_test_file(path)
            .await
            .ok_or("upstream fixture unavailable")?;
        let mut runner = sqllogictest::Runner::new(|| async {
            Ok(DataFusion::new(
                fixture.session_ctx().clone(),
                path.to_path_buf(),
                indicatif::ProgressBar::hidden(),
            ))
        });
        runner.with_column_validator(sqllogictest::strict_column_validator);
        runner.with_normalizer(value_normalizer);
        runner.with_validator(df_value_validator);
        runner
            .run_file_async(Path::new("test_files").join(path))
            .await?;
        runner.shutdown_async().await;
        Ok(())
    })
}

#[test]
#[ignore = "diagnostic for tight-memory streaming with a delayed consumer"]
fn upstream_spill_with_delayed_consumer() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Runtime::new()?.block_on(async {
        let fixture = TestContext::try_new_for_test_file(Path::new("aggregate_memory_spill.slt"))
            .await.ok_or("fixture unavailable")?;
        let ctx = fixture.session_ctx();
        for sql in ["SET datafusion.execution.target_partitions = 4",
                    "SET datafusion.execution.batch_size = 128",
                    "SET datafusion.runtime.memory_limit = '1M'"] {
            ctx.sql(sql).await?.collect().await?;
        }
        let stream = ctx.sql("SELECT count(*), sum(total) FROM (SELECT (v * 7) % 100000 AS k, sum(v) AS total FROM generate_series(1, 100000) AS t(v) GROUP BY (v * 7) % 100000)")
            .await?.execute_stream().await?;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let batches = tokio::time::timeout(std::time::Duration::from_secs(5), datafusion::physical_plan::common::collect(stream)).await??;
        assert_eq!(batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
        let values = datafusion_sqllogictest::convert_batches(&batches[0].schema(), batches.clone(), false)?;
        assert_eq!(values, vec![vec!["100000".to_owned(), "5000050000".to_owned()]]);
        Ok(())
    })
}

#[test]
#[ignore = "diagnostic for separate synchronous planning and stream polls"]
fn upstream_spill_with_blocking_pulls() -> Result<(), Box<dyn std::error::Error>> {
    use futures::StreamExt;
    use std::time::Duration;

    let runtime = tokio::runtime::Runtime::new()?;
    let fixture = runtime
        .block_on(TestContext::try_new_for_test_file(Path::new(
            "aggregate_memory_spill.slt",
        )))
        .ok_or("fixture unavailable")?;
    let ctx = fixture.session_ctx();
    for sql in [
        "SET datafusion.execution.target_partitions = 4",
        "SET datafusion.execution.batch_size = 128",
        "SET datafusion.runtime.memory_limit = '1M'",
    ] {
        runtime.block_on(async { ctx.sql(sql).await?.collect().await })?;
    }
    let query = "SELECT count(*), sum(total) FROM (SELECT (v * 7) % 100000 AS k, sum(v) AS total FROM generate_series(1, 100000) AS t(v) GROUP BY (v * 7) % 100000)";
    let explain = format!("EXPLAIN ANALYZE {query}");
    let async_collect = std::env::var_os("DFGO_SQLLOGICTEST_ASYNC_COLLECT").is_some();
    for iteration in 0..25 {
        for (sql, is_explain) in [(query, false), (explain.as_str(), true)] {
            println!("iteration {iteration}: explain={is_explain}, async_collect={async_collect}");
            let batches = if async_collect {
                runtime.block_on(async {
                    tokio::time::timeout(Duration::from_secs(5), async {
                        ctx.sql(sql).await?.collect().await
                    })
                    .await
                })??
            } else {
                let mut stream = runtime.block_on(async {
                    tokio::time::timeout(Duration::from_secs(5), async {
                        ctx.sql(sql).await?.execute_stream().await
                    })
                    .await
                })??;
                let mut batches = Vec::new();
                loop {
                    let next = runtime.block_on(async {
                        tokio::time::timeout(Duration::from_secs(5), stream.next()).await
                    })?;
                    match next {
                        Some(batch) => batches.push(batch?),
                        None => break,
                    }
                }
                batches
            };
            let schema = batches.first().ok_or("query returned no batches")?.schema();
            let values = datafusion_sqllogictest::convert_batches(&schema, batches, false)?;
            if is_explain {
                let spilled = values
                    .iter()
                    .flatten()
                    .flat_map(|value| value.lines())
                    .any(|line| {
                        line.contains("mode=FinalPartitioned")
                            && line.split_once("spill_count=").is_some_and(|(_, count)| {
                                count
                                    .chars()
                                    .take_while(|c| c.is_ascii_digit())
                                    .collect::<String>()
                                    .parse::<usize>()
                                    .is_ok_and(|count| count > 0)
                            })
                    });
                assert!(
                    spilled,
                    "iteration {iteration}: final aggregation did not spill: {values:?}"
                );
            } else {
                assert_eq!(
                    values,
                    vec![vec!["100000".to_owned(), "5000050000".to_owned()]],
                    "iteration {iteration}: spill lost rows"
                );
            }
        }
    }
    Ok(())
}
