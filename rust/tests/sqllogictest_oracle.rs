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
    // This integration-test binary has one test. Keep the upstream working
    // directory convention isolated from the library's ordinary unit tests.
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
