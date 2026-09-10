// Go exits without C atexit hooks. Only instrumented test builds export this
// hook so their TestMain can persist the native counters before os.Exit.
#[unsafe(no_mangle)]
pub extern "C" fn dfgo_test_write_coverage() -> i32 {
    unsafe extern "C" {
        fn __llvm_profile_write_file() -> i32;
    }
    // SAFETY: the coverage build links LLVM's profiling runtime, whose flush
    // function has no arguments or caller-owned memory requirements.
    unsafe { __llvm_profile_write_file() }
}
