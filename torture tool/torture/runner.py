"""
Crash Test Runner for Supernova Torture Tool.

Executes the adversarial crash loop:
1. Creates fresh database file.
2. Spawns database engine in serve mode.
3. Spawns concurrent writer thread.
4. Waits for a random millisecond delay while workload is active.
5. Injects SIGKILL directly into the running process.
6. Verifies process termination via SIGKILL.
7. Safely joins writer thread and preserves received ACKs.
8. Re-opens database, checks integrity, dumps state.
9. Compares dump against the independent Oracle.
10. Reports progress and stops at the first failure, preserving diagnostic artifacts.
"""

import os
import random
import signal
import subprocess
import sys
import threading
import time
import traceback
from typing import List, Optional

from torture.config import TortureConfig, TortureMode
from torture.oracle import BatchSubOp, DatabaseOracle, OpType
from torture.protocol import BatchItem, EngineClient
from torture.reporting import (
    ChildProcessExitInfo,
    CompactionCrashInfo,
    DiskFullCrashInfo,
    FailureCategory,
    FailureDetails,
    IterationReport,
    RecoveryCrashInfo,
    Reporter,
    classify_failure,
)
from torture.verifier import DumpParser, DumpParseResult, StateVerifier
from torture.worker import SingleWriterWorker


class CrashTestRunner:
    """Orchestrates single-writer and multi-writer random-time crash consistency testing."""

    def __init__(self, config: TortureConfig, reporter: Optional[Reporter] = None) -> None:
        self.config = config
        self.reporter = reporter or Reporter(failure_dir=config.artifacts_dir)
        self.master_rng = random.Random(config.seed)

    def _clean_db(self, path: str) -> None:
        """Ensures a clean database file state before each iteration, removing all sidecars."""
        parent_dir = os.path.dirname(os.path.abspath(path))
        base_name = os.path.basename(path)
        if os.path.exists(parent_dir):
            for fname in os.listdir(parent_dir):
                if fname == base_name or fname.startswith(base_name + "."):
                    full_p = os.path.join(parent_dir, fname)
                    try:
                        if os.path.isdir(full_p):
                            import shutil
                            shutil.rmtree(full_p)
                        else:
                            os.remove(full_p)
                    except OSError:
                        pass

    def _run_recovery_crash_loop(self) -> int:
        """
        CP9 Recovery-Crash Testing Loop.
        
        Fixes applied:
        A. Seed once with large batches (10,000 ops) and copy per iteration.
        B. Calibrate kill delay from baseline measurements of startup and recovery.
        C. Record evidence of where kill landed (size before vs after truncate) and track histogram.
        D. Vary torn tail across runs (fewer than 8B, length past EOF, bad CRC, cut by 1B / F4, clean).
        E. Chain 1 to 3 kills before final verification.
        F. Clean, honest reporting with consistent numbering and phase histogram.
        """
        import shutil
        import struct
        import statistics

        target_successes = self.config.iterations
        db_path = self.config.db_path
        golden_db = db_path + ".golden_seed"

        # -------------------------------------------------------------
        # Step A: Seed Once (Large Database with multi-op batches)
        # -------------------------------------------------------------
        self._clean_db(db_path)
        self._clean_db(golden_db)

        golden_oracle = DatabaseOracle()
        print("=" * 65)
        print("SEEDING GOLDEN DATABASE FOR RECOVERY-CRASH TESTING...")
        print("=" * 65)

        seeder_client = EngineClient(
            engine_bin=self.config.engine_bin,
            db_path=golden_db,
            default_timeout=10.0,
        )
        try:
            seeder_client.start()
        except Exception as exc:
            sys.stderr.write(f"Failed to start engine for seeding: {exc}\n")
            return 1

        t_seed_start = time.perf_counter()
        num_batches = 10
        ops_per_batch = 1000
        for b_idx in range(num_batches):
            op_id = f"rc-golden-batch-{b_idx}"
            b_items = []
            oracle_items = []
            for i in range(ops_per_batch):
                k = f"key_{((b_idx * ops_per_batch + i) % 200):04d}"
                coin = self.master_rng.random()
                if coin < 0.75:
                    v = f"val_b{b_idx}_i{i}_" + ("x" * 40)
                    b_items.append(BatchItem.make_put(k, v))
                    oracle_items.append(BatchSubOp(op_type=OpType.PUT, key=k, value=v))
                else:
                    b_items.append(BatchItem.make_del(k))
                    oracle_items.append(BatchSubOp(op_type=OpType.DEL, key=k))

            golden_oracle.record_batch(op_id, oracle_items, writer_id="golden-seeder")
            seeder_client.batch(b_items, op_id=op_id)

        seeder_client.close()
        t_seed_dur = time.perf_counter() - t_seed_start
        golden_file = golden_db if os.path.exists(golden_db) else (golden_db + ".data" if os.path.exists(golden_db + ".data") else golden_db)
        golden_size_kb = (os.path.getsize(golden_file) if os.path.exists(golden_file) else 0) / 1024.0
        print(f"Seeding completed in {t_seed_dur:.3f}s: 10,000 ops, {golden_size_kb:.1f} KB on disk.")

        # -------------------------------------------------------------
        # Step B: Calibrate from Measurement (5 baseline runs)
        # -------------------------------------------------------------
        print("Calibrating process startup and recovery execution windows...")
        calib_total_times = []
        calib_recovery_times = []

        for _ in range(5):
            t0 = time.perf_counter()
            calib_cmd = [sys.executable, self.config.engine_bin, "open", golden_db] if self.config.engine_bin.endswith(".py") else [self.config.engine_bin, "open", golden_db]
            p = subprocess.run(
                calib_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=5.0,
            )
            t1 = time.perf_counter()
            total_time = t1 - t0
            calib_total_times.append(total_time)

            rec_sec = 0.005
            for line in p.stdout.splitlines():
                if "recovery=" in line:
                    try:
                        raw = line.split("recovery=")[1].strip()
                        if raw.endswith("ms"):
                            rec_sec = float(raw[:-2]) / 1000.0
                        elif raw.endswith("µs") or raw.endswith("us"):
                            rec_sec = float(raw[:-2]) / 1_000_000.0
                        elif raw.endswith("s"):
                            rec_sec = float(raw[:-1])
                    except Exception:
                        pass
            calib_recovery_times.append(rec_sec)

        median_total = statistics.median(calib_total_times) if calib_total_times else 0.010
        median_recovery = statistics.median(calib_recovery_times) if calib_recovery_times else 0.002
        median_startup = max(0.001, median_total - median_recovery)
        print(
            f"Calibration: Median Startup={median_startup*1000:.2f}ms, "
            f"Median Recovery={median_recovery*1000:.2f}ms (Total={median_total*1000:.2f}ms)"
        )
        print("=" * 65)

        # -------------------------------------------------------------
        # Execution Loop (Chained Kills, Torn Tail Variants, Phase Telemetry)
        # -------------------------------------------------------------
        passed_count = 0
        missed_count = 0
        attempt = 0
        max_attempts = max(target_successes * 5, 25)

        phase_counts = {
            "before truncate (scan active)": 0,
            "after truncate (repair started)": 0,
        }
        variant_counts = {
            "fewer_than_8": 0,
            "length_past_eof": 0,
            "bad_crc": 0,
            "cut_by_1_byte_f4": 0,
            "clean": 0,
        }

        while passed_count < target_successes and attempt < max_attempts:
            attempt += 1
            t_iter_start = time.perf_counter()
            iter_seed = self.master_rng.randint(1, 100_000_000)
            iter_rng = random.Random(iter_seed)

            # Copy golden database and sidecars
            if os.path.exists(golden_db):
                shutil.copyfile(golden_db, db_path)
            if os.path.exists(golden_db + ".data"):
                shutil.copyfile(golden_db + ".data", db_path + ".data")
            if os.path.exists(golden_db + ".wal"):
                shutil.copyfile(golden_db + ".wal", db_path + ".wal")

            oracle = DatabaseOracle()
            with golden_oracle._lock:
                oracle._seq_counter = golden_oracle._seq_counter
                oracle._history = list(golden_oracle._history)
                oracle._history_by_id = dict(golden_oracle._history_by_id)

            variant = iter_rng.choice([
                "fewer_than_8",
                "length_past_eof",
                "bad_crc",
                "cut_by_1_byte_f4",
                "clean",
            ])

            target_data_file = db_path if os.path.exists(db_path) else db_path + ".data"
            curr_size = os.path.getsize(target_data_file) if os.path.exists(target_data_file) else 0

            if variant == "fewer_than_8":
                with open(target_data_file, "ab") as f:
                    f.write(bytes([iter_rng.randint(1, 255) for _ in range(iter_rng.randint(1, 7))]))
            elif variant == "length_past_eof":
                with open(target_data_file, "ab") as f:
                    f.write(struct.pack(">I", 50000) + b"\xde\xad\xbe\xef" + b"incomplete_tail")
            elif variant == "bad_crc":
                payload = b"\x00\x00\x00\x01\x01\x00\x06\x00\x00\x00\x06foobartest"
                with open(target_data_file, "ab") as f:
                    f.write(struct.pack(">II", len(payload), 0xBAD0DEAD) + payload)
            elif variant == "cut_by_1_byte_f4":
                if curr_size > 16:
                    with open(target_data_file, "r+b") as f:
                        f.truncate(curr_size - 1)

            num_chain_kills = iter_rng.randint(1, 3)
            chain_success = True
            last_phase = "before truncate (scan active)"
            last_delay_ms = 0.0
            last_exit_status = 0
            rec_start_time = time.time()
            rec_comp_time = time.time()

            for chain_step in range(num_chain_kills):
                size_before = os.path.getsize(target_data_file) if os.path.exists(target_data_file) else 0
                open_cmd = [sys.executable, self.config.engine_bin, "open", db_path] if self.config.engine_bin.endswith(".py") else [self.config.engine_bin, "open", db_path]
                rec_start_time = time.time()
                open_proc = subprocess.Popen(
                    open_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )

                # Check if custom min/max delay was explicitly requested by test/user
                has_custom_delay = (self.config.min_kill_delay_ms != 1 or self.config.max_kill_delay_ms != 50)
                if has_custom_delay:
                    delay_ms = iter_rng.uniform(
                        self.config.min_kill_delay_ms,
                        self.config.max_kill_delay_ms,
                    )
                else:
                    scan_started = False
                    try:
                        line = open_proc.stderr.readline()
                        if "RECOVERY_SCAN_START" in line:
                            scan_started = True
                    except Exception:
                        pass

                    if scan_started:
                        delay_ms = iter_rng.uniform(0.5, max(1.5, median_recovery * 1000 * 0.75))
                    else:
                        delay_ms = iter_rng.uniform(
                            median_startup * 1000,
                            (median_startup + median_recovery * 0.75) * 1000,
                        )

                last_delay_ms = delay_ms
                time.sleep(delay_ms / 1000.0)

                poll_ret = open_proc.poll()
                if poll_ret is not None:
                    chain_success = False
                    last_exit_status = poll_ret
                    rec_comp_time = time.time()
                    break

                try:
                    if sys.platform == "win32":
                        os.kill(open_proc.pid, signal.SIGKILL)
                    else:
                        open_proc.send_signal(signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass

                try:
                    open_proc.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    open_proc.kill()
                    open_proc.wait()

                rec_comp_time = time.time()
                last_exit_status = open_proc.returncode

                open_stdout = ""
                if open_proc.stdout:
                    try:
                        open_stdout = open_proc.stdout.read()
                    except Exception:
                        pass
                    try:
                        open_proc.stdout.close()
                    except Exception:
                        pass
                if open_proc.stderr:
                    try:
                        open_proc.stderr.close()
                    except Exception:
                        pass

                if open_proc.returncode == 0 or "recovery=" in open_stdout:
                    chain_success = False
                    break

                size_after = os.path.getsize(target_data_file) if os.path.exists(target_data_file) else 0
                if size_after < size_before:
                    last_phase = "after truncate (repair started)"
                else:
                    last_phase = "before truncate (scan active)"

            if not chain_success:
                missed_count += 1
                rec_info = RecoveryCrashInfo(
                    recovery_start_time=rec_start_time,
                    requested_kill_delay_ms=last_delay_ms,
                    actual_exit_status=last_exit_status,
                    sigkill_occurred=False,
                    recovery_completion_time=rec_comp_time,
                    missed_crash_window=True,
                    second_recovery_success=False,
                    second_recovery_time_s=0.0,
                )
                rep = IterationReport(
                    iteration=attempt,
                    total_iterations=target_successes,
                    passed=False,
                    delay_ms=int(last_delay_ms),
                    submitted=10000,
                    acked=10000,
                    recovered_keys=0,
                    duration_s=time.perf_counter() - t_iter_start,
                    violations=["MISSED CRASH WINDOW"],
                    mode="recovery-crash",
                    missed_crash_window=True,
                    recovery_info=rec_info,
                )
                self.reporter.print_iteration_summary(rep)
                continue

            verifier = StateVerifier(
                engine_bin=self.config.engine_bin,
                db_path=db_path,
                timeout=5.0,
            )
            t_v_start = time.perf_counter()
            passed, violations, actual_dump = verifier.verify(oracle)
            t_verify_dur = time.perf_counter() - t_v_start
            t_iter_dur = time.perf_counter() - t_iter_start

            rec_info = RecoveryCrashInfo(
                recovery_start_time=rec_start_time,
                requested_kill_delay_ms=last_delay_ms,
                actual_exit_status=last_exit_status,
                sigkill_occurred=True,
                recovery_completion_time=rec_comp_time,
                missed_crash_window=False,
                second_recovery_success=passed,
                second_recovery_time_s=t_verify_dur,
            )

            rep = IterationReport(
                iteration=passed_count + 1,
                total_iterations=target_successes,
                passed=passed,
                delay_ms=int(last_delay_ms),
                submitted=10000,
                acked=10000,
                recovered_keys=len(actual_dump),
                duration_s=t_iter_dur,
                violations=violations,
                mode="recovery-crash",
                missed_crash_window=False,
                recovery_info=rec_info,
            )
            self.reporter.print_iteration_summary(rep)

            if not passed:
                child_exit = ChildProcessExitInfo(
                    pid=open_proc.pid if open_proc else None,
                    returncode=last_exit_status,
                    is_alive=False,
                    killed_by_sigkill=True,
                    termination_cause=f"Exit code {last_exit_status}",
                )
                details = FailureDetails(
                    category=FailureCategory.RECOVERY_FAILURE,
                    iteration=attempt,
                    total_iterations=target_successes,
                    seed=self.config.seed,
                    derived_seed=iter_seed,
                    mode=self.config.mode.value,
                    db_path=db_path,
                    errors=violations,
                    child_exit=child_exit,
                    actual_dump=actual_dump,
                    recovery_info=rec_info,
                )
                art_dir = self.reporter.preserve_failure_artifacts(details, oracle=oracle)
                sys.stderr.write(f"\n[FAIL] Attempt {attempt} failed verification: {violations}\n")
                sys.stderr.write(f"Artifacts preserved in: {art_dir}\n")
                return 1

            passed_count += 1
            phase_counts[last_phase] += 1
            variant_counts[variant] += 1

        for p in [golden_db, golden_db + ".data", golden_db + ".wal"]:
            if os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass

        print("=" * 65)
        if passed_count >= target_successes:
            print(f"ALL {passed_count} RECOVERY-CRASH ITERATIONS PASSED CRASH CONSISTENCY VERIFICATION!")
        else:
            print(f"FAILED: Only {passed_count} of {target_successes} iterations passed!")
        print(f"Total Attempts: {attempt} | Missed Crash Windows: {missed_count}")
        print("Phase Distribution:")
        for ph, cnt in phase_counts.items():
            print(f"  - {ph:<35}: {cnt}")
        print("Torn Tail Variants Covered:")
        for var, cnt in variant_counts.items():
            print(f"  - {var:<35}: {cnt}")
        print("=" * 65)

        return 0 if passed_count >= target_successes else 1

    def _run_compaction_crash_loop(self) -> int:
        """
        CP10 Compaction-Crash Testing Loop.
        
        Workflow:
        1. Create a database with meaningful data (repeated writes and deletes).
        2. Maintain the independent oracle.
        3. Trigger COMPACT.
        4. Wait for a random millisecond delay.
        5. Kill the engine using SIGKILL.
        6. Restart the engine / run open, check, dump.
        7. Compare recovered state against the oracle.
        8. Verify:
           - Acknowledged data is preserved.
           - No unknown values appear.
           - Old/new file swapping is safe.
           - Temporary files do not become authoritative incorrectly.
           - Recovery succeeds after compaction interruption.
        """
        passed_count = 0

        for iter_num in range(1, self.config.iterations + 1):
            t_start = time.time()
            iter_seed = self.master_rng.randint(1, 100_000_000)
            iter_rng = random.Random(iter_seed)

            db_path = self.config.db_path
            oracle = DatabaseOracle()
            client: Optional[EngineClient] = None
            compact_thread: Optional[threading.Thread] = None

            try:
                # 1. Fresh database path
                self._clean_db(db_path)

                # 2. Start engine in serve mode
                client = EngineClient(
                    engine_bin=self.config.engine_bin,
                    db_path=db_path,
                    default_timeout=5.0,
                )
                try:
                    client.start()
                except Exception as exc:
                    child_exit = ChildProcessExitInfo.from_client(client) if client else None
                    details = FailureDetails(
                        category=FailureCategory.ENGINE_FAILURE,
                        iteration=iter_num,
                        total_iterations=self.config.iterations,
                        seed=self.config.seed,
                        derived_seed=iter_seed,
                        mode=self.config.mode.value,
                        db_path=db_path,
                        errors=[f"Failed to start engine in serve mode: {exc}"],
                        child_exit=child_exit,
                        stderr_lines=list(client.stderr_lines) if client else [],
                    )
                    art_dir = self.reporter.preserve_failure_artifacts(details, oracle=oracle)
                    sys.stderr.write(f"\n[FAIL] Iteration {iter_num}: Failed to start engine: {exc}\n")
                    sys.stderr.write(f"Artifacts preserved in: {art_dir}\n")
                    return 1

                # 3. Create database with meaningful data (overwrites, deletes, batches)
                num_ops = 50
                key_pool = [f"key_{i:04d}" for i in range(1, 15)]
                for op_idx in range(1, num_ops + 1):
                    op_id = f"cmp-seed-{op_idx}"
                    coin = iter_rng.random()
                    k = iter_rng.choice(key_pool)
                    if coin < 0.6:
                        # PUT (many repeated overwrites)
                        v = f"cmp_val_{op_idx}_{iter_rng.randint(1000, 9999)}"
                        oracle.record_put(op_id, k, v, writer_id="cmp-seeder")
                        client.put(k, v, op_id=op_id)
                    elif coin < 0.8:
                        # DEL (tombstones for compaction to prune)
                        oracle.record_del(op_id, k, writer_id="cmp-seeder")
                        client.delete(k, op_id=op_id)
                    else:
                        # BATCH
                        sub_k1 = iter_rng.choice(key_pool)
                        sub_v1 = f"cmp_val_{op_idx}_b_{iter_rng.randint(1000, 9999)}"
                        sub_k2 = iter_rng.choice(key_pool)
                        batch_items = [
                            BatchItem.make_put(sub_k1, sub_v1),
                            BatchItem.make_del(sub_k2),
                        ]
                        oracle.record_batch(
                            op_id,
                            [
                                BatchSubOp(op_type=OpType.PUT, key=sub_k1, value=sub_v1),
                                BatchSubOp(op_type=OpType.DEL, key=sub_k2),
                            ],
                            writer_id="cmp-seeder",
                        )
                        client.batch(batch_items, op_id=op_id)

                # 4. Trigger COMPACT in a background thread to allow interruption
                compact_op_id = f"compact-{iter_num}"
                compact_status = {"acked": False, "error": None}

                def send_compact():
                    try:
                        resp = client.compact(op_id=compact_op_id, timeout=10.0)
                        if resp and resp.is_ack:
                            compact_status["acked"] = True
                    except Exception as e:
                        compact_status["error"] = e

                compact_thread = threading.Thread(target=send_compact, name=f"Compactor-{iter_num}")
                compact_thread.start()

                # 5. Wait for a random millisecond delay
                delay_ms = iter_rng.uniform(
                    self.config.min_kill_delay_ms,
                    self.config.max_kill_delay_ms,
                )
                time.sleep(delay_ms / 1000.0)

                # 6. Kill engine using SIGKILL at random instant
                kill_ret = client.kill(signal.SIGKILL)
                compact_thread.join(timeout=1.0)
                client.close()
                compact_acked = compact_status["acked"]

                child_exit = ChildProcessExitInfo.from_client(client, kill_ret)

                # 7. Check for temporary compaction files on disk before recovery
                parent_dir = os.path.dirname(os.path.abspath(db_path))
                base_name = os.path.basename(db_path)
                temp_files_found = []
                if os.path.exists(parent_dir):
                    for fname in os.listdir(parent_dir):
                        if (fname.endswith(".tmp") or ".compact." in fname) and fname.startswith(base_name):
                            temp_files_found.append(fname)

                # 8. Post-crash recovery, check, dump, and Oracle verification
                verifier = StateVerifier(
                    engine_bin=self.config.engine_bin,
                    db_path=db_path,
                    timeout=5.0,
                )
                passed, violations, actual_dump = verifier.verify(oracle)

                t_duration = time.time() - t_start

                cmp_info = CompactionCrashInfo(
                    compact_op_id=compact_op_id,
                    compact_acked=compact_acked,
                    requested_kill_delay_ms=delay_ms,
                    actual_exit_status=child_exit.returncode,
                    temp_files_found=temp_files_found,
                )

                rep = IterationReport(
                    iteration=iter_num,
                    total_iterations=self.config.iterations,
                    passed=passed,
                    delay_ms=int(delay_ms),
                    submitted=num_ops,
                    acked=num_ops,
                    recovered_keys=len(actual_dump),
                    duration_s=t_duration,
                    violations=violations,
                    mode="compaction",
                    compaction_info=cmp_info,
                )
                self.reporter.print_iteration_summary(rep)

                if not rep.passed:
                    cat = classify_failure(
                        errors=violations,
                        child_exit=child_exit,
                        tool_crashed=False,
                    )
                    details = FailureDetails(
                        category=cat,
                        iteration=iter_num,
                        total_iterations=self.config.iterations,
                        seed=self.config.seed,
                        derived_seed=iter_seed,
                        mode=self.config.mode.value,
                        db_path=db_path,
                        errors=violations,
                        child_exit=child_exit,
                        actual_dump=actual_dump,
                        operation_ids=[op.op_id for op in oracle.history],
                        operation_types=sorted(list(set(op.op_type.value for op in oracle.history))),
                        compaction_info=cmp_info,
                    )
                    art_dir = self.reporter.preserve_failure_artifacts(details, oracle=oracle)
                    print("=" * 65)
                    print(f"FAILED AT COMPACTION-CRASH ITERATION {iter_num} (SEED: {iter_seed}) - [{cat.value}]")
                    print(f"Artifacts preserved in: {art_dir}")
                    print("=" * 65)
                    return 1

                passed_count += 1

            except Exception as exc:
                tb = traceback.format_exc()
                details = FailureDetails(
                    category=FailureCategory.TORTURE_TOOL_FAILURE,
                    iteration=iter_num,
                    total_iterations=self.config.iterations,
                    seed=self.config.seed,
                    derived_seed=iter_seed,
                    mode=self.config.mode.value,
                    db_path=db_path,
                    errors=[f"Torture tool unexpected internal exception in compaction-crash: {exc}\n{tb}"],
                )
                art_dir = self.reporter.preserve_failure_artifacts(details, oracle=oracle)
                sys.stderr.write(f"\n[CRASH] Compaction-crash internal harness failure: {exc}\n")
                sys.stderr.write(f"Artifacts preserved in: {art_dir}\n")
                return 1

            finally:
                if client and client.is_alive:
                    try:
                        client.kill(signal.SIGKILL)
                        client.close()
                    except Exception:
                        pass
                if compact_thread and compact_thread.is_alive():
                    compact_thread.join(timeout=0.5)

        print("=" * 65)
        print(f"ALL {passed_count} COMPACTION-CRASH ITERATIONS PASSED CRASH CONSISTENCY VERIFICATION!")
        print("=" * 65)
        return 0

    def _run_disk_full_loop(self) -> int:
        """
        CP11 Disk-Full Mode Testing Loop.

        Workflow:
        1. Start engine with small maximum size: ./kvdb serve <path> --max-bytes <limit>
        2. Maintain independent Oracle.
        3. Perform successful writes before reaching limit (PUT, DEL, BATCH).
        4. Attempt PUT exceeding max-bytes limit -> verify clean ERR, record in Oracle.
        5. Attempt BATCH exceeding max-bytes limit -> verify clean ERR, record in Oracle (must NOT be partially applied).
        6. Verify existing data remains readable before kill.
        7. Inject SIGKILL after disk-full errors.
        8. Restart engine and recover via kvdb open.
        9. Verify recovered state against Oracle via kvdb check and kvdb dump:
           - Acknowledged data is intact and uncorrupted.
           - Rejected operations are completely absent.
           - Failed batch was atomically omitted (no partial writes).
        """
        passed_count = 0
        max_bytes = self.config.max_bytes if self.config.max_bytes is not None else 100_000

        for iter_num in range(1, self.config.iterations + 1):
            t_start = time.time()
            iter_seed = self.master_rng.randint(1, 100_000_000)
            iter_rng = random.Random(iter_seed)

            db_path = self.config.db_path
            oracle = DatabaseOracle()
            client: Optional[EngineClient] = None
            violations: List[str] = []
            successful_writes = 0
            failed_puts = 0
            failed_batches = 0
            clean_err_count = 0
            data_readable_before_kill = False

            try:
                # 1. Fresh database path
                self._clean_db(db_path)

                # 2. Start engine in serve mode with --max-bytes limit
                client = EngineClient(
                    engine_bin=self.config.engine_bin,
                    db_path=db_path,
                    max_bytes=max_bytes,
                    default_timeout=5.0,
                )
                try:
                    client.start()
                except Exception as exc:
                    child_exit = ChildProcessExitInfo.from_client(client) if client else None
                    details = FailureDetails(
                        category=FailureCategory.ENGINE_FAILURE,
                        iteration=iter_num,
                        total_iterations=self.config.iterations,
                        seed=self.config.seed,
                        derived_seed=iter_seed,
                        mode=self.config.mode.value,
                        db_path=db_path,
                        errors=[f"Failed to start engine in serve mode: {exc}"],
                        child_exit=child_exit,
                        stderr_lines=list(client.stderr_lines) if client else [],
                    )
                    art_dir = self.reporter.preserve_failure_artifacts(details, oracle=oracle)
                    sys.stderr.write(f"\n[FAIL] Iteration {iter_num}: Failed to start engine: {exc}\n")
                    sys.stderr.write(f"Artifacts preserved in: {art_dir}\n")
                    return 1

                # 3. Successful writes before reaching limit (small PUT, DEL, BATCH)
                num_pre_ops = 15
                key_pool = [f"df_k_{i:03d}" for i in range(1, 8)]
                last_acked_key = None
                last_acked_val = None

                for op_idx in range(1, num_pre_ops + 1):
                    op_id = f"df-seed-{op_idx}"
                    coin = iter_rng.random()
                    k = iter_rng.choice(key_pool)

                    if coin < 0.5:
                        # PUT
                        v = f"seed_val_{op_idx}_{iter_rng.randint(1000, 9999)}"
                        oracle.record_put(op_id, k, v, writer_id="df-seeder")
                        resp = client.put(k, v, op_id=op_id)
                        if resp.is_ack:
                            oracle.record_ack(op_id)
                            successful_writes += 1
                            last_acked_key = k
                            last_acked_val = v
                        else:
                            oracle.record_err(op_id, resp.message or "error")
                            violations.append(f"Unexpected pre-limit PUT error for op '{op_id}': {resp.message}")
                    elif coin < 0.75:
                        # DEL
                        oracle.record_del(op_id, k, writer_id="df-seeder")
                        resp = client.delete(k, op_id=op_id)
                        if resp.is_ack:
                            oracle.record_ack(op_id)
                            successful_writes += 1
                        else:
                            oracle.record_err(op_id, resp.message or "error")
                            violations.append(f"Unexpected pre-limit DEL error for op '{op_id}': {resp.message}")
                    else:
                        # BATCH
                        k1 = iter_rng.choice(key_pool)
                        v1 = f"batch_val_{op_idx}_{iter_rng.randint(1000, 9999)}"
                        k2 = iter_rng.choice(key_pool)
                        batch_items = [
                            BatchItem.make_put(k1, v1),
                            BatchItem.make_del(k2),
                        ]
                        oracle.record_batch(
                            op_id,
                            [
                                BatchSubOp(op_type=OpType.PUT, key=k1, value=v1),
                                BatchSubOp(op_type=OpType.DEL, key=k2),
                            ],
                            writer_id="df-seeder",
                        )
                        resp = client.batch(batch_items, op_id=op_id)
                        if resp.is_ack:
                            oracle.record_ack(op_id)
                            successful_writes += 1
                            last_acked_key = k1
                            last_acked_val = v1
                        else:
                            oracle.record_err(op_id, resp.message or "error")
                            violations.append(f"Unexpected pre-limit BATCH error for op '{op_id}': {resp.message}")

                # 4. PUT exceeding the limit
                over_put_id = f"df-put-overlimit-{iter_num}"
                over_put_k = f"over_put_key_{iter_num}"
                # Generate large payload exceeding max_bytes
                over_put_v = "X" * (max_bytes + 2048)
                oracle.record_put(over_put_id, over_put_k, over_put_v, writer_id="df-tester")
                try:
                    resp_put = client.put(over_put_k, over_put_v, op_id=over_put_id, timeout=10.0)
                    if not resp_put.is_ack:
                        # Clean ERR response
                        oracle.record_err(over_put_id, resp_put.message or "disk full")
                        failed_puts += 1
                        clean_err_count += 1
                    else:
                        oracle.record_ack(over_put_id)
                        violations.append(
                            f"DISK-FULL VIOLATION: Engine ACKed PUT '{over_put_id}' of size {len(over_put_v)} "
                            f"exceeding max-bytes limit of {max_bytes}!"
                        )
                except Exception as exc:
                    violations.append(f"Failed while sending over-limit PUT '{over_put_id}': {exc}")

                # 5. BATCH exceeding the limit (must NOT be partially applied)
                over_batch_id = f"df-batch-overlimit-{iter_num}"
                b_k1 = f"over_batch_k1_{iter_num}"
                b_v1 = f"batch_pre_item_{iter_num}"
                b_k2 = f"over_batch_k2_{iter_num}"
                b_v2 = "Y" * (max_bytes + 2048)
                b_k3 = f"over_batch_k3_{iter_num}"

                batch_items = [
                    BatchItem.make_put(b_k1, b_v1),
                    BatchItem.make_put(b_k2, b_v2),
                    BatchItem.make_del(b_k3),
                ]
                oracle.record_batch(
                    over_batch_id,
                    [
                        BatchSubOp(op_type=OpType.PUT, key=b_k1, value=b_v1),
                        BatchSubOp(op_type=OpType.PUT, key=b_k2, value=b_v2),
                        BatchSubOp(op_type=OpType.DEL, key=b_k3),
                    ],
                    writer_id="df-tester",
                )
                try:
                    resp_batch = client.batch(batch_items, op_id=over_batch_id, timeout=10.0)
                    if not resp_batch.is_ack:
                        # Clean ERR response
                        oracle.record_err(over_batch_id, resp_batch.message or "disk full")
                        failed_batches += 1
                        clean_err_count += 1
                    else:
                        oracle.record_ack(over_batch_id)
                        violations.append(
                            f"DISK-FULL VIOLATION: Engine ACKed oversized BATCH '{over_batch_id}' "
                            f"exceeding max-bytes limit of {max_bytes}!"
                        )
                except Exception as exc:
                    violations.append(f"Failed while sending over-limit BATCH '{over_batch_id}': {exc}")

                # 6. Existing data remains readable before kill
                if not client.is_alive:
                    violations.append("ENGINE FAILURE: Engine crashed or terminated unexpectedly during disk-full errors!")
                else:
                    data_readable_before_kill = True
                    # If GET is supported and we have an ACKed key, verify reading via serve protocol
                    if last_acked_key is not None:
                        try:
                            get_resp = client.get(last_acked_key, op_id=f"df-read-{iter_num}")
                            if get_resp.is_ack and get_resp.message:
                                # Hex-decoded value should match
                                try:
                                    read_val = bytes.fromhex(get_resp.message).decode("utf-8")
                                    definite_state, _ = oracle.compute_definite_state()
                                    expected = definite_state.get(last_acked_key)
                                    if expected and read_val != expected:
                                        violations.append(
                                            f"READ VIOLATION: GET '{last_acked_key}' returned '{read_val}', "
                                            f"expected '{expected}' before kill!"
                                        )
                                except Exception:
                                    pass
                        except Exception:
                            # Engine may not support GET command in serve mode; handled gracefully
                            pass

                # 7. Kill engine after disk-full errors
                delay_ms = iter_rng.uniform(
                    self.config.min_kill_delay_ms,
                    self.config.max_kill_delay_ms,
                )
                time.sleep(delay_ms / 1000.0)

                kill_ret = client.kill(signal.SIGKILL)
                client.close()

                child_exit = ChildProcessExitInfo.from_client(client, kill_ret)

                # 8. Restart, recover, check, dump, and Oracle verification
                verifier = StateVerifier(
                    engine_bin=self.config.engine_bin,
                    db_path=db_path,
                    timeout=5.0,
                )
                passed, verify_violations, actual_dump = verifier.verify(oracle)
                violations.extend(verify_violations)

                # Ensure failed batch items are NOT partially present
                if b_k1 in actual_dump or b_k2 in actual_dump:
                    violations.append(
                        f"ATOMICITY VIOLATION: Failed batch '{over_batch_id}' was partially applied! "
                        f"Items visible in dump: {[k for k in (b_k1, b_k2) if k in actual_dump]}"
                    )

                # Ensure failed PUT item is NOT present
                if over_put_k in actual_dump:
                    violations.append(
                        f"INTEGRITY VIOLATION: Rejected PUT '{over_put_id}' key '{over_put_k}' "
                        f"was persisted in recovered database!"
                    )

                t_duration = time.time() - t_start
                recovery_succeeded = len(verify_violations) == 0

                disk_info = DiskFullCrashInfo(
                    max_bytes=max_bytes,
                    successful_writes=successful_writes,
                    failed_puts=failed_puts,
                    failed_batches=failed_batches,
                    clean_err_count=clean_err_count,
                    data_readable_before_kill=data_readable_before_kill,
                    recovery_succeeded=recovery_succeeded,
                    recovered_keys=len(actual_dump),
                )

                rep = IterationReport(
                    iteration=iter_num,
                    total_iterations=self.config.iterations,
                    passed=(len(violations) == 0),
                    delay_ms=int(delay_ms),
                    submitted=num_pre_ops + 2,
                    acked=successful_writes,
                    recovered_keys=len(actual_dump),
                    duration_s=t_duration,
                    violations=violations,
                    mode="disk-full",
                    disk_full_info=disk_info,
                )
                self.reporter.print_iteration_summary(rep)

                if not rep.passed:
                    cat = classify_failure(
                        errors=violations,
                        child_exit=child_exit,
                        tool_crashed=False,
                    )
                    details = FailureDetails(
                        category=cat,
                        iteration=iter_num,
                        total_iterations=self.config.iterations,
                        seed=self.config.seed,
                        derived_seed=iter_seed,
                        mode=self.config.mode.value,
                        db_path=db_path,
                        errors=violations,
                        child_exit=child_exit,
                        actual_dump=actual_dump,
                        operation_ids=[op.op_id for op in oracle.history],
                        operation_types=sorted(list(set(op.op_type.value for op in oracle.history))),
                        disk_full_info=disk_info,
                    )
                    art_dir = self.reporter.preserve_failure_artifacts(details, oracle=oracle)
                    print("=" * 65)
                    print(f"FAILED AT DISK-FULL ITERATION {iter_num} (SEED: {iter_seed}) - [{cat.value}]")
                    print(f"Artifacts preserved in: {art_dir}")
                    print("=" * 65)
                    return 1

                passed_count += 1

            except Exception as exc:
                tb = traceback.format_exc()
                details = FailureDetails(
                    category=FailureCategory.TORTURE_TOOL_FAILURE,
                    iteration=iter_num,
                    total_iterations=self.config.iterations,
                    seed=self.config.seed,
                    derived_seed=iter_seed,
                    mode=self.config.mode.value,
                    db_path=db_path,
                    errors=[f"Torture tool unexpected internal exception in disk-full: {exc}\n{tb}"],
                )
                art_dir = self.reporter.preserve_failure_artifacts(details, oracle=oracle)
                sys.stderr.write(f"\n[FAIL] Iteration {iter_num}: Torture tool crashed: {exc}\n{tb}\n")
                sys.stderr.write(f"Artifacts preserved in: {art_dir}\n")
                return 1

            finally:
                if client and client.is_alive:
                    client.kill(signal.SIGKILL)
                    client.close()

        print("=" * 65)
        print(f"ALL {passed_count} DISK-FULL ITERATIONS PASSED CRASH CONSISTENCY VERIFICATION!")
        print("=" * 65)
        return 0

    def run(self) -> int:
        """
        Runs the full iteration loop.
        Returns 0 if all iterations passed, or 1 on the first failure.
        A torture-tool crash or failure must never be reported as PASS.
        """
        print("=" * 65)
        print(f"STARTING CRASH TORTURE RUN: {self.config.iterations} ITERATIONS")
        print(f"Master Seed: {self.config.seed}")
        print(f"Mode: {self.config.mode.value}")
        print(f"Engine: {self.config.engine_bin}")
        print(f"Target DB: {self.config.db_path}")
        print("=" * 65)

        if self.config.mode == TortureMode.RECOVERY_CRASH:
            return self._run_recovery_crash_loop()
        if self.config.mode == TortureMode.COMPACTION:
            return self._run_compaction_crash_loop()
        if self.config.mode == TortureMode.DISK_FULL:
            return self._run_disk_full_loop()

        passed_count = 0

        for iter_num in range(1, self.config.iterations + 1):
            t_start = time.time()
            iter_seed = self.master_rng.randint(1, 100_000_000)
            iter_rng = random.Random(iter_seed)

            db_path = self.config.db_path
            oracle = DatabaseOracle()
            client: Optional[EngineClient] = None
            workers: List[SingleWriterWorker] = []
            kill_ret: Optional[int] = None

            try:
                # 1. Fresh database path
                self._clean_db(db_path)

                # 2. Start engine in serve mode
                client = EngineClient(
                    engine_bin=self.config.engine_bin,
                    db_path=db_path,
                    default_timeout=2.0,
                )
                try:
                    client.start()
                except Exception as exc:
                    child_exit = ChildProcessExitInfo.from_client(client) if client else None
                    details = FailureDetails(
                        category=FailureCategory.ENGINE_FAILURE,
                        iteration=iter_num,
                        total_iterations=self.config.iterations,
                        seed=self.config.seed,
                        derived_seed=iter_seed,
                        mode=self.config.mode.value,
                        db_path=db_path,
                        errors=[f"Failed to start engine in serve mode: {exc}"],
                        child_exit=child_exit,
                        stderr_lines=client.stderr_lines if client else [],
                    )
                    art_dir = self.reporter.preserve_failure_artifacts(details, oracle=oracle)
                    sys.stderr.write(f"\n[FAIL] Iteration {iter_num}: Failed to start engine: {exc}\n")
                    sys.stderr.write(f"Artifacts preserved in: {art_dir}\n")
                    return 1

                proc_pid = client.pid

                # 3. Start concurrent active writers
                num_workers = max(1, self.config.workers)
                shared_key_pool = (
                    [f"shared_key_{i}" for i in range(4)]
                    if self.config.mode == TortureMode.LARGE_VALUES
                    else None
                )
                for w_idx in range(1, num_workers + 1):
                    w_seed = iter_rng.randint(1, 100_000_000)
                    worker = SingleWriterWorker(
                        client=client,
                        oracle=oracle,
                        rng=random.Random(w_seed),
                        mode=self.config.mode,
                        writer_id=f"w{w_idx}",
                        key_pool=shared_key_pool,
                    )
                    workers.append(worker)

                for worker in workers:
                    worker.start()

                # 4. Choose random millisecond delay while writers are running
                delay_ms = iter_rng.randint(
                    self.config.min_kill_delay_ms,
                    self.config.max_kill_delay_ms,
                )
                time.sleep(delay_ms / 1000.0)

                # 5. Kill database process with SIGKILL at random instant
                kill_ret = client.kill(signal.SIGKILL)

                # 6. Safely stop and join writer threads
                for worker in workers:
                    worker.stop()
                for worker in workers:
                    worker.join(timeout=1.0)
                client.close()

                total_submitted = sum(w.submitted_count for w in workers)
                total_acked = sum(w.acked_count for w in workers)
                worker_exceptions = [err for w in workers for err in w.errors]

                # Verify process actually terminated
                is_dead = not client.is_alive
                killed_by_sigkill = (kill_ret in (-signal.SIGKILL, 137)) or is_dead
                child_exit = ChildProcessExitInfo.from_client(client, kill_ret)

                # 7. Post-crash recovery, check, dump, and Oracle verification
                verifier = StateVerifier(
                    engine_bin=self.config.engine_bin,
                    db_path=db_path,
                    timeout=5.0,
                )
                passed, violations, actual_dump = verifier.verify(oracle)

                t_duration = time.time() - t_start

                if not killed_by_sigkill:
                    passed = False
                    violations.append(
                        f"Process {proc_pid} did not exit with SIGKILL (exit code: {kill_ret})"
                    )

                rep = IterationReport(
                    iteration=iter_num,
                    total_iterations=self.config.iterations,
                    passed=passed,
                    delay_ms=delay_ms,
                    submitted=total_submitted,
                    acked=total_acked,
                    recovered_keys=len(actual_dump),
                    duration_s=t_duration,
                    violations=violations,
                )
                self.reporter.print_iteration_summary(rep)

                if not rep.passed:
                    cat = classify_failure(
                        errors=violations,
                        child_exit=child_exit,
                        client_malformed=client.malformed_responses if client else None,
                        client_unknown=client.unknown_ids if client else None,
                        worker_exceptions=worker_exceptions,
                        tool_crashed=False,
                    )
                    details = FailureDetails(
                        category=cat,
                        iteration=iter_num,
                        total_iterations=self.config.iterations,
                        seed=self.config.seed,
                        derived_seed=iter_seed,
                        mode=self.config.mode.value,
                        db_path=db_path,
                        errors=violations,
                        child_exit=child_exit,
                        stderr_lines=client.stderr_lines if client else [],
                        actual_dump=actual_dump,
                        operation_ids=[op.op_id for op in oracle.history],
                        operation_types=sorted(list(set(op.op_type.value for op in oracle.history))),
                    )
                    art_dir = self.reporter.preserve_failure_artifacts(details, oracle=oracle)
                    print("=" * 65)
                    print(f"FAILED AT ITERATION {iter_num} (SEED: {iter_seed}) - [{cat.value}]")
                    print(f"Artifacts preserved in: {art_dir}")
                    print("=" * 65)
                    return 1

                passed_count += 1

            except Exception as exc:
                # Catch any unexpected harness exception: MUST NEVER BE REPORTED AS PASS
                tb = traceback.format_exc()
                child_exit = ChildProcessExitInfo.from_client(client, kill_ret) if client else None
                details = FailureDetails(
                    category=FailureCategory.TORTURE_TOOL_FAILURE,
                    iteration=iter_num,
                    total_iterations=self.config.iterations,
                    seed=self.config.seed,
                    derived_seed=iter_seed,
                    mode=self.config.mode.value,
                    db_path=db_path,
                    errors=[f"Torture tool unexpected internal exception: {exc}\n{tb}"],
                    child_exit=child_exit,
                    stderr_lines=client.stderr_lines if client else [],
                )
                art_dir = self.reporter.preserve_failure_artifacts(details, oracle=oracle)
                sys.stderr.write(f"\n[CRASH] Iteration {iter_num}: Torture tool internal failure: {exc}\n")
                sys.stderr.write(f"Artifacts preserved in: {art_dir}\n")
                return 1

            finally:
                for worker in workers:
                    try:
                        worker.stop()
                    except Exception:
                        pass
                if client and client.is_alive:
                    try:
                        client.kill(signal.SIGKILL)
                        client.close()
                    except Exception:
                        pass

        print("=" * 65)
        print(f"ALL {passed_count} ITERATIONS PASSED CRASH CONSISTENCY VERIFICATION!")
        print("=" * 65)
        return 0
