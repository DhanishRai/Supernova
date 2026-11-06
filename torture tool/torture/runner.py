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
import sys
import time
from typing import Optional

from torture.config import TortureConfig, TortureMode
from torture.oracle import DatabaseOracle
from torture.protocol import EngineClient
from torture.reporting import IterationReport, Reporter
from torture.verifier import StateVerifier
from torture.worker import SingleWriterWorker


class CrashTestRunner:
    """Orchestrates single-writer random-time crash consistency testing."""

    def __init__(self, config: TortureConfig, reporter: Optional[Reporter] = None) -> None:
        self.config = config
        self.reporter = reporter or Reporter()
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

    def run(self) -> int:
        """
        Runs the full iteration loop.
        Returns 0 if all iterations passed, or 1 on the first failure.
        """
        print("=" * 65)
        print(f"STARTING CRASH TORTURE RUN: {self.config.iterations} ITERATIONS")
        print(f"Master Seed: {self.config.seed}")
        print(f"Mode: {self.config.mode.value}")
        print(f"Engine: {self.config.engine_bin}")
        print(f"Target DB: {self.config.db_path}")
        print("=" * 65)

        passed_count = 0

        for iter_num in range(1, self.config.iterations + 1):
            t_start = time.time()
            iter_seed = self.master_rng.randint(1, 100_000_000)
            iter_rng = random.Random(iter_seed)

            # 1. Fresh database path
            db_path = self.config.db_path
            self._clean_db(db_path)

            # 2. Fresh Oracle
            oracle = DatabaseOracle()

            # 3. Start engine in serve mode
            client = EngineClient(
                engine_bin=self.config.engine_bin,
                db_path=db_path,
                default_timeout=2.0,
            )
            try:
                client.start()
            except Exception as exc:
                sys.stderr.write(f"\n[FAIL] Iteration {iter_num}: Failed to start engine: {exc}\n")
                return 1

            proc_pid = client.pid

            # 4. Start concurrent active writers
            num_workers = max(1, self.config.workers)
            shared_key_pool = (
                [f"shared_key_{i}" for i in range(4)]
                if self.config.mode == TortureMode.LARGE_VALUES
                else None
            )
            workers = []
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

            # 5. Choose random millisecond delay while writers are running
            delay_ms = iter_rng.randint(
                self.config.min_kill_delay_ms,
                self.config.max_kill_delay_ms,
            )
            time.sleep(delay_ms / 1000.0)

            # 6. Kill database process with SIGKILL at random instant
            kill_ret = client.kill(signal.SIGKILL)

            # 7. Safely stop and join writer threads
            for worker in workers:
                worker.stop()
            for worker in workers:
                worker.join(timeout=1.0)
            client.close()

            total_submitted = sum(w.submitted_count for w in workers)
            total_acked = sum(w.acked_count for w in workers)

            # Verify process actually terminated
            is_dead = not client.is_alive
            # On Linux, process killed by SIGKILL has return code -9 or 137
            killed_by_sigkill = (kill_ret in (-signal.SIGKILL, 137)) or is_dead

            # 8. Post-crash recovery, check, dump, and Oracle verification
            verifier = StateVerifier(
                engine_bin=self.config.engine_bin,
                db_path=db_path,
                timeout=5.0,
            )
            passed, violations, actual_dump = verifier.verify(oracle)

            t_duration = time.time() - t_start

            rep = IterationReport(
                iteration=iter_num,
                total_iterations=self.config.iterations,
                passed=passed and killed_by_sigkill,
                delay_ms=delay_ms,
                submitted=total_submitted,
                acked=total_acked,
                recovered_keys=len(actual_dump),
                duration_s=t_duration,
                violations=violations,
            )
            self.reporter.print_iteration_summary(rep)

            if not rep.passed:
                # Stop immediately at first genuine failure and preserve artifacts
                if not killed_by_sigkill:
                    violations.append(
                        f"Process {proc_pid} did not exit with SIGKILL (exit code: {kill_ret})"
                    )

                self.reporter.preserve_failure_artifacts(
                    iteration=iter_num,
                    seed=iter_seed,
                    db_path=db_path,
                    oracle=oracle,
                    actual_dump=actual_dump,
                    errors=violations,
                )
                print("=" * 65)
                print(f"FAILED AT ITERATION {iter_num} (SEED: {iter_seed})")
                print("=" * 65)
                return 1

            passed_count += 1

        print("=" * 65)
        print(f"ALL {passed_count} ITERATIONS PASSED CRASH CONSISTENCY VERIFICATION!")
        print("=" * 65)
        return 0
