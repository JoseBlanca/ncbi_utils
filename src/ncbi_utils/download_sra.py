import tempfile
from pathlib import Path
from subprocess import run, CalledProcessError

PREFETCH_BIN = "prefetch"
VALIDATE_BIN = "vdb-validate"
FASTERQ_DUMP_BIN = "fasterq-dump"
GZIP_BIN = "gzip"


def download_fastq_from_sra(
    run_acc, out_dir, temp_dir: None | Path = None, fasterq_dump_num_threads=6
):
    out_dir = Path(out_dir)
    if not out_dir.exists():
        raise ValueError(f"out_dir should exist: {out_dir}")
    if not out_dir.is_dir():
        raise ValueError(
            f"out_dir should be a directory, but the given one is not: {out_dir}"
        )

    previous_downloaded_files = [
        str(path) for path in out_dir.iterdir() if path.name.startswith(run_acc)
    ]
    if previous_downloaded_files:
        msg = "There are previous downloaded files for this run: " + ",".join(
            previous_downloaded_files
        )
        raise RuntimeError(msg)

    with tempfile.TemporaryDirectory(
        dir=temp_dir, prefix="sra_download_"
    ) as working_dir:
        working_dir_path = Path(working_dir)
        cmd = [PREFETCH_BIN, "-O", str(working_dir_path), run_acc]
        try:
            run(cmd, check=True, capture_output=True)
        except CalledProcessError:
            msg = (
                f"There was an error prefetching the accession {run_acc}, the command was: "
                + " ".join(cmd)
            )
            raise CalledProcessError(msg)

        sra_dir = working_dir_path / run_acc
        cmd = [VALIDATE_BIN, str(sra_dir)]
        try:
            run(cmd, check=True, capture_output=True)
        except CalledProcessError:
            msg = (
                f"There was an error validating the prefetched accession {run_acc}, the command was: "
                + " ".join(cmd)
            )
            raise CalledProcessError(msg)

        fast_out_dir = working_dir_path / "fast"

        cmd = [
            FASTERQ_DUMP_BIN,
            "--outdir",
            str(fast_out_dir),
            "--temp",
            str(working_dir_path),
            "--split-3",
            "--threads",
            str(fasterq_dump_num_threads),
            "--skip-technical",
            "--seq-defline",
            r"@$ac.$si.$ri:$sg:$sn",
            str(sra_dir),
        ]
        try:
            run(cmd, check=True, capture_output=True)
        except CalledProcessError:
            msg = (
                f"There was an error doing fasterq_dump the accession {run_acc}, the command was: "
                + " ".join(cmd)
            )
            raise CalledProcessError(msg)

        for path in fast_out_dir.iterdir():
            cmd = [GZIP_BIN, path]
            run(cmd, check=True)
        for path in fast_out_dir.iterdir():
            cmd = ["mv", str(path), str(out_dir)]
            run(cmd)
