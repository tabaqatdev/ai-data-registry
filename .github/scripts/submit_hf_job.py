# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "huggingface-hub>=1.8.0",
# ]
# ///
"""Submit a workspace pipeline to HuggingFace Jobs and wait for completion.

Reads configuration from environment variables set by extract-huggingface.yml:
- HF_TOKEN: HuggingFace API token
- HF_JOB_NAMESPACE: Organization namespace
- HF_JOB_IMAGE: Docker image URL
- HF_JOB_FLAVOR: Hardware flavor (e.g., a10g-large)
- HF_JOB_WORKSPACE: Workspace name

Passes S3 credentials and workspace secrets to the container as environment
variables so the Docker image can write directly to S3.

Note: This breaks the write-isolation pattern used by GitHub/Hetzner backends.
This is an accepted trade-off because HF containers run on external
infrastructure without workflow-level upload steps.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.registry_config import (
    WORKSPACE_NAME_RE,
    get_default_storage_name,
    resolve_storage_env,
)


def main():
    # Required env vars
    token = os.environ.get("HF_TOKEN")
    if not token:
        print("ERROR: HF_TOKEN not set. Add it as a repository secret.")
        sys.exit(1)

    image = os.environ.get("HF_JOB_IMAGE")
    flavor = os.environ.get("HF_JOB_FLAVOR", "a10g-large")
    namespace = os.environ.get("HF_JOB_NAMESPACE")
    workspace = os.environ.get("HF_JOB_WORKSPACE")

    if not image:
        print("ERROR: HF_JOB_IMAGE not set.")
        sys.exit(1)

    if not workspace:
        print("ERROR: HF_JOB_WORKSPACE not set.")
        sys.exit(1)

    if not WORKSPACE_NAME_RE.match(workspace):
        print(f"ERROR: Invalid workspace name: {workspace}")
        sys.exit(1)

    from huggingface_hub import run_job

    # Build environment variables for the container
    env = {
        "WORKSPACE": workspace,
        "OUTPUT_DIR": "/output",
    }

    # Pass repo/branch prefix info so the container can construct correct S3 paths
    for var in ["GITHUB_REPOSITORY", "GITHUB_REF_NAME"]:
        val = os.environ.get(var)
        if val:
            env[var] = val

    # Pass S3 credentials so the container can upload directly.
    # Resolve from registry config so multi-storage secret names work correctly.
    secrets = {}
    try:
        storage_name = get_default_storage_name()
        creds = resolve_storage_env(storage_name)
        if creds["endpoint_url"]:
            secrets["S3_ENDPOINT_URL"] = creds["endpoint_url"]
        if creds["bucket"]:
            secrets["S3_BUCKET"] = creds["bucket"]
        if creds["region"]:
            secrets["S3_REGION"] = creds["region"]
        if creds["access_key"]:
            secrets["S3_WRITE_KEY_ID"] = creds["access_key"]
            secrets["AWS_ACCESS_KEY_ID"] = creds["access_key"]
        if creds["secret_key"]:
            secrets["S3_WRITE_SECRET"] = creds["secret_key"]
            secrets["AWS_SECRET_ACCESS_KEY"] = creds["secret_key"]
    except (ValueError, KeyError) as e:
        print(f"WARNING: Could not resolve storage credentials: {e}")

    # Pass workspace-specific secrets
    ws_api_key = os.environ.get("WORKSPACE_SECRET_API_KEY")
    if ws_api_key:
        secrets["WORKSPACE_SECRET_API_KEY"] = ws_api_key

    print(f"Submitting HF Job:")
    print(f"  Image: {image}")
    print(f"  Flavor: {flavor}")
    print(f"  Workspace: {workspace}")
    print(f"  Namespace: {namespace or '(default)'}")

    # Submit the job
    job = run_job(
        image=image,
        command=["python", "main.py"],
        flavor=flavor,
        env=env,
        secrets=secrets,
        timeout="2h",
        labels={"workspace": workspace, "source": "ai-data-registry"},
        namespace=namespace,
        token=token,
    )

    print(f"  Job ID: {job.id}")
    print(f"  Job URL: {job.url}")

    # Poll for completion
    poll_interval = 30  # seconds
    max_polls = 240  # 2 hours at 30s intervals
    polls = 0

    while polls < max_polls:
        time.sleep(poll_interval)
        polls += 1

        try:
            from huggingface_hub import inspect_job

            info = inspect_job(job_id=job.id, token=token)
            stage = info.status.stage if hasattr(info, "status") else "unknown"

            if stage in ("COMPLETED",):
                print(f"  Job completed successfully after ~{polls * poll_interval}s.")
                return
            elif stage in ("ERROR", "CANCELED"):
                msg = info.status.message if hasattr(info.status, "message") else ""
                print(f"  Job failed with stage: {stage}. {msg}")
                sys.exit(1)
            else:
                if polls % 4 == 0:  # Log every 2 minutes
                    print(f"  Status: {stage} ({polls * poll_interval}s elapsed)")

        except Exception as e:
            print(f"  Warning: Failed to check job status: {e}")

    print(f"  ERROR: Job timed out after {max_polls * poll_interval}s.")
    sys.exit(1)


if __name__ == "__main__":
    main()
