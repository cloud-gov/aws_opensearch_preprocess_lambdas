"""
Org/space S3 partitioning shared by both transform Lambdas.

Each Lambda is deployed as a flat set of `.py` files at the root of its zip, so
this module is imported as a top-level `org_partitioning` rather than as part of
the `lambda_functions` package. The Terraform that builds each zip must include
this file alongside the handler; see `lambda.tf` in the metrics_s3_ingestor and
cloudwatch_s3_ingestor modules.
"""

import gzip
import hashlib
import io
import json
import logging
import re
from datetime import datetime

logger = logging.getLogger()

# Tag names must match cf-tags.conf, which renames them to @cf.org_id and
# @cf.space_id downstream.
ORG_GUID_TAG = "Organization GUID"
SPACE_GUID_TAG = "Space GUID"

SAFE_KEY_SEGMENT = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")

UNKNOWN_ORG_PARTITION = "unknown-org"
UNKNOWN_SPACE_PARTITION = "unknown-space"
ORG_KEY_NAMESPACE = "orgs"


def partition_for(entry, source=None):
    """
    Returns the (org, space) S3 partition for one enriched record. `source` only
    names the record in warnings.
    """
    tags = entry.get("Tags", {})
    return (
        guid_segment(
            tags.get(ORG_GUID_TAG), ORG_GUID_TAG, UNKNOWN_ORG_PARTITION, source
        ),
        guid_segment(
            tags.get(SPACE_GUID_TAG), SPACE_GUID_TAG, UNKNOWN_SPACE_PARTITION, source
        ),
    )


def guid_segment(value, tag, fallback, source):
    """
    Returns a tag value safe to use as one S3 key segment, else `fallback`.
    """
    if not value:
        logger.warning(
            "Missing %s tag for %s; using %s partition", tag, source, fallback
        )
        return fallback
    if not isinstance(value, str) or not SAFE_KEY_SEGMENT.fullmatch(value):
        logger.warning(
            "Unsafe %s value for %s; using %s partition", tag, source, fallback
        )
        return fallback
    return value


def batch_time(entries):
    """
    Returns the datetime for a batch's date prefix, taken from the newest entry
    timestamp (epoch ms) so the key does not depend on when the Lambda ran.
    Falls back to now if no entry carries a usable timestamp.
    """
    stamps = [
        e.get("timestamp")
        for e in entries
        if isinstance(e.get("timestamp"), (int, float))
        and not isinstance(e.get("timestamp"), bool)
    ]
    if not stamps:
        logger.warning("No usable timestamp in batch; date prefix will not be stable")
        return datetime.now()
    return datetime.fromtimestamp(max(stamps) / 1000)


def body_digest(raw):
    """
    Returns the key suffix for a batch body. Content-addressed so that a retry
    of the same batch overwrites its object instead of adding a duplicate.
    """
    return hashlib.sha256(raw).hexdigest()[:32]


def build_key(partition, name_prefix, digest, written_at):
    """
    Builds the S3 key for one partition:

        orgs/<org>/<space>/<YYYY>/<MM>/<DD>/<HH>/<name_prefix>-<epoch>-<digest>.json.gz

    A bad org GUID goes to unknown-org/ instead, so everything under orgs/ is a
    real org. Every component derives from the batch content, so the key is
    stable across retries.
    """
    org_guid, space_guid = partition
    date_path = written_at.strftime("%Y/%m/%d/%H")
    epoch = int(written_at.timestamp())
    if org_guid == UNKNOWN_ORG_PARTITION:
        prefix = UNKNOWN_ORG_PARTITION
    else:
        prefix = f"{ORG_KEY_NAMESPACE}/{org_guid}"
    key_path =  f"{prefix}/{space_guid}/{date_path}/"
    name = f"{name_prefix}-{epoch}-{digest}.json.gz"
    return key_path + name


def put_partition(s3_client, bucket, partition, entries, name_prefix="batch"):
    """
    Writes one gzipped NDJSON object for a single partition.
    """
    raw = b"".join((json.dumps(entry) + "\n").encode("utf-8") for entry in entries)
    buffer = io.BytesIO()
    # mtime=0 keeps the compressed bytes identical for identical input.
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as gz_file:
        gz_file.write(raw)
    s3_key = build_key(partition, name_prefix, body_digest(raw), batch_time(entries))
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/gzip",
        ContentEncoding="gzip",
        ServerSideEncryption="AES256",
    )
    logger.info(f"Successfully pushed {len(entries)} records to S3: {s3_key}")
    return s3_key


def put_all_partitions(s3_client, bucket, groups, name_prefix="batch"):
    """
    Writes one object per partition.

    Every partition is attempted even if an earlier one fails, so a single bad
    partition cannot strand the others. If any failed, raises afterwards so the
    caller fails the batch and Firehose retries it; keys are content-addressed,
    so re-writing the partitions that already succeeded is a no-op.
    """
    failed = []
    for partition, entries in sorted(groups.items()):
        try:
            put_partition(s3_client, bucket, partition, entries, name_prefix)
        except Exception as e:
            logger.error("Failed to push partition %s to S3: %s", partition, str(e))
            failed.append(partition)
    if failed:
        raise RuntimeError(
            f"Failed to push {len(failed)} of {len(groups)} partitions to S3: {failed}"
        )
