[OPEN]

# Debug Session: download-blob-metadata

## Symptom
- Download fails with: `no valid blob metadata could be retrieved from any storage node`

## Expected
- Clicking blob download should read the blob from Walrus and save it into the configured vault folder.

## Hypotheses
- H1: The `blobId` passed to `readBlob()` is still not a valid Walrus blob ID.
- H2: The blob exists on-chain, but storage nodes do not currently serve valid metadata for it.
- H3: The active Sui/Walrus network or storage-node routing is mismatched.
- H4: The listed object is not a standard downloadable blob object for `readBlob()`.

## Instrumentation Plan
- Log the raw `blob_id` field extracted from chain objects.
- Log the normalized `blobId` used by the UI.
- Log the exact identifier used during download.
- Log success/failure from `getBlobMetadata()` before `readBlob()`.

## Status
- Evidence collected:
  - `readBlob(blobId)` still fails for the selected on-chain blob.
  - Official `by-object-id` fallback reaches the aggregator but returns HTTP 503.
- Interim conclusion:
  - The issue is no longer a UI click problem.
  - The current failure is consistent with aggregator availability / per-endpoint data availability.
- Next fix:
  - Retry download across multiple public aggregators before surfacing the failure to the user.
