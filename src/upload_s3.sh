#!/usr/bin/env bash
# Envoie les CSV bruts de raw/ vers S3 (couche Bronze), sans les modifier
set -euo pipefail
BUCKET="${BUCKET:-$KUBERNETES_NAMESPACE}"
DEST="s3://${BUCKET}/raw/football-data/"
aws s3 cp raw/ "$DEST" --recursive --endpoint-url "https://$AWS_S3_ENDPOINT"
aws s3 ls "$DEST" --endpoint-url "https://$AWS_S3_ENDPOINT" --human-readable --summarize