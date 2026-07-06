#!/usr/bin/env bash
set -euo pipefail

RPC_URL="${RPC_URL:-http://127.0.0.1:18232/}"
OUTPUT_FILE="${OUTPUT_FILE:-nullifiers.txt}"
START_HEIGHT="${START_HEIGHT:-0}"
PROGRESS_INTERVAL="${PROGRESS_INTERVAL:-100}"

if ! command -v jq >/dev/null 2>&1; then
  echo "Error: jq is required to parse RPC responses." >&2
  exit 1
fi

if ! [[ "$START_HEIGHT" =~ ^[0-9]+$ ]]; then
  echo "Error: START_HEIGHT must be a non-negative integer." >&2
  exit 1
fi

if ! [[ "$PROGRESS_INTERVAL" =~ ^[0-9]+$ ]] || [[ "$PROGRESS_INTERVAL" -eq 0 ]]; then
  echo "Error: PROGRESS_INTERVAL must be a positive integer." >&2
  exit 1
fi

rpc_call() {
  local method="$1"
  local params="$2"

  curl --silent --show-error \
    --data-binary "{\"jsonrpc\":\"1.0\",\"id\":\"nullifier-scan\",\"method\":\"${method}\",\"params\":${params}}" \
    -H 'Content-type: application/json' \
    "$RPC_URL"
}

json_error_message() {
  jq -r '.error.message // empty'
}

json_has_rpc_error() {
  jq -e '.error != null' >/dev/null
}

json_nullifiers() {
  jq -r '.. | objects | .nullifier? | select(. != null) | if type == "array" then .[] else . end'
}

tmp_file="$(mktemp "${OUTPUT_FILE}.tmp.XXXXXX")"
trap 'rm -f "$tmp_file"' EXIT

height="$START_HEIGHT"
blocks_scanned=0
transactions_scanned=0
nullifiers_found=0
stop_reason=""

: >"$tmp_file"

while :; do
  block_response="$(rpc_call getblock "[\"${height}\"]")"

  if printf '%s' "$block_response" | json_has_rpc_error; then
    error_message="$(printf '%s' "$block_response" | json_error_message)"
    if [[ "$error_message" == *"block height not in best chain"* ]]; then
      stop_reason="$error_message"
      break
    fi

    echo "Error fetching block ${height}: ${error_message:-unknown RPC error}" >&2
    exit 1
  fi

  mapfile -t tx_hashes < <(printf '%s' "$block_response" | jq -r '.result.tx[]?')
  blocks_scanned=$((blocks_scanned + 1))

  for tx_hash in "${tx_hashes[@]}"; do
    tx_response="$(rpc_call getrawtransaction "[\"${tx_hash}\",1]")"

    if printf '%s' "$tx_response" | json_has_rpc_error; then
      error_message="$(printf '%s' "$tx_response" | json_error_message)"
      echo "Error fetching transaction ${tx_hash} at block ${height}: ${error_message:-unknown RPC error}" >&2
      exit 1
    fi

    while IFS= read -r nullifier; do
      [[ -n "$nullifier" ]] || continue
      if [[ "$nullifiers_found" -gt 0 ]]; then
        printf ',' >>"$tmp_file"
      fi
      printf '%s' "$nullifier" >>"$tmp_file"
      nullifiers_found=$((nullifiers_found + 1))
    done < <(printf '%s' "$tx_response" | json_nullifiers)

    transactions_scanned=$((transactions_scanned + 1))
  done

  if (( blocks_scanned % PROGRESS_INTERVAL == 0 )); then
    echo "Scanned ${blocks_scanned} blocks through height ${height}; transactions: ${transactions_scanned}; nullifiers: ${nullifiers_found}"
  fi

  height=$((height + 1))
done

mv "$tmp_file" "$OUTPUT_FILE"
trap - EXIT

file_size_bytes="$(wc -c <"$OUTPUT_FILE" | tr -d '[:space:]')"

echo "Completed nullifier scan"
echo "Stop reason: ${stop_reason}"
echo "Start height: ${START_HEIGHT}"
echo "Last requested height: ${height}"
echo "Blocks scanned: ${blocks_scanned}"
echo "Transactions scanned: ${transactions_scanned}"
echo "Nullifiers collected: ${nullifiers_found}"
echo "Output file: ${OUTPUT_FILE}"
echo "Output size: ${file_size_bytes} bytes"
