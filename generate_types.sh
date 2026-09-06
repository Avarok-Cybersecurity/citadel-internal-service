#!/bin/bash

# Automated TypeScript Type Generation Script
# This script builds the Rust crate and generates TypeScript types automatically

set -e  # Exit on any error

# Detect OS for sed compatibility
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    SED_INPLACE="sed -i ''"
else
    # Linux
    SED_INPLACE="sed -i"
fi

echo "🔧 Building Rust crate with TypeScript features..."
cd citadel-internal-service-types
cargo build --features typescript

echo "📝 Generating TypeScript types with proper imports..."
TS_RS_EXPORT_DIR=../typescript-client/src/types cargo run --example generate_ts_types --features typescript

echo "🔧 Fixing missing imports in generated TypeScript files..."
cd ../typescript-client/src/types

# Helper function to add import at beginning of file (cross-platform)
add_import() {
    local file=$1
    local import_line=$2

    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS sed
        sed -i '' "1i\\
$import_line
" "$file"
    else
        # Linux sed
        sed -i "1i$import_line" "$file"
    fi
}

# Fix AccountInformation.ts
if [ -f "AccountInformation.ts" ]; then
    if ! grep -q "import.*PeerSessionInformation" AccountInformation.ts; then
        add_import "AccountInformation.ts" 'import type { PeerSessionInformation } from "./PeerSessionInformation";'
    fi
fi

# Fix Accounts.ts
if [ -f "Accounts.ts" ]; then
    if ! grep -q "import.*AccountInformation" Accounts.ts; then
        add_import "Accounts.ts" 'import type { AccountInformation } from "./AccountInformation";'
    fi
fi

# Fix ListAllPeersResponse.ts
if [ -f "ListAllPeersResponse.ts" ]; then
    if ! grep -q "import.*PeerInformation" ListAllPeersResponse.ts; then
        add_import "ListAllPeersResponse.ts" 'import type { PeerInformation } from "./PeerInformation";'
    fi
fi

# Fix ListRegisteredPeersResponse.ts
if [ -f "ListRegisteredPeersResponse.ts" ]; then
    if ! grep -q "import.*PeerInformation" ListRegisteredPeersResponse.ts; then
        add_import "ListRegisteredPeersResponse.ts" 'import type { PeerInformation } from "./PeerInformation";'
    fi
fi

# Fix SessionInformation.ts
if [ -f "SessionInformation.ts" ]; then
    if ! grep -q "import.*PeerSessionInformation" SessionInformation.ts; then
        add_import "SessionInformation.ts" 'import type { PeerSessionInformation } from "./PeerSessionInformation";'
    fi
fi

# Fix protocol type imports from @avarok/citadel-protocol-types
# ts-rs generates type references (e.g., connect_mode: ConnectMode) via #[ts(type = "...")]
# but does NOT generate the import statements for external packages.
echo "🔧 Fixing @avarok/citadel-protocol-types imports in generated files..."
PROTOCOL_TYPES=("ConnectMode" "UdpMode" "SessionSecuritySettings" "SecurityLevel" "TransferType" "ObjectId" "PreSharedKey" "MessageGroupKey" "UserIdentifier" "VirtualObjectMetadata" "ObjectTransferStatus" "MemberState")

for ts_file in *.ts; do
    [ "$ts_file" = "index.ts" ] && continue

    # Find which protocol types are used in this file (excluding existing imports)
    needed_types=()
    for ptype in "${PROTOCOL_TYPES[@]}"; do
        if grep -q "$ptype" "$ts_file" && ! grep -q "import.*$ptype.*@avarok/citadel-protocol-types" "$ts_file"; then
            needed_types+=("$ptype")
        fi
    done

    # Add import if any protocol types are needed
    if [ ${#needed_types[@]} -gt 0 ]; then
        types_str=$(IFS=", "; echo "${needed_types[*]}")
        import_line="import type { $types_str } from '@avarok/citadel-protocol-types';"
        echo "  Adding import to $ts_file: { $types_str }"
        add_import "$ts_file" "$import_line"
    fi
done

echo "📦 Creating index.ts file for convenient imports..."
# index.ts is DERIVED from what was generated, not written from a list.
#
# It used to be a heredoc of ~100 hardcoded `export *` lines. A newly generated
# type was therefore not exported by anything, silently -- and nothing anywhere
# checked, because `tsc` is perfectly happy with a file nobody imports.
#
# That is not hypothetical: five media types (MediaFrameNotification,
# MediaGapNotification, MediaSessionOpened, MediaSessionFailed,
# MediaSessionClosed) were generated, committed, and reachable from no package
# entry point at all. The UI needed the frame shape, could not import it, and
# hand-wrote it -- and the hand copy had already drifted, omitting `sequence`.
# An `as` cast meant the compiler never said so.
#
# Anyone who fixed index.ts by hand also lost the edit on the next run of this
# script, since the heredoc overwrote it unconditionally, while README.md
# advertises the script as safe to re-run.
{
  echo "// Auto-generated index for all TypeScript types"
  echo "// This provides a convenient single import point for all types"
  echo "//"
  echo "// DERIVED from the files in this directory. Do not hand-edit: this file is"
  echo "// rewritten wholesale by generate_types.sh, and a hand-added export would be"
  echo "// lost on the next run. Add the Rust type instead."
  echo ""
  # Deterministic order, so a regeneration that changes nothing produces no diff.
  for ts_file in $(ls *.ts | grep -v '^index\.ts$' | LC_ALL=C sort); do
    echo "export * from './${ts_file%.ts}.js';"
  done
  echo ""
  # NOT derivable from this directory, and dropping it broke the build.
  #
  # These come from an external package, not from ts-rs, and
  # `InternalServiceRequest` references them in its field types -- so a consumer
  # importing `SecurityLevel` from this package gets TS2614 without them. The
  # first version of this derivation compared the directory against the index in
  # both directions, was satisfied, and silently lost this line, because the old
  # heredoc contained something the directory does not.
  echo "// Re-export protocol types used in InternalServiceRequest fields."
  echo "// From an external package, so not derivable from this directory."
  echo "export type { ConnectMode, UdpMode, SessionSecuritySettings, SecurityLevel, TransferType, ObjectId, PreSharedKey, MessageGroupKey, UserIdentifier } from '@avarok/citadel-protocol-types';"
} > index.ts

generated_count=$(ls *.ts | grep -v '^index\.ts$' | wc -l | tr -d ' ')
exported_count=$(grep -c "^export \* from" index.ts | tr -d ' ')
if [ "$generated_count" != "$exported_count" ]; then
  echo "❌ index.ts exports $exported_count of $generated_count generated types"
  exit 1
fi
echo "   index.ts exports all $exported_count generated types"

echo "🎉 TypeScript types generated successfully!"
echo "📁 Types are available in: typescript-client/src/types/"
echo "📦 All imports automatically fixed!"
echo "🏗️  Index file created for convenient imports!"
echo ""
echo "Note: Run npm install && npm run build in typescript-client/ to compile"
echo "      (This is handled automatically by sync-wasm-clients.sh)" 