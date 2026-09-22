#!/bin/sh
# IPIP-0499 unixfs-v1-2025. Kubo's IPFS_PROFILE is first-init only;
# this runs on every container start so existing volumes pick up the import profile.
if ! ipfs config profile apply unixfs-v1-2025; then
	echo "dlockss: unixfs-v1-2025 not applied (need Kubo v0.43+)" >&2
fi
exit 0
