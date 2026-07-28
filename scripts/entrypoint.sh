#!/bin/sh
# This script must retain LF line endings so Alpine can resolve /bin/sh.
set -e

# Default values
PUID=${PUID:-1000}
PGID=${PGID:-1000}
UMASK=${UMASK:-022}

# Set umask
umask "$UMASK"

# Function to create directories and files
setup_directories() {
    # Ensure directories exist
    mkdir -p /app/logs /app/cache 2>/dev/null || true

    # Create log file if it doesn't exist
    touch /app/logs/decypharr.log 2>/dev/null || true

    # Try to set permissions if possible
    chmod 755 /app 2>/dev/null || true
    chmod 666 /app/logs/decypharr.log 2>/dev/null || true
}

# Check if we're running as root
if [ "$(id -u)" != "0" ]; then
    echo "Running as non-root user $(id -u):$(id -g) with umask $UMASK"

    # Try to create directories as the current user
    setup_directories

    export USER="$(id -un)"
    export HOME="/app"

    exec "$@"
fi

echo "Running as root, setting up user $PUID:$PGID with umask $UMASK"

# Create group if it doesn't exist
if ! getent group "$PGID" > /dev/null 2>&1; then
    addgroup -g "$PGID" appgroup
fi

# Create user if it doesn't exist
if ! getent passwd "$PUID" > /dev/null 2>&1; then
    adduser -D -u "$PUID" -G "$(getent group "$PGID" | cut -d: -f1)" -s /bin/sh appuser
fi

# Get the actual username and groupname
USERNAME=$(getent passwd "$PUID" | cut -d: -f1)
GROUPNAME=$(getent group "$PGID" | cut -d: -f1)

# Create directories and set proper ownership
mkdir -p /app/logs /app/cache
touch /app/logs/decypharr.log

# `chown -R /app` costs one syscall per file, and /app holds the usenet meta
# store -- tens of thousands of files in a real deployment. Measured at ~6
# minutes on a cold filesystem metadata cache (54,700 files), every second of it
# before the HTTP server binds, so each arr polling the download client gets
# connection-refused for the whole walk. In steady state every file already has
# the right owner and the walk changes nothing, so only recurse when the
# top-level owner actually differs.
#   DECYPHARR_CHOWN=auto   (default) recurse only on an ownership mismatch
#   DECYPHARR_CHOWN=always always recurse -- use after a restore or a UID change
#   DECYPHARR_CHOWN=never  never recurse, even on a mismatch
CHOWN_MODE=${DECYPHARR_CHOWN:-auto}
APP_OWNER=$(stat -c '%u:%g' /app 2>/dev/null || echo "")
if [ "$CHOWN_MODE" = "always" ] || { [ "$CHOWN_MODE" = "auto" ] && [ "$APP_OWNER" != "$PUID:$PGID" ]; }; then
    echo "Setting ownership of /app to $PUID:$PGID recursively (owner was '${APP_OWNER:-unknown}'); this can take minutes on a large store"
    chown -R "$PUID:$PGID" /app
else
    echo "Ownership of /app already $PUID:$PGID; skipping recursive chown"
fi

# Always correct the handful of paths this script creates. Bounded cost, and
# they may have just been created as root regardless of the branch above.
chown "$PUID:$PGID" /app /app/logs /app/cache /app/logs/decypharr.log 2>/dev/null || true

chmod 755 /app
chmod 666 /app/logs/decypharr.log

# Export for rclone/fuse
export USER="$USERNAME"
export HOME="/app"

# Execute the command as the specified user
exec su-exec "$PUID:$PGID" "$@"
