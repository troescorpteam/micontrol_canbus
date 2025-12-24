#!/bin/bash

# Configuration
INTERFACE="can0"
BITRATE=500000

# Check for root/sudo privileges
if [[ $EUID -ne 0 ]]; then
   echo "This script must be run with sudo"
   exit 1
fi

echo "Bringing $INTERFACE down..."
ip link set $INTERFACE down

echo "Bringing $INTERFACE up with bitrate $BITRATE..."
ip link set $INTERFACE up type can bitrate $BITRATE

# Verify the status
if ip link show $INTERFACE | grep -q "UP"; then
    echo "Successfully initialized $INTERFACE at $BITRATE bps."
else
    echo "Error: Failed to bring up $INTERFACE."
    exit 1
fi
