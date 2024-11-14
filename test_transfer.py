#!/usr/bin/env python3
import sys
import uuid
import logging as log
import argparse
import time
import binascii
import os

from libflagship.pppp import P2PCmdType, FileTransfer, Type
from libflagship.ppppapi import FileUploadInfo, PPPPState
from cli.pppp import pppp_open
from cli.config import configmgr

class RateLimiter:
    def __init__(self, rate_mbps):
        self.rate_bytes = rate_mbps * 1024 * 1024 / 8  # Convert Mbps to bytes/sec
        self.last_check = time.time()
        self.bytes_sent = 0
    
    def wait(self, chunk_size):
        current_time = time.time()
        time_passed = current_time - self.last_check
        
        if time_passed >= 1.0:
            # Reset counter every second
            self.bytes_sent = 0
            self.last_check = current_time
        elif self.bytes_sent + chunk_size > self.rate_bytes:
            # Wait until next second if we would exceed the rate limit
            sleep_time = 1.0 - time_passed
            time.sleep(sleep_time)
            self.bytes_sent = 0
            self.last_check = time.time()
        
        self.bytes_sent += chunk_size

def send_file(api, fui, data, rate_limit_mbps=10):
    """Send file with debug logging"""
    # Wait for Connected state
    log.debug(f"Waiting for Connected state (current: {api.state})")
    while api.state != PPPPState.Connected:
        time.sleep(0.1)
        if api.stopped.is_set():
            raise ConnectionError("API thread stopped")
    log.debug("Connected to printer")
    
    # Create rate limiter
    rate_limiter = RateLimiter(rate_limit_mbps)
    
    # Request file transfer
    transfer_id = str(uuid.uuid4())[:16].encode()
    log.info("Requesting file transfer...")
    log.debug(f"Transfer ID: {transfer_id!r}")
    api.send_xzyh(transfer_id, cmd=P2PCmdType.P2P_SEND_FILE)
    
    # Send metadata
    metadata = bytes(fui) + b"\x00"
    log.info("Sending metadata...")
    log.debug(f"Metadata: {str(fui)}")
    log.debug(f"Raw metadata ({len(metadata)} bytes): {binascii.hexlify(metadata[:32]).decode()}...")
    api.aabb_request(metadata, frametype=FileTransfer.BEGIN)
    
    # Send file contents
    log.info("Sending file contents...")
    blocksize = 1024 * 32  # 32KB chunks
    total_chunks = (len(data) + blocksize - 1) // blocksize
    total_sent = 0
    chunk_num = 0
    
    for pos in range(0, len(data), blocksize):
        chunk = data[pos:pos + blocksize]
        chunk_num += 1
        
        # Apply rate limiting
        rate_limiter.wait(len(chunk))
        
        # Send chunk
        log.debug(f"Sending chunk {chunk_num}/{total_chunks} at position {pos} ({len(chunk)} bytes)")
        log.debug(f"Chunk prefix: {binascii.hexlify(chunk[:32]).decode()}...")
        api.aabb_request(chunk, frametype=FileTransfer.DATA, pos=pos)
        
        total_sent += len(chunk)
        progress = (total_sent / len(data)) * 100
        if int(progress) % 10 == 0:
            log.info(f"Progress: {progress:.1f}%")
    
    # Send end marker
    log.info("Sending end marker...")
    api.aabb_request(b"", frametype=FileTransfer.END)
    log.info("Upload complete")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('-p', '--printer', type=int, default=0)
    parser.add_argument('-r', '--rate', type=int, default=10,
                      help='Upload rate limit in Mbps (default: 10)')
    parser.add_argument('-c', '--config', default=os.path.expanduser('~/.config/ankermake/config.json'),
                      help='Path to config file (default: ~/.config/ankermake/config.json)')
    parser.add_argument('filename')
    args = parser.parse_args()

    # Set up logging
    log_level = log.INFO
    if args.verbose >= 1:
        log_level = log.DEBUG
    if args.verbose >= 2:
        log_level = 5  # TRACE
    log.basicConfig(level=log_level, format='[%(levelname).1s] [%(asctime)s] %(message)s',
                   datefmt='%H:%M:%S')

    # Load config
    log.debug(f"Loading config from {args.config}")
    config = configmgr()

    # Open connection
    log.info("Opening connection...")
    api = pppp_open(config, args.printer, timeout=10.0)

    try:
        # Read file
        with open(args.filename, 'rb') as f:
            data = f.read()

        # Create file info
        fui = FileUploadInfo.from_data(data, args.filename, user_name="test", user_id="-", machine_id="-")
        log.info(f"Uploading {fui.size} bytes as {fui.name!r}")
        log.debug(f"File MD5: {fui.md5}")

        # Send file
        send_file(api, fui, data, rate_limit_mbps=args.rate)

    except KeyboardInterrupt:
        log.info("Interrupted by user")
    except Exception as e:
        log.error(f"Error: {e}")
        raise
    finally:
        log.info("Closing connection...")
        api.stop()

if __name__ == '__main__':
    main()
