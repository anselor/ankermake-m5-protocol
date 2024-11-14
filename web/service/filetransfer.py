import uuid
import logging as log
import binascii
import time
import io

from multiprocessing import Queue

from ..lib.service import Service
from .. import app

from libflagship.pppp import P2PCmdType, FileTransfer, Type
from libflagship.ppppapi import FileUploadInfo, PPPPError, PPPPState

# Import the working implementation from test_transfer.py
from test_transfer import send_file as test_send_file, RateLimiter


class FileTransferService(Service):
    """Service that handles file transfers to the printer.
    
    This service accepts files from clients, caches them in memory,
    then uses the proven test_transfer.py method to send them to the printer.
    """

    def __init__(self):
        super().__init__()
        self.cached_file = None
        self.cached_filename = None

    def send_file(self, fd, user_name, rate_limit_mbps=10):
        """Cache file in memory and send it to printer using test_transfer method"""
        try:
            # Get fresh PPPP service for each transfer
            self.pppp = app.svc.get("pppp")
            api = self.pppp._api
            if not api:
                raise ConnectionError("No pppp connection to printer")
            log.debug(f"Using PPPP API: state={api.state}")
            
            # Wait for Connected state
            while api.state != PPPPState.Connected:
                time.sleep(0.1)
                if api.stopped.is_set():
                    raise ConnectionError("API thread stopped")
                    
        except AttributeError:
            raise ConnectionError("No pppp connection to printer")

        try:
            # Cache file in memory
            self.cached_file = fd.read()
            self.cached_filename = fd.filename
            log.info(f"Cached {len(self.cached_file)} bytes as {self.cached_filename}")

            # Create file info
            fui = FileUploadInfo.from_data(self.cached_file, self.cached_filename, 
                                         user_name=user_name, user_id="-", machine_id="-")
            log.info(f"Going to upload {fui.size} bytes as {fui.name!r}")
            log.debug(f"File MD5: {fui.md5}")

            # Use test_transfer's send_file function
            test_send_file(api, fui, self.cached_file, rate_limit_mbps)
            log.info("Successfully sent print job")

        except Exception as e:
            log.error(f"Could not send print job: {e}")
            raise
        finally:
            # Clear cache and release PPPP service
            self.cached_file = None
            self.cached_filename = None
            app.svc.put("pppp")

    def worker_start(self):
        # Don't get PPPP service at start - get fresh one for each transfer
        log.debug("File transfer service started")

    def worker_run(self, timeout):
        self.idle(timeout=timeout)

    def worker_stop(self):
        # Nothing to clean up since we get/put PPPP service per transfer
        log.debug("File transfer service stopped")
