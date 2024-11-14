import json
import socket
import struct
import logging as log
import time
import platform
import weakref
from datetime import datetime, timedelta

from ..lib.service import Service, ServiceRestartSignal, ServiceStoppedError
from .. import app

from libflagship.pktdump import PacketWriter
from libflagship.pppp import P2PCmdType, PktClose, Duid, Type, Xzyh, Aabb
from libflagship.ppppapi import AnkerPPPPApi, PPPPState, PPPP_LAN_PORT


class PPPPService(Service):
    # Use weakref set to avoid keeping services alive
    _instances = weakref.WeakSet()
    _last_instance_log = 0
    INSTANCE_LOG_INTERVAL = 10  # Log instance count every 10 seconds

    HEARTBEAT_INTERVAL = 15  # Send heartbeat every 15 seconds
    MAX_RETRY_INTERVAL = 30  # Maximum retry interval in seconds
    MAX_RETRY_COUNT = 5      # Maximum number of retries before resetting
    INITIAL_RETRY_DELAY = 2  # Initial retry delay in seconds
    HEARTBEAT_FAIL_THRESHOLD = 3  # Number of failed heartbeats before restart
    HEARTBEAT_TIMEOUT = 5.0  # Timeout for heartbeat response
    CLEANUP_WAIT = 1.0  # Wait time after cleanup
    RECONNECT_DELAY = 5.0  # Delay before reconnecting after cleanup

    def __init__(self):
        super().__init__()
        self.retry_count = 0
        self.last_connection_attempt = 0
        self.last_successful_connection = 0
        self._api = None
        self._last_heartbeat = 0
        self.heartbeat_fail_count = 0
        self.total_restarts = 0
        self.last_restart_time = None
        self.handlers = []
        self.stopping = False
        self.dumper = None
        self.restart_pending = False
        self.last_cleanup_time = 0
        
        # Add this instance to the tracking set
        PPPPService._instances.add(self)
        log.debug(f"Created new PPPP instance. Total instances: {len(PPPPService._instances)}")

    @classmethod
    def log_instance_count(cls):
        """Log the current number of PPPP instances if enough time has passed"""
        now = time.time()
        if now - cls._last_instance_log >= cls.INSTANCE_LOG_INTERVAL:
            active_instances = len(cls._instances)
            active_apis = sum(1 for instance in cls._instances if instance._api is not None)
            connected_apis = sum(1 for instance in cls._instances if instance._api is not None and instance._api.state == PPPPState.Connected)
            
            log.debug(f"PPPP Instances - Total: {active_instances}, With API: {active_apis}, Connected: {connected_apis}")
            cls._last_instance_log = now

    def cleanup_connection(self):
        """Perform complete connection cleanup"""
        if self._api:
            log.debug("Cleaning up PPPP connection")
            try:
                if self._api.state:
                    log.debug(f"Connection state before cleanup: {self._api.state}")
                if hasattr(self._api, 'sock') and self._api.sock:
                    try:
                        local_addr = self._api.sock.getsockname()
                        remote_addr = self._api.sock.getpeername()
                        log.debug(f"Socket info - Local: {local_addr}, Remote: {remote_addr}")
                    except:
                        pass

                if not hasattr(self._api, '_close_received'):
                    try:
                        self._api.send(PktClose())
                        log.debug("Close packet sent successfully")
                    except Exception as e:
                        log.warning(f"Failed to send close packet: {e}")
                    time.sleep(0.1)
                
                if not self._api.stopped.is_set():
                    try:
                        self._api.stop()
                        log.debug("API thread stopped successfully")
                    except Exception as e:
                        log.warning(f"Error stopping API thread: {e}")
                time.sleep(0.1)
                
                if hasattr(self._api, 'chans'):
                    for ch in self._api.chans:
                        try:
                            ch.rx.rx.close()
                            ch.rx.tx.close()
                            ch.tx.rx.close()
                            ch.tx.tx.close()
                        except Exception as e:
                            log.warning(f"Error closing channel: {e}")
                
                try:
                    if hasattr(self._api, 'sock') and self._api.sock:
                        if platform.system() != 'Windows':
                            self._api.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                        try:
                            self._api.sock.shutdown(socket.SHUT_RDWR)
                            log.debug("Socket shutdown successful")
                        except Exception as e:
                            log.warning(f"Socket shutdown error: {e}")
                        try:
                            self._api.sock.close()
                            log.debug("Socket closed successfully")
                        except Exception as e:
                            log.warning(f"Socket close error: {e}")
                except Exception as e:
                    log.warning(f"Socket cleanup error: {e}")
                
                handler_count = len(self.handlers)
                self.handlers.clear()
                log.debug(f"Cleared {handler_count} handlers")
                
                if self.dumper:
                    try:
                        self.dumper.close()
                        log.debug("Packet dumper closed")
                    except Exception as e:
                        log.warning(f"Error closing packet dumper: {e}")
                self.dumper = None
                
                api_state = self._api.state if self._api else None
                self._api = None
                log.debug(f"Cleared API reference (previous state: {api_state})")
                
                self._last_heartbeat = 0
                self.heartbeat_fail_count = 0
                
                time.sleep(self.CLEANUP_WAIT)
                
                # Record cleanup time
                self.last_cleanup_time = time.time()
                
                log.debug("PPPP connection cleanup complete")
            except Exception as e:
                log.warning(f"Error during connection cleanup: {e}")
                log.debug("Stack trace:", exc_info=True)
                self.handlers.clear()
                self.dumper = None
                if hasattr(self, '_api') and self._api:
                    try:
                        if hasattr(self._api, 'chans'):
                            for ch in self._api.chans:
                                try:
                                    ch.rx.rx.close()
                                    ch.rx.tx.close()
                                    ch.tx.rx.close()
                                    ch.tx.tx.close()
                                except:
                                    pass
                        if hasattr(self._api, 'sock') and self._api.sock:
                            self._api.sock.close()
                    except:
                        pass
                self._api = None
                self._last_heartbeat = 0
                self.heartbeat_fail_count = 0
                self.last_cleanup_time = time.time()

    def get_retry_interval(self):
        """Calculate retry interval with exponential backoff"""
        if time.time() - self.last_connection_attempt > self.MAX_RETRY_INTERVAL:
            log.debug("Resetting retry count due to timeout")
            self.retry_count = 0
            
        if self.retry_count >= self.MAX_RETRY_COUNT:
            log.warning(f"Hit maximum retry count ({self.MAX_RETRY_COUNT}), resetting")
            self.retry_count = 0
            
        interval = min(self.INITIAL_RETRY_DELAY * (2 ** self.retry_count), self.MAX_RETRY_INTERVAL)
        return interval

    def reset_retry_count(self):
        """Reset retry counter after successful connection"""
        self.retry_count = 0
        self.last_successful_connection = time.time()
        self.heartbeat_fail_count = 0
        log.debug("Reset connection retry counters")

    def api_command(self, commandType, **kwargs):
        if not self._api or not self.connected:
            log.warning("Command failed: No PPPP connection")
            raise ConnectionError("No pppp connection")

        cmd = {
            "commandType": commandType,
            **kwargs
        }
        return self._api.send_xzyh(
            json.dumps(cmd).encode(),
            cmd=P2PCmdType.P2P_JSON_CMD,
            block=False
        )

    def send_heartbeat(self):
        """Send a heartbeat command to keep the connection alive"""
        if not self._api or not self.connected:
            log.warning("Failed to send heartbeat: No pppp connection")
            if self._api and self._api.state == PPPPState.Connected:
                self.heartbeat_fail_count += 1
            return False

        try:
            self.api_command("heartbeat", data={})
            log.debug("PPPP heartbeat sent")
            self.heartbeat_fail_count = 0
            return True
        except Exception as e:
            log.warning(f"Failed to send heartbeat: {e}")
            self.heartbeat_fail_count += 1
            return False

    def worker_start(self):
        if self.stopping:
            log.info("Service is stopping, skipping start")
            return

        self.cleanup_connection()
        time.sleep(self.CLEANUP_WAIT * 2)

        now = time.time()
        retry_interval = self.get_retry_interval()
        
        if now - self.last_connection_attempt < retry_interval:
            wait_time = retry_interval - (now - self.last_connection_attempt)
            log.debug(f"Waiting {wait_time:.1f}s before retry (attempt {self.retry_count + 1}/{self.MAX_RETRY_COUNT})")
            time.sleep(1)
            return
            
        self.last_connection_attempt = now
        self.retry_count += 1
        self.total_restarts += 1
        self.last_restart_time = now
        
        config = app.config["config"]
        deadline = datetime.now() + timedelta(seconds=15)

        with config.open() as cfg:
            if not cfg:
                log.warning("No config available")
                raise ServiceStoppedError("No config available")
            printer = cfg.printers[app.config["printer_index"]]

        if not printer.ip_addr:
            log.warning("Printer IP address not available")
            raise ServiceStoppedError("Printer IP address not available")

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 0)
            except AttributeError:
                pass
                
            if platform.system() != 'Windows':
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
            
            for attempt in range(3):
                try:
                    sock.bind(('', 0))
                    bound_port = sock.getsockname()[1]
                    log.debug(f"Bound to random port {bound_port} (attempt {attempt + 1})")
                    break
                except socket.error as e:
                    if attempt == 2:
                        raise
                    log.warning(f"Failed to bind socket (attempt {attempt + 1}): {e}")
                    time.sleep(self.CLEANUP_WAIT)
            
            # Create synchronous API
            api = AnkerPPPPApi(sock, Duid.from_string(printer.p2p_duid), addr=(printer.ip_addr, PPPP_LAN_PORT))
            
            if app.config["pppp_dump"]:
                dumpfile = app.config["pppp_dump"]
                log.info(f"Logging all pppp traffic to {dumpfile!r}")
                self.dumper = PacketWriter.open(dumpfile)
                api.set_dumper(self.dumper)

            log.debug(f"Trying connect to printer {printer.name} ({printer.p2p_duid}) over pppp using ip {printer.ip_addr}")

            api.connect_lan_search()
            api.start()

            while api.state != PPPPState.Connected:
                try:
                    timeout = (deadline - datetime.now()).total_seconds()
                    if timeout <= 0:
                        raise TimeoutError("Connection timed out")
                    log.debug(f"Waiting for connection... {timeout:.1f}s remaining")
                    msg = api.recv(timeout=min(timeout, 1.0))
                    if msg:
                        api.process(msg)
                except TimeoutError:
                    log.warning("Connection attempt timed out")
                    raise
                except StopIteration:
                    log.warning("Connection rejected by device")
                    raise ConnectionRefusedError("Connection rejected by device")

            log.info(f"Successfully connected to printer {printer.name} ({printer.p2p_duid}) over pppp using ip {printer.ip_addr}")
            self._api = api
            self._last_heartbeat = time.time()
            self.reset_retry_count()

        except Exception as e:
            log.warning(f"Failed to establish PPPP connection: {e}")
            self.cleanup_connection()
            raise

    def worker_run(self, timeout):
        # Don't process anything if we're stopping
        if self.stopping:
            return

        # Log instance count periodically
        PPPPService.log_instance_count()

        # Check if we need to wait after cleanup
        if not self._api:
            now = time.time()
            if now - self.last_cleanup_time < self.RECONNECT_DELAY:
                # Sleep a bit to avoid rapid restarts
                time.sleep(0.1)
                return
            
            # Only log warning and set restart after delay
            log.warning("PPPP service running without API")
            self.restart_pending = True
            return

        if self._api.stopped.is_set():
            log.warning("PPPP API has stopped")
            self.cleanup_connection()
            self.restart_pending = True
            return

        now = time.time()
        if now - self._last_heartbeat >= self.HEARTBEAT_INTERVAL:
            if not self.send_heartbeat():
                if self.heartbeat_fail_count >= self.HEARTBEAT_FAIL_THRESHOLD:
                    log.warning(f"Failed to send heartbeat {self.heartbeat_fail_count} times, restarting connection")
                    self.cleanup_connection()
                    self.restart_pending = True
                    return
            self._last_heartbeat = now

        try:
            msg = None
            try:
                msg = self._api.recv(timeout=min(timeout, self.HEARTBEAT_INTERVAL))
                if msg:
                    self._api.process(msg)
                    for handler in self.handlers[:]:
                        try:
                            handler((getattr(msg, "chan", None), msg))
                        except Exception as e:
                            log.warning(f"Handler error: {e}")
            except TimeoutError:
                pass

        except ConnectionResetError as e:
            log.warning("PPPP connection lost, restarting service")
            self.cleanup_connection()
            self.restart_pending = True
            return
        except Exception as e:
            log.warning(f"PPPP error: {str(e)}")
            self.heartbeat_fail_count += 1
            if self.heartbeat_fail_count >= self.HEARTBEAT_FAIL_THRESHOLD:
                self.cleanup_connection()
                self.restart_pending = True
                return

        # Only restart if we're not stopping
        if self.restart_pending and not self.stopping:
            self.restart_pending = False
            raise ServiceRestartSignal("Service needs restart")

    def worker_stop(self):
        self.stopping = True
        log.info(f"Stopping PPPP service")
        self.cleanup_connection()
        self.stopping = False

    @property
    def connected(self):
        return (self._api is not None and 
                not self._api.stopped.is_set() and 
                self._api.state == PPPPState.Connected)
