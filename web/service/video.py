import json
import logging as log
import time
from datetime import datetime

from queue import Empty
from multiprocessing import Queue

from ..lib.service import Service, ServiceRestartSignal, RunState
from .. import app

from libflagship.pppp import P2PSubCmdType, P2PCmdType, Xzyh


class VideoQueue(Service):
    FRAME_RATE_CHECK_INTERVAL = 5.0  # Check frame rate every 5 seconds
    STREAM_START_TIMEOUT = 10.0  # Wait up to 10 seconds for stream to start
    STALL_THRESHOLD = 10.0  # Time without frames before considering stalled
    MIN_ACCEPTABLE_FPS = 3.0  # Minimum acceptable frame rate
    WARNING_FPS = 5.0  # Frame rate that triggers warnings
    PPPP_STABILITY_WAIT = 2.0  # Wait for PPPP connection to stabilize
    QUALITY_CHANGE_TIMEOUT = 5.0  # Wait up to 5 seconds for quality change

    def __init__(self):
        super().__init__()
        self.stream_start_time = None
        self.first_frame_received = False
        self.total_frames = 0
        self.stream_uptime = 0
        self.stream_start_timestamp = None
        self.stall_warning_count = 0
        self.last_warning_time = 0
        self.pppp_connect_time = 0
        self.video_enabled = False  # Video disabled by default
        self.current_quality = 0  # 0 = SD, 1 = HD
        self.frame_count = 0
        self.last_frame_check = time.time()
        self.last_frame_time = time.time()
        self.pppp = None  # Initialize PPPP reference
        self.quality_change_time = None
        self.last_start_request = 0
        self.start_request_debounce = 1.0  # Minimum time between start requests

    def api_start_live(self):
        """Start video stream"""
        if not self.pppp or not self.pppp.connected:
            log.warning("Cannot start video: PPPP not connected")
            return

        now = time.time()
        if now - self.last_start_request < self.start_request_debounce:
            log.info("Debouncing start request")
            return

        self.last_start_request = now
        try:
            # Start live video stream
            self.pppp.api_command(P2PSubCmdType.START_LIVE, data={
                "encryptkey": "x",
                "accountId": "y",
            })

            self.stream_start_time = time.time()
            self.stream_start_timestamp = datetime.now()
            self.first_frame_received = False
            self.total_frames = 0
            self.frame_count = 0
            self.last_frame_check = time.time()
            log.info(f"Starting video stream (quality: {'HD' if self.current_quality else 'SD'})")
        except Exception as e:
            log.warning(f"Failed to start video stream: {e}")

    def api_stop_live(self):
        if not self.pppp:
            return

        if self.stream_start_timestamp:
            uptime = time.time() - self.stream_start_time
            avg_fps = self.total_frames / uptime if uptime > 0 else 0
            log.info(f"Stopping video stream (uptime: {uptime:.1f}s, avg fps: {avg_fps:.1f})")  # Visible with -v
        try:
            self.pppp.api_command(P2PSubCmdType.CLOSE_LIVE)
            self.stream_start_timestamp = None
            self.first_frame_received = False
        except Exception as e:
            log.warning(f"Error stopping video stream: {e}")

    def api_light_state(self, light):
        if not self.pppp or not self.pppp.connected:
            log.warning("Cannot set light state: PPPP not connected")
            return

        try:
            self.saved_light_state = light
            self.pppp.api_command(P2PSubCmdType.LIGHT_STATE_SWITCH, data={
                "open": light,
            })
        except Exception as e:
            log.warning(f"Failed to set light state: {e}")

    def api_video_mode(self, mode):
        """Set video quality mode"""
        if not self.pppp or not self.pppp.connected:
            log.warning("Cannot change video mode: PPPP not connected")
            return

        try:
            self.saved_video_mode = mode
            self.pppp.api_command(P2PSubCmdType.LIVE_MODE_SET, data={
                "mode": mode
            })
        except Exception as e:
            log.warning(f"Failed to change video mode: {e}")

    def set_video_enabled(self, enabled):
        """Set video enabled state"""
        if enabled == self.video_enabled:
            return True

        log.info(f"Setting video {'enabled' if enabled else 'disabled'}")
        self.video_enabled = enabled

        if enabled:
            if self.state == RunState.Stopped:
                self.start()
        else:
            if self.state == RunState.Running:
                self.stop()
        return True

    def _handler(self, data):
        chan, msg = data

        if chan != 1:
            return

        if not isinstance(msg, Xzyh):
            return

        # Update frame statistics
        now = time.time()
        self.frame_count += 1
        self.total_frames += 1
        
        # First frame received
        if not self.first_frame_received:
            self.first_frame_received = True
            if self.stream_start_time:  # Only calculate if start time was set
                startup_time = now - self.stream_start_time
                log.info(f"First video frame received after {startup_time:.1f}s")  # Visible with -v
            else:
                log.info("First video frame received")
        
        # Check frame rate periodically
        if now - self.last_frame_check >= self.FRAME_RATE_CHECK_INTERVAL:
            elapsed = now - self.last_frame_check
            frame_rate = self.frame_count / elapsed
            if self.stream_start_time:  # Only calculate if start time was set
                self.stream_uptime = now - self.stream_start_time
                avg_fps = self.total_frames / self.stream_uptime
                log.info(f"Video stream stats: current {frame_rate:.1f} fps, average {avg_fps:.1f} fps, uptime {self.stream_uptime:.1f}s")  # Visible with -v
            else:
                log.info(f"Video stream stats: current {frame_rate:.1f} fps")
            
            # Don't check frame rate during quality change
            if self.quality_change_time and now - self.quality_change_time < self.QUALITY_CHANGE_TIMEOUT:
                log.info("Skipping frame rate check during quality change")
            else:
                # Check for low frame rate
                if frame_rate < self.WARNING_FPS:
                    log.info(f"Low frame rate: {frame_rate:.1f} fps")  # Visible with -v
                    if frame_rate < self.MIN_ACCEPTABLE_FPS:
                        self.stall_warning_count += 1
                        if self.stall_warning_count >= 3:  # Three consecutive low frame rate periods
                            log.warning(f"Persistent low frame rate: {frame_rate:.1f} fps")
                            raise ServiceRestartSignal("Persistent low frame rate")
                    else:
                        self.stall_warning_count = 0
                else:
                    self.stall_warning_count = 0
            
            # Reset frame counter for next interval
            self.frame_count = 0
            self.last_frame_check = now
        
        self.last_frame_time = now
        self.notify(msg)

    def worker_init(self):
        self.saved_light_state = None
        self.saved_video_mode = None
        self.frame_count = 0
        self.last_frame_check = time.time()
        self.last_frame_time = time.time()
        self.pppp = None
        self.quality_change_time = None
        log.info("Video service initialized")  # Visible with -v

    def worker_start(self):
        """Start the video service"""
        if not self.video_enabled:
            return
            
        try:
            # Get PPPP service
            self.pppp = app.svc.get("pppp")
            if not self.pppp:
                log.warning("Cannot start video: PPPP service not available")
                return
                
            self.api_id = id(self.pppp._api)
            self.pppp.handlers.append(self._handler)
            
            # Start video stream
            self.api_start_live()
            
            # Restore saved states
            if self.saved_light_state is not None:
                self.api_light_state(self.saved_light_state)
                
            if self.saved_video_mode is not None:
                self.api_video_mode(self.saved_video_mode)
                
            log.info("Video service started")
        except Exception as e:
            log.warning(f"Failed to start video service: {e}")
            if self.pppp and self._handler in self.pppp.handlers:
                self.pppp.handlers.remove(self._handler)
            self.pppp = None
            raise

    def worker_run(self, timeout):
        if not self.video_enabled:
            return

        if not self.pppp or not self.pppp.connected:
            log.warning("Video service lost PPPP connection")
            return

        self.idle(timeout=timeout)

        now = time.time()

        # Check if stream started successfully
        if not self.first_frame_received:
            if now - self.stream_start_time > self.STREAM_START_TIMEOUT:
                log.warning("Video stream failed to start (no frames received)")
                log.info(f"Stream start timeout after {self.STREAM_START_TIMEOUT}s")  # Visible with -v
                log.info("Video stream state at timeout:")
                log.info(f"- Video enabled: {self.video_enabled}")
                log.info(f"- Service state: {self.state}")
                log.info(f"- PPPP connected: {self.pppp.connected if self.pppp else False}")
                if self.pppp and self.pppp._api:
                    log.info(f"- PPPP state: {self.pppp._api.state}")
                if self.video_enabled:  # Only restart if video is still enabled
                    raise ServiceRestartSignal("Stream start timeout")
                return

        # Check for connection issues
        if not self.pppp.connected:
            log.warning("Video service lost PPPP connection")
            log.info(f"Last frame received: {datetime.fromtimestamp(self.last_frame_time).strftime('%H:%M:%S')}")  # Visible with -v
            log.info(f"Time since last frame: {now - self.last_frame_time:.1f}s")  # Visible with -v
            log.info("Connection state:")
            log.info(f"- Video enabled: {self.video_enabled}")
            log.info(f"- Service state: {self.state}")
            log.info(f"- PPPP API exists: {self.pppp._api is not None}")
            if self.pppp._api:
                log.info(f"- PPPP state: {self.pppp._api.state}")
            if self.video_enabled:  # Only restart if video is still enabled
                raise ServiceRestartSignal("PPPP connection lost")
            return

        # Check for API changes
        if id(self.pppp._api) != self.api_id:
            log.warning("Video service detected new PPPP connection")
            log.info(f"Old API ID: {self.api_id}, New API ID: {id(self.pppp._api)}")  # Visible with -v
            log.info("Connection state:")
            log.info(f"- Video enabled: {self.video_enabled}")
            log.info(f"- Service state: {self.state}")
            log.info(f"- PPPP state: {self.pppp._api.state}")
            # Wait before restarting to let connection stabilize
            time.sleep(self.PPPP_STABILITY_WAIT)
            if self.video_enabled:  # Only restart if video is still enabled
                raise ServiceRestartSignal("New pppp connection detected")
            return

        # Check for stream stalls
        if self.first_frame_received and now - self.last_frame_time > self.STALL_THRESHOLD:
            if now - self.last_warning_time >= 5.0:  # Limit warning frequency
                log.warning("Video stream stall detected")
                log.info(f"Time since last frame: {now - self.last_frame_time:.1f}s")  # Visible with -v
                log.info("Stream state at stall:")
                log.info(f"- Video enabled: {self.video_enabled}")
                log.info(f"- Service state: {self.state}")
                log.info(f"- PPPP connected: {self.pppp.connected}")
                log.info(f"- PPPP state: {self.pppp._api.state}")
                log.info(f"- Frame count: {self.total_frames}")
                log.info(f"- Stream uptime: {self.stream_uptime:.1f}s")
                self.last_warning_time = now
                if now - self.last_frame_time > self.STALL_THRESHOLD * 2:
                    if self.video_enabled:  # Only restart if video is still enabled
                        # Try to restart stream first
                        try:
                            self.api_stop_live()
                            time.sleep(0.5)
                            self.api_start_live()
                            time.sleep(2.0)  # Give stream time to start
                        except:
                            pass
                        # If still no frames, restart service
                        if now - self.last_frame_time > self.STALL_THRESHOLD * 3:
                            raise ServiceRestartSignal("Stream stall detected")
                    return

    def worker_stop(self):
        try:
            if self.pppp:
                self.api_stop_live()
                if self.first_frame_received:
                    log.info(f"Video service stopped, last frame at {datetime.fromtimestamp(self.last_frame_time).strftime('%H:%M:%S')}")  # Visible with -v
                    log.info(f"Stream stats: uptime {self.stream_uptime:.1f}s, total frames {self.total_frames}")  # Visible with -v
                if self._handler in self.pppp.handlers:
                    self.pppp.handlers.remove(self._handler)
                app.svc.put("pppp")
                self.pppp = None
        except Exception as E:
            log.warning(f"{self.name}: Failed to send stop command ({E})")
            log.info(f"Stop error details: {str(E)}")  # Visible with -v
