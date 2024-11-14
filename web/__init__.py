"""
This module is designed to implement a Flask web server for video
streaming and handling other functionalities of AnkerMake M5.
It also implements various services, routes and functions including.

Methods:
    - startup(): Registers required services on server start

Routes:
    - /ws/mqtt: Handles receiving and sending messages on the 'mqttqueue' stream service through websocket
    - /ws/pppp-state: Provides the status of the 'pppp' stream service through websocket
    - /ws/video: Handles receiving and sending messages on the 'videoqueue' stream service through websocket
    - /ws/ctrl: Handles controlling of light and video quality through websocket
    - /video: Handles the video streaming/downloading feature in the Flask app
    - /: Renders the html template for the root route, which is the homepage of the Flask app
    - /api/version: Returns the version details of api and server as dictionary
    - /api/ankerctl/config/upload: Handles the uploading of configuration file \s
        to Flask server and returns a HTML redirect response
    - /api/ankerctl/server/reload: Reloads the Flask server and returns a HTML redirect response
    - /api/files/local: Handles the uploading of files to Flask server and returns a dictionary containing file details

Functions:
    - webserver(config, host, port, **kwargs): Starts the Flask webserver

Services:
    - util: Houses utility services for use in the web module
    - config: Handles configuration manipulation for ankerctl
"""
import json
import logging as log
import time

from datetime import datetime
from secrets import token_urlsafe as token
from flask import Flask, flash, request, render_template, Response, session, url_for, jsonify
from flask_sock import Sock
from user_agents import parse as user_agent_parse

from libflagship import ROOT_DIR
from web.lib.service import ServiceManager, RunState, ServiceStoppedError

import web.config
import web.platform
import web.util

import cli.util
import cli.config
import cli.countrycodes

app = Flask(__name__, root_path=ROOT_DIR, static_folder="static", template_folder="static")
# secret_key is required for flash() to function
app.secret_key = token(24)
app.config.from_prefixed_env()
app.svc = ServiceManager()

sock = Sock(app)

# autopep8: off
import web.service.pppp
import web.service.video
import web.service.mqtt
import web.service.filetransfer
# autopep8: on

from libflagship.ppppapi import PPPPState  # Import PPPPState after web.service.pppp


PRINTERS_WITHOUT_CAMERA = ["V8110"]


@sock.route("/ws/mqtt")
def mqtt(sock):
    """
    Handles receiving and sending messages on the 'mqttqueue' stream service through websocket
    """
    if not app.config["login"]:
        return
    for data in app.svc.stream("mqttqueue"):
        log.debug(f"MQTT message: {data}")
        sock.send(json.dumps(data))


@sock.route("/ws/video")
def video(sock):
    """
    Handles receiving and sending messages on the 'videoqueue' stream service through websocket
    """
    if not app.config["login"] or not app.config["video_supported"]:
        return
        
    vq = app.svc.svcs.get("videoqueue")
    if not vq or not vq.video_enabled:
        log.info("Video websocket requested but video is disabled")
        return
        
    for msg in app.svc.stream("videoqueue"):
        sock.send(msg.data)


@sock.route("/ws/pppp-state")
def pppp_state(sock):
    """
    Handles a status request for the 'pppp' stream service through websocket
    """
    if not app.config["login"]:
        log.info("Websocket connection rejected: not logged in")
        return

    pppp_connected = False
    log.info("Starting PPPP state websocket handler")

    try:
        while True:  # Keep websocket handler running
            try:
                # Keep track of last message time to detect stalls
                last_msg_time = time.time()
                last_status_time = 0
                
                for chan, msg in app.svc.stream("pppp", timeout=1.0):  # Poll more frequently
                    # Update last message time
                    last_msg_time = time.time()
                    
                    # Check PPPP connection status periodically
                    if time.time() - last_status_time >= 3.0:  # Check every 3 seconds
                        pppp = app.svc.get("pppp")  # Use get instead of borrow to avoid stopping service
                        if pppp:
                            # Check both connected flag and state
                            current_connected = (pppp.connected and 
                                              pppp._api and 
                                              pppp._api.state == PPPPState.Connected)
                            
                            if current_connected:
                                if not pppp_connected:
                                    pppp_connected = True
                                    # Send initial connected status
                                    sock.send(json.dumps({"status": "connected"}))
                                    log.info("PPPP connection established, sent status to websocket")
                                    if hasattr(pppp._api, 'sock') and pppp._api.sock:
                                        try:
                                            local_addr = pppp._api.sock.getsockname()
                                            remote_addr = pppp._api.sock.getpeername()
                                            log.info(f"PPPP socket info at websocket connect - Local: {local_addr}, Remote: {remote_addr}")
                                        except:
                                            pass
                                    log.info("PPPP service state at websocket connect:")
                                    log.info(f"- Connected: {pppp.connected}")
                                    if pppp._api:
                                        log.info(f"- API state: {pppp._api.state}")
                                        log.info(f"- API stopped: {pppp._api.stopped.is_set()}")
                                        log.info(f"- Last heartbeat: {datetime.fromtimestamp(pppp._last_heartbeat).strftime('%H:%M:%S')}")
                            else:
                                if pppp_connected:
                                    # Connection was lost, send disconnected status
                                    sock.send(json.dumps({"status": "disconnected"}))
                                    log.info("PPPP connection lost, sent status to websocket")
                                    pppp_connected = False
                        last_status_time = time.time()
                    
                    # Check for message stalls
                    if time.time() - last_msg_time > 10.0:  # Increased from 5s to 10s
                        log.warning("No PPPP messages received for 10 seconds")
                        pppp = app.svc.get("pppp")
                        if pppp and pppp._api:
                            log.info("PPPP service state during stall:")
                            log.info(f"- Connected: {pppp.connected}")
                            log.info(f"- API state: {pppp._api.state}")
                            log.info(f"- API stopped: {pppp._api.stopped.is_set()}")
                            log.info(f"- Last heartbeat: {datetime.fromtimestamp(pppp._last_heartbeat).strftime('%H:%M:%S')}")
                            if hasattr(pppp._api, 'sock') and pppp._api.sock:
                                try:
                                    local_addr = pppp._api.sock.getsockname()
                                    remote_addr = pppp._api.sock.getpeername()
                                    log.info(f"Socket info during stall - Local: {local_addr}, Remote: {remote_addr}")
                                except:
                                    pass
                        break  # Break inner loop to trigger reconnect
                        
                if not pppp_connected:
                    log.warning(f'[{datetime.now().strftime("%d/%b/%Y %H:%M:%S")}] PPPP connection lost, restarting PPPPService')
                    pppp = app.svc.get("pppp")
                    if pppp:
                        if hasattr(pppp._api, 'sock') and pppp._api.sock:
                            try:
                                local_addr = pppp._api.sock.getsockname()
                                remote_addr = pppp._api.sock.getpeername()
                                log.info(f"PPPP socket info before restart - Local: {local_addr}, Remote: {remote_addr}")
                            except:
                                pass
                        log.info("PPPP service state before restart:")
                        log.info(f"- Connected: {pppp.connected}")
                        if pppp._api:
                            log.info(f"- API state: {pppp._api.state}")
                            log.info(f"- API stopped: {pppp._api.stopped.is_set()}")
                            log.info(f"- Last heartbeat: {datetime.fromtimestamp(pppp._last_heartbeat).strftime('%H:%M:%S')}")
                        pppp.worker_start()
                    time.sleep(1)  # Wait before retrying connection
            except Exception as e:
                if "WebSocket is already closed" in str(e):
                    log.info("WebSocket connection closed by client")
                    break
                log.warning(f"Error in PPPP state websocket handler: {e}")
                log.info("Stack trace:", exc_info=True)
                time.sleep(1)  # Wait before retrying connection
                continue  # Continue outer loop to retry connection
    finally:
        log.info("PPPP state websocket handler ending")



@sock.route("/ws/ctrl")
def ctrl(sock):
    """
    Handles controlling of light and video quality through websocket
    """
    if not app.config["login"]:
        return

    # send a response on connect, to let the client know the connection is ready
    sock.send(json.dumps({"ankerctl": 1}))

    while True:
        msg = json.loads(sock.receive())

        if "mqtt" in msg:
            with app.svc.borrow("mqttqueue") as mq:
                mq.client.command(msg["mqtt"])

        if "light" in msg:
            with app.svc.borrow("videoqueue") as vq:
                vq.api_light_state(msg["light"])

        if "quality" in msg:
            with app.svc.borrow("videoqueue") as vq:
                vq.api_video_mode(msg["quality"])
                
        if "video_enabled" in msg:
            vq = app.svc.svcs.get("videoqueue")
            if vq:
                vq.set_video_enabled(msg["video_enabled"])
                if msg["video_enabled"]:
                    if vq.state == RunState.Stopped:
                        vq.start()
                else:
                    if vq.state == RunState.Running:
                        vq.stop()


@app.get("/video")
def video_download():
    """
    Handles the video streaming/downloading feature in the Flask app
    """
    def generate():
        if not app.config["login"] or not app.config["video_supported"]:
            return
        # Only start videoqueue if video is enabled
        vq = app.svc.svcs.get("videoqueue")
        if vq:
            if not vq.video_enabled:
                log.info("Video stream requested but video is disabled")
                return
            if vq.state == RunState.Stopped:
                try:
                    vq.start()
                    vq.await_ready()
                except ServiceStoppedError:
                    log.error("VideoQueueService could not be started")
                    return
            for msg in app.svc.stream("videoqueue"):
                yield msg.data

    return Response(generate(), mimetype="video/mp4")


@app.get("/")
def app_root():
    """
    Renders the html template for the root route, which is the homepage of the Flask app
    """
    config = app.config["config"]
    with config.open() as cfg:
        user_agent = user_agent_parse(request.headers.get("User-Agent"))
        user_os = web.platform.os_platform(user_agent.os.family)

        if cfg:
            anker_config = str(web.config.config_show(cfg))
            config_existing_email = cfg.account.email
            printer = cfg.printers[app.config["printer_index"]]
            country = cfg.account.country
            if not printer.ip_addr:
                flash("Printer IP address is not set yet, please complete the setup...",
                      "warning")
        else:
            anker_config = "No printers found, please load your login config..."
            config_existing_email = ""
            printer = None
            country = ""

        if ":" in request.host:
            request_host, request_port = request.host.split(":", 1)
        else:
            request_host = request.host
            request_port = "80"

        return render_template(
            "index.html",
            request_host=request_host,
            request_port=request_port,
            configure=app.config["login"],
            login_file_path=web.platform.login_path(user_os),
            anker_config=anker_config,
            video_supported=app.config["video_supported"],
            config_existing_email=config_existing_email,
            country_codes=json.dumps(cli.countrycodes.country_codes),
            current_country=country,
            printer=printer
        )


@app.get("/api/version")
def app_api_version():
    """
    Returns the version details of api and server as dictionary

    Returns:
        A dictionary containing version details of api and server
    """
    return {"api": "0.1", "server": "1.9.0", "text": "OctoPrint 1.9.0"}


@app.post("/api/ankerctl/config/updateip")
def app_api_ankerctl_config_update_ip_addresses():
    """
    Handles the uploading of configuration file to Flask server

    Returns:
        A HTML redirect response
    """
    if request.method != "POST":
        return web.util.flash_redirect(url_for('app_root'),
                                       f"Wrong request method {request.method}", "danger")

    message = None
    category = "info"
    url = url_for("app_root")
    config = app.config["config"]
    found_printers = dict(list(cli.pppp.pppp_find_printer_ip_addresses()))

    if found_printers:
        # update printer IP addresses
        log.debug(f"Checking configured printer IP addresses:")
        updated_printers = cli.config.update_printer_ip_addresses(config, found_printers)

        # determine the message to display to the user
        if updated_printers is not None:
            if updated_printers:
                category = "success"
                message = f"Successfully update IP addresses of printer(s) {', '.join(updated_printers)}"
                url = url_for("app_api_ankerctl_server_internal_reload")
            else:
                message = f"No IP addresses were updated."
        else:
            category = "danger"
            message = f"Internal error."
    else:
        category = "danger"
        message = "No printers responded within timeout. " \
                  "Are you connected to the same network as the printer?"

    return web.util.flash_redirect(url, message, category)


@app.post("/api/ankerctl/config/upload")
def app_api_ankerctl_config_upload():
    """
    Handles the uploading of configuration file to Flask server

    Returns:
        A HTML redirect response
    """
    if request.method != "POST":
        return web.util.flash_redirect(url_for('app_root'))
    if "login_file" not in request.files:
        return web.util.flash_redirect(url_for('app_root'), "No file found", "danger")
    file = request.files["login_file"]

    try:
        web.config.config_import(file, app.config["config"])
        return web.util.flash_redirect(url_for('app_api_ankerctl_server_internal_reload'),
                                       "AnkerMake Config Imported!", "success")
    except web.config.ConfigImportError as err:
        log.exception(f"Config import failed: {err}")
        return web.util.flash_redirect(url_for('app_root'), f"Error: {err}", "danger")
    except Exception as err:
        log.exception(f"Config import failed: {err}")
        return web.util.flash_redirect(url_for('app_root'), f"Unexpected Error occurred: {err}", "danger")


@app.post("/api/ankerctl/config/login")
def app_api_ankerctl_config_login():
    if request.method != "POST":
        flash(f"Invalid request method '{request.method}", "danger")
        return jsonify({"redirect": url_for('app_root')})

    # get form data
    form_data = request.form.to_dict()

    for key in ["login_email", "login_password", "login_country"]:
        if key not in form_data:
            return jsonify({"error": "Error: Missing form entry '{key}'"})

    if not cli.countrycodes.code_to_country(form_data["login_country"]):
        return jsonify({"error": f"Error: Invalid country code '{form_data['login_country']}'"})

    try:
        web.config.config_login(form_data['login_email'], form_data['login_password'],
                                form_data['login_country'],
                                form_data['login_captcha_id'], form_data['login_captcha_text'],
                                app.config["config"])
        flash("AnkerMake Config Imported!", "success")
        return jsonify({"redirect": url_for('app_api_ankerctl_server_reload')})
    except web.config.ConfigImportError as err:
        if err.captcha:
            # we have to solve a capture, display it
            return jsonify({"captcha_id": err.captcha["id"],
                            "captcha_url": err.captcha["img"]})
        # unknown import error
        log.exception(f"Config import failed: {err}")
        flash(f"Error: {err}", "danger")
        return jsonify({"redirect": url_for('app_root')})
    except Exception as err:
        # unknown error
        log.exception(f"Config import failed: {err}")
        flash(f"Unexpected error occurred: {err}", "danger")
        return jsonify({"redirect": url_for('app_root')})


@app.get("/api/ankerctl/server/reload")
def app_api_ankerctl_server_reload():
    """
    Reloads the Flask server

    Returns:
        A HTML redirect response
    """
    # clear any pending flash messages
    if "_flashes" in session:
        session["_flashes"].clear()

    config = app.config["config"]

    with config.open() as cfg:
        if not cfg:
            return web.util.flash_redirect(url_for('app_root'), "No printers found in config", "warning")

    return app_api_ankerctl_server_internal_reload("Ankerctl reloaded successfully")


@app.get("/api/ankerctl/server/intreload")
def app_api_ankerctl_server_internal_reload(success_message: str=None):
    """
    Internal variant for reloading the Flask server.

    This version shall be used as the forwarding target of actions displaying
    flash messages. The current function will not clear and overwrite such
    messages.

    Returns:
        A HTML redirect response
    """
    config = app.config["config"]

    with config.open() as cfg:
        app.config["login"] = bool(cfg)
        app.config["video_supported"] = any([printer.model not in PRINTERS_WITHOUT_CAMERA for printer in cfg.printers])
        if cfg.printers and not app.svc.svcs:
            register_services(app)

    try:
        app.svc.restart_all(await_ready=False)
    except Exception as err:
        log.exception(err)
        return web.util.flash_redirect(url_for('app_root'), f"Ankerctl could not be reloaded: {err}", "danger")

    return web.util.flash_redirect(url_for('app_root'), success_message, "success")


@app.post("/api/ankerctl/file/upload")
def app_api_ankerctl_file_upload():
    if request.method != "POST":
        return web.util.flash_redirect(url_for('app_root'))
    if "gcode_file" not in request.files:
        return web.util.flash_redirect(url_for('app_root'), "No file found", "danger")
    file = request.files["gcode_file"]

    try:
        web.util.upload_file_to_printer(app, file)
        return web.util.flash_redirect(url_for('app_root'),
                                       f"File {file.filename} sent to printer!", "success")
    except ConnectionError as err:
        return web.util.flash_redirect(url_for('app_root'),
                                       "Cannot connect to printer!\n"
                                       "Please verify that printer is online, and on the same network as ankerctl.\n"
                                       f"Exception information: {err}", "danger")
    except Exception as err:
        return web.util.flash_redirect(url_for('app_root'),
                                       f"Unknown error occurred: {err}", "danger")


@app.post("/api/files/local")
def app_api_files_local():
    """
    Handles the uploading of files to Flask server

    Returns:
        A dictionary containing file details
    """
    no_act = not cli.util.parse_http_bool(request.form["print"])

    if no_act:
        cli.util.http_abort(409, "Upload-only not supported by Ankermake M5")

    fd = request.files["file"]

    try:
        web.util.upload_file_to_printer(app, fd)
    except ConnectionError as E:
        log.error(f"Connection error: {E}")
        # This message will be shown in i.e. PrusaSlicer, so attempt to
        # provide a readable explanation.
        cli.util.http_abort(
            503,
            "Cannot connect to printer!\n" \
            "\n" \
            "Please verify that printer is online, and on the same network as ankerctl.\n" \
            "\n" \
            f"Exception information: {E}"
        )

    return {}


@app.get("/api/ankerctl/status")
def app_api_ankerctl_status() -> dict:
    """
    Returns the status of the services

    Returns:
        A dictionary containing the keys 'status', possible_states and 'services'
        status = 'ok' == some service is online, 'error' == no service is online
        services = {svc_name: {online: bool, state: str, state_value: int}}
        possible_states = {state_name: state_value}
        version = {api: str, server: str, text: str}
    """
    def get_svc_status(svc):
        # NOTE: Some services might not update their state on stop, so we can't rely on it to be 100% accurate
        state = svc.state
        if state == RunState.Running:
            return {'online': True, 'state': state.name, 'state_value': state.value}
        return {'online': False, 'state': state.name, 'state_value': state.value}

    svcs_status = {svc_name: get_svc_status(svc) for svc_name, svc in app.svc.svcs.items()}

    # If any service is online, the status is 'ok'
    ok = any([svc['online'] for svc_name, svc in svcs_status.items()])

    return {
        "status": "ok" if ok else "error",
        "services": svcs_status,
        "possible_states": {state.name: state.value for state in RunState},
        "version": app_api_version(),
    }


def register_services(app):
    app.svc.register("pppp", web.service.pppp.PPPPService())
    if app.config["video_supported"]:
        app.svc.register("videoqueue", web.service.video.VideoQueue())
    app.svc.register("mqttqueue", web.service.mqtt.MqttQueue())
    app.svc.register("filetransfer", web.service.filetransfer.FileTransferService())


def webserver(config, printer_index, host, port, insecure=False, **kwargs):
    """
    Starts the Flask webserver

    Args:
        - config: A configuration object containing configuration information
        - host: A string containing host address to start the server
        - port: An integer specifying the port number of server
        - **kwargs: A dictionary containing additional configuration information

    Returns:
        - None
    """
    with config.open() as cfg:
        video_supported = False
        if cfg:
            if printer_index < len(cfg.printers):
                video_supported = cfg.printers[printer_index].model not in PRINTERS_WITHOUT_CAMERA
        else:
            if not cfg.printers:
                log.error("No printers found in config")
            else:
                log.critical(f"Printer number {printer_index} out of range, max printer number is {len(cfg.printers)-1} ")
        app.config["config"] = config
        app.config["login"] = bool(cfg)
        app.config["printer_index"] = printer_index
        app.config["video_supported"] = video_supported
        app.config["port"] = port
        app.config["host"] = host
        app.config["insecure"] = insecure
        app.config.update(kwargs)
        if cfg.printers:
            register_services(app)
        app.run(host=host, port=port)



