#!/usr/bin/env python3
"""
Vehicle agent — Jetson + ZED 2i.

Implements the vehicle half of the contract in SERVER_API_SPEC.md §1:

  subscribes  {vid}/command/livestream   ENABLE -> WHIP publish, DISABLE -> teardown
  subscribes  {vid}/command/teleop       {"lin":..,"ang":..} -> cmd_vel with deadman
  publishes   one WHIP session to the SFU, non-trickle ICE, Bearer auth

Always-on process: it connects to MQTT at boot and idles. MQTT never starts or
stops the process, only moves it in and out of a publish session.

Video source is selectable so you can bisect failures:
  SOURCE=test   videotestsrc              — no camera at all
  SOURCE=uvc    ZED 2i as a plain UVC cam — no ZED SDK, no ROS, left eye cropped
  SOURCE=ros    ZED ROS2 wrapper topic    — the real path

Encoder is nvv4l2h264enc when present (Jetson), x264enc otherwise.
"""

from __future__ import annotations

import json
import logging
from collections import deque
import os
import signal
import sys
import threading
import time
from typing import Optional
from urllib.parse import urljoin

import gi

gi.require_version("Gst", "1.0")
gi.require_version("GstSdp", "1.0")
gi.require_version("GstWebRTC", "1.0")
from gi.repository import GLib, Gst, GstSdp, GstWebRTC  # noqa: E402

import requests  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-5s %(name)s: %(message)s")
log = logging.getLogger("vehicle")

# --------------------------------------------------------------------------
# config (see vehicle.env)
# --------------------------------------------------------------------------


def once(fn, *args):
    """Wrap a call for GLib.idle_add / timeout_add so it runs exactly once.

    GLib repeats any callback that returns a truthy value, and several methods
    here return True to mean "this worked". Scheduling them directly therefore
    re-runs them forever: a start that succeeds is queued again, finds a
    session already publishing, tears it down and rebuilds — over and over.
    Wrapping states the one-shot intent instead of depending on what each
    function happens to return."""
    def run(*_ignored):
        try:
            fn(*args)
        except Exception:  # noqa: BLE001
            log.exception("scheduled call failed")
        return False          # GLib.SOURCE_REMOVE
    return run


def env(*names: str, default: str = "") -> str:
    """First of several names that is set. Lets the agent read the vehicle's
    existing .env_vars (VEHICLE_ID, MQTT_USERNAME, ...) without renaming
    anything the rest of the vehicle stack depends on."""
    for n in names:
        v = os.environ.get(n)
        if v:
            return v
    return default


VID = env("VID", "VEHICLE_ID", default="vehicle-01")   # the only per-vehicle value
MQTT_HOST = env("MQTT_HOST", "MQTT_BROKER_HOST", default="127.0.0.1")
MQTT_PORT = int(env("MQTT_PORT", "MQTT_BROKER_PORT", default="1883"))
MQTT_USER = env("MQTT_USER", "MQTT_USERNAME")
MQTT_PASS = env("MQTT_PASS", "MQTT_PASSWORD")
# TLS is implied by 8883 unless told otherwise.
MQTT_TLS = env("MQTT_TLS", default="true" if MQTT_PORT == 8883 else "false").lower() == "true"
MQTT_CLIENT_ID = env("MQTT_CLIENT_ID", default=f"livestream-{VID}")

# Where this vehicle publishes. Two ways to set it:
#   1. Bake WHIP_URL/PUBLISH_TOKEN into this vehicle's config (provision by hand)
#   2. Leave them empty and set STREAM_API_URL + PROVISION_TOKEN — the vehicle
#      then asks for its own publish URL using its VID, so VID is the only
#      per-vehicle value and everything else is fleet-wide constant.
WHIP_URL = os.environ.get("WHIP_URL", "")
PUBLISH_TOKEN = os.environ.get("PUBLISH_TOKEN", "")
STREAM_API_URL = os.environ.get("STREAM_API_URL", "").rstrip("/")
PROVISION_TOKEN = os.environ.get("PROVISION_TOKEN", "")

AUTOSTART = os.environ.get("AUTOSTART", "false").lower() == "true"

# Where start/stop commands come from.
#   ros   — a ROS2 topic, published by the vehicle's existing MQTT bridge.
#           BC keeps talking to the broker it already uses; this agent never
#           opens a second broker connection.
#   mqtt  — subscribe to the broker directly.
CONTROL = env("CONTROL", default="ros").lower()
CONTROL_TOPIC = env("LIVESTREAM_CONTROL_TOPIC", default="/livestream_control")
# Left empty, the message type is detected from the running graph.
CONTROL_TYPE = env("LIVESTREAM_CONTROL_TYPE", default="")
# Gap between stop and start on a restart. Long enough for the ingress to
# release the previous session.
RESTART_GAP_MS = int(env("RESTART_GAP_MS", default="600"))
# How long to wait for the first camera frame before giving up on a start.
FIRST_FRAME_WAIT_S = float(env("FIRST_FRAME_WAIT_S", default="10"))
# How often a keyframe is emitted, in seconds rather than frames.
#
# The encoder counts in frames, so a keyframe interval set from the configured
# FPS is wrong whenever the camera delivers something else: at a configured 15
# and an actual 3, "every 30 frames" is a keyframe every ten seconds. Nothing
# looks broken until someone joins — a decoder cannot start without a keyframe,
# so they stare at nothing for ten seconds — or until one packet is lost, which
# freezes the picture for the same ten seconds.
KEYFRAME_EVERY_S = float(env("KEYFRAME_EVERY_S", default="2"))
# A publish that drops on its own — the uplink went away, the server restarted,
# a tower handover — should come back without anyone asking. Only a session
# that died unexpectedly is retried; a stop stays stopped.
RECONNECT_MAX_TRIES = int(env("RECONNECT_MAX_TRIES", default="6"))
RECONNECT_BASE_S = float(env("RECONNECT_BASE_S", default="2"))
# Publishing but no frame for this long means the camera has stopped. The
# WebRTC session stays up and healthy, so nothing else notices — the viewer
# would just see a frozen picture.
FRAME_STALL_S = float(env("FRAME_STALL_S", default="5"))

SOURCE = os.environ.get("SOURCE", "test")          # test | uvc | ros
VIDEO_DEVICE = os.environ.get("VIDEO_DEVICE", "/dev/video0")
ZED_TOPIC = os.environ.get("ZED_TOPIC", "/traversability_segmenter/relay/image")
# A topic ending in /compressed carries sensor_msgs/CompressedImage (JPEG).
# Override with ZED_TOPIC_COMPRESSED=true/false if your naming differs.
ZED_TOPIC_COMPRESSED = os.environ.get(
    "ZED_TOPIC_COMPRESSED",
    "true" if ZED_TOPIC.rstrip("/").endswith("compressed") else "false"
).lower() == "true"
CMD_VEL_TOPIC = os.environ.get("CMD_VEL_TOPIC", "/cmd_vel")

WIDTH = int(os.environ.get("WIDTH", "1280"))        # per eye, after cropping
HEIGHT = int(os.environ.get("HEIGHT", "720"))
FPS = int(os.environ.get("FPS", "30"))
BITRATE_KBPS = int(os.environ.get("BITRATE_KBPS", "1200"))
# Burn a clock into the picture, for latency measurement. Off by default: it
# costs a little CPU and nobody driving wants it on screen.
CLOCK_OVERLAY = os.environ.get("CLOCK_OVERLAY", "false").lower() == "true"

STUN_SERVER = os.environ.get("STUN_SERVER", "stun://stun.l.google.com:19302")
TURN_SERVER = os.environ.get("TURN_SERVER", "")     # turns://user:pass@host:443
ICE_GATHER_TIMEOUT_S = float(os.environ.get("ICE_GATHER_TIMEOUT_S", "10"))
# How long to keep gathering once the first candidates are in hand.
#
# Waiting for gathering to COMPLETE means waiting for the STUN round trip to
# finish, which costs about two seconds on every start whether or not the
# result is needed. Host candidates are enough on an overlay network, and a
# server-reflexive one usually arrives within a few hundred milliseconds. So
# the offer goes out once candidates exist and this has elapsed, rather than
# when gathering says it has nothing left to look for.
ICE_SETTLE_MS = int(os.environ.get("ICE_SETTLE_MS", "400"))

MAX_LIN = float(os.environ.get("MAX_LIN", "1.0"))
MAX_ANG = float(os.environ.get("MAX_ANG", "1.5"))
DEADMAN_S = float(os.environ.get("DEADMAN_S", "0.5"))

TOPIC_LIVESTREAM = f"{VID}/command/livestream"
TOPIC_TELEOP = f"{VID}/command/teleop"


def have(element: str) -> bool:
    return Gst.ElementFactory.find(element) is not None


# --------------------------------------------------------------------------
# pipeline construction
# --------------------------------------------------------------------------

def latency_clock() -> str:
    """An optional burnt-in clock, for measuring part of the delay.

    Note what this does and does not measure. A clock inserted here times
    everything from this point on — encode, network, buffer, decode, display —
    but not camera exposure or the ROS hop that got the frame here, both of
    which happen before it. For the full glass-to-glass figure, point the
    camera at a stopwatch instead; see docs/LATENCY.md. This is for isolating
    the half of the delay this pipeline is responsible for."""
    if not CLOCK_OVERLAY:
        return ""
    if not have("clockoverlay"):
        log.warning("CLOCK_OVERLAY asked for but the pango plugin is missing "
                    "(apt install gstreamer1.0-plugins-base)")
        return ""
    return ('clockoverlay time-format="%H:%M:%S" halignment=left '
            'valignment=top font-desc="Mono 26" ! ')


def source_branch() -> str:
    """Everything up to (and including) videoconvert, in system memory."""
    if SOURCE == "test":
        # A burnt-in clock makes glass-to-glass latency measurable with a
        # phone camera. Skipped if the pango plugin isn't installed.
        clock = ('timeoverlay halignment=right valignment=bottom '
                 'font-desc="Sans 28" ! ') if have("timeoverlay") else ""
        return (f"videotestsrc is-live=true pattern=ball ! "
                f"video/x-raw,width={WIDTH},height={HEIGHT},framerate={FPS}/1 ! "
                f"{clock}videoconvert")

    if SOURCE == "uvc":
        # A ZED 2i enumerates as a UVC camera delivering both eyes side by
        # side in one frame. Crop away the right half to get the left eye.
        # Check what your unit actually offers first:
        #   v4l2-ctl -d /dev/video0 --list-formats-ext
        full_w = WIDTH * 2
        return (f"v4l2src device={VIDEO_DEVICE} io-mode=2 ! "
                f"video/x-raw,format=YUY2,width={full_w},height={HEIGHT},framerate={FPS}/1 ! "
                f"videocrop right={WIDTH} ! videoconvert")

    if SOURCE == "ros":
        # Frames are pushed in by the rclpy subscriber below.
        clock = latency_clock()
        if ZED_TOPIC_COMPRESSED:
            # JPEG in, H264 out. WebRTC cannot carry MJPEG, so the frames are
            # decoded and re-encoded — but on Jetson both halves run on
            # dedicated hardware, and only a few MB/s crosses loopback instead
            # of ~110 MB/s of raw pixels.
            if have("nvjpegdec"):
                log.info("decoder: nvjpegdec (Jetson hardware)")
                dec = "nvjpegdec"          # outputs NVMM, feeds nvvidconv directly
            else:
                log.warning("decoder: jpegdec (software)")
                dec = "jpegdec ! videoconvert"
            # The clock goes after the decode: it has to be drawn on pixels.
            return ("appsrc name=rossrc is-live=true do-timestamp=true format=time "
                    "block=false max-bytes=8000000 ! jpegparse ! " + dec
                    + (f" ! videoconvert ! {clock}videoconvert" if clock else ""))
        return ("appsrc name=rossrc is-live=true do-timestamp=true format=time "
                f"block=false max-bytes=8000000 ! videoconvert ! {clock}"
                "videoconvert" if clock else
                "appsrc name=rossrc is-live=true do-timestamp=true format=time "
                "block=false max-bytes=8000000 ! videoconvert")

    raise SystemExit(f"unknown SOURCE={SOURCE!r} (test|uvc|ros)")


def keyframe_interval(measured_fps: Optional[float] = None) -> int:
    """Keyframes per KEYFRAME_EVERY_S, counted in frames because that is what
    the encoder takes.

    Uses the rate frames are actually arriving at when that is known. A camera
    delivering 3 fps against a configured 15 would otherwise get a keyframe
    every ten seconds, and a viewer cannot start decoding until one arrives."""
    rate = measured_fps or float(FPS)
    frames = max(1, int(round(rate * KEYFRAME_EVERY_S)))
    if measured_fps and abs(measured_fps - FPS) > 1:
        log.info("source is running at %.1f fps, not the configured %d — "
                 "keyframe every %d frames (%.0fs)",
                 measured_fps, FPS, frames, KEYFRAME_EVERY_S)
    return frames


def encoder_branch(keyframe_frames: int) -> str:
    """Hardware encode on Jetson, software everywhere else."""
    if have("nvv4l2h264enc"):
        log.info("encoder: nvv4l2h264enc (Jetson hardware)")
        return (
            "nvvidconv ! video/x-raw(memory:NVMM),format=NV12 ! "
            f"nvv4l2h264enc bitrate={BITRATE_KBPS * 1000} "
            "control-rate=1 preset-level=1 profile=0 "
            f"iframeinterval={keyframe_frames} idrinterval={keyframe_frames} "
            "insert-sps-pps=true insert-vui=true maxperf-enable=true"
        )
    log.warning("encoder: x264enc (software) — expect higher latency than the Jetson path")
    return (
        f"x264enc tune=zerolatency speed-preset=ultrafast bitrate={BITRATE_KBPS} "
        f"key-int-max={keyframe_frames} ! video/x-h264,profile=constrained-baseline"
    )


def build_pipeline_desc(measured_fps: Optional[float] = None) -> str:
    return (
        f"{source_branch()} ! "
        f"{encoder_branch(keyframe_interval(measured_fps))} ! "
        "h264parse config-interval=-1 ! "
        "video/x-h264,stream-format=byte-stream,alignment=au ! "
        "rtph264pay config-interval=-1 aggregate-mode=zero-latency pt=96 ! "
        "application/x-rtp,media=video,encoding-name=H264,payload=96,clock-rate=90000 ! "
        "webrtcbin name=whip bundle-policy=max-bundle latency=0"
    )


# --------------------------------------------------------------------------
# one WHIP publish session
# --------------------------------------------------------------------------

class WhipSession:
    def __init__(self, whip_url: str, token: str, on_dead,
                 on_stale_endpoint=None, on_connected=None) -> None:
        self.whip_url = whip_url
        self.token = token
        self.on_dead = on_dead
        self.on_stale_endpoint = on_stale_endpoint
        self.on_connected = on_connected
        self.resource_url: Optional[str] = None
        self.pipeline: Optional[Gst.Pipeline] = None
        self.webrtc = None
        self.appsrc = None
        self._offer_sent = False

    # -- lifecycle ---------------------------------------------------------

    def start(self, measured_fps: Optional[float] = None) -> None:
        desc = build_pipeline_desc(measured_fps)
        log.info("pipeline: %s", desc)
        self.pipeline = Gst.parse_launch(desc)
        self.webrtc = self.pipeline.get_by_name("whip")
        self.appsrc = self.pipeline.get_by_name("rossrc")

        self.webrtc.set_property("stun-server", STUN_SERVER or None)
        if TURN_SERVER:
            # Without this, a vehicle behind carrier NAT may never connect.
            self.webrtc.emit("add-turn-server", TURN_SERVER)
            log.info("turn-server configured")

        self.webrtc.connect("on-negotiation-needed", self._on_negotiation_needed)
        self.webrtc.connect("notify::ice-gathering-state", self._on_ice_gathering)
        self.webrtc.connect("notify::connection-state", self._on_conn_state)

        bus = self.pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message::error", self._on_bus_error)
        bus.connect("message::eos", lambda *_: self.stop("eos"))

        self.pipeline.set_state(Gst.State.PLAYING)
        GLib.timeout_add_seconds(int(ICE_GATHER_TIMEOUT_S) + 5, self._gather_watchdog)

    def stop(self, reason: str = "requested") -> None:
        log.info("session stopping (%s)", reason)
        if self.resource_url:
            try:
                requests.delete(self.resource_url,
                                headers={"Authorization": f"Bearer {self.token}"},
                                timeout=5)
                log.info("WHIP DELETE %s", self.resource_url)
            except Exception as exc:  # noqa: BLE001
                log.warning("WHIP DELETE failed: %s", exc)
            self.resource_url = None
        if self.pipeline:
            self.pipeline.set_state(Gst.State.NULL)
            self.pipeline = None
        self.webrtc = None
        self.appsrc = None

    # -- negotiation -------------------------------------------------------

    def _on_negotiation_needed(self, element) -> None:
        # WHIP is publish-only; announce sendonly or some servers reject the offer.
        trans = element.emit("get-transceiver", 0)
        if trans:
            trans.set_property(
                "direction", GstWebRTC.WebRTCRTPTransceiverDirection.SENDONLY)
        promise = Gst.Promise.new_with_change_func(self._on_offer_created, element, None)
        element.emit("create-offer", None, promise)

    def _on_offer_created(self, promise, element, _) -> None:
        reply = promise.get_reply()
        offer = reply.get_value("offer")
        element.emit("set-local-description", offer, Gst.Promise.new())
        log.info("local offer set, gathering ICE (non-trickle)")

    def _on_ice_gathering(self, element, _param) -> None:
        state = element.get_property("ice-gathering-state")
        log.info("ice-gathering-state: %s", state.value_nick)
        if state == GstWebRTC.WebRTCICEGatheringState.GATHERING:
            GLib.timeout_add(ICE_SETTLE_MS, self._settle)
        elif state == GstWebRTC.WebRTCICEGatheringState.COMPLETE:
            self._send_offer_once("gathering complete")

    def _candidate_count(self) -> int:
        try:
            return self.webrtc.get_property(
                "local-description").sdp.as_text().count("a=candidate")
        except Exception:  # noqa: BLE001
            return 0

    def _settle(self) -> bool:
        """Send what we have rather than waiting for gathering to finish.

        Repeats until at least one candidate exists: an offer with none in it
        gives the server nothing to connect to, which fails slower than
        waiting would have."""
        if self._offer_sent:
            return False
        n = self._candidate_count()
        if n == 0:
            return True                      # nothing yet — come back
        self._send_offer_once(f"{n} candidate(s) after {ICE_SETTLE_MS}ms")
        return False

    def _send_offer_once(self, why: str) -> None:
        if self._offer_sent:
            return
        self._offer_sent = True
        log.info("sending the offer — %s", why)
        threading.Thread(target=self._post_offer, daemon=True).start()

    def _gather_watchdog(self) -> bool:
        if not self._offer_sent and self.pipeline:
            log.error("ICE gathering never completed — aborting session")
            self.stop("ice gathering timeout")
            self.on_dead()
        return False

    def _post_offer(self) -> None:
        """Non-trickle: the full offer, all candidates embedded, one POST."""
        try:
            sdp = self.webrtc.get_property("local-description").sdp.as_text()
        except Exception as exc:  # noqa: BLE001
            log.error("no local description: %s", exc)
            return GLib.idle_add(self._fail)

        log.info("POST %s (%d bytes of SDP)", self.whip_url, len(sdp))
        try:
            r = requests.post(
                self.whip_url,
                data=sdp.encode(),
                headers={"Content-Type": "application/sdp",
                         "Authorization": f"Bearer {self.token}"},
                timeout=15,
            )
        except Exception as exc:  # noqa: BLE001
            log.error("WHIP POST failed: %s", exc)
            return GLib.idle_add(self._fail)

        if r.status_code in (403, 404):
            # The ingress is gone — the server closes unwatched streams by
            # removing it. The cached URL is stale; the next start re-provisions.
            log.warning("WHIP POST -> %s: the publish endpoint no longer exists, "
                        "will re-provision on the next start", r.status_code)
            if self.on_stale_endpoint:
                self.on_stale_endpoint()
            self.on_dead()
            return False
        if r.status_code not in (200, 201):
            log.error("WHIP POST -> %s: %s", r.status_code, r.text[:300])
            return GLib.idle_add(self._fail)

        # Location may be relative (LiveKit returns /w/<key>/<id>) — RFC 3986
        # resolve against the request URL.
        loc = r.headers.get("Location")
        self.resource_url = urljoin(r.url, loc) if loc else None
        log.info("WHIP %s, resource=%s", r.status_code, self.resource_url)
        GLib.idle_add(self._apply_answer, r.text)

    def _apply_answer(self, answer_sdp: str) -> bool:
        res, sdp = GstSdp.SDPMessage.new_from_text(answer_sdp)
        if res != GstSdp.SDPResult.OK:
            log.error("could not parse SDP answer")
            self._fail()
            return False
        answer = GstWebRTC.WebRTCSessionDescription.new(
            GstWebRTC.WebRTCSDPType.ANSWER, sdp)
        self.webrtc.emit("set-remote-description", answer, Gst.Promise.new())
        log.info("remote answer applied — media should be flowing")
        return False

    # -- health ------------------------------------------------------------

    def _on_conn_state(self, element, _param) -> None:
        state = element.get_property("connection-state")
        log.info("connection-state: %s", state.value_nick)
        if state == GstWebRTC.WebRTCPeerConnectionState.CONNECTED:
            # A publish that holds clears the retry history, so a drop next
            # week starts from a short wait rather than wherever the last
            # bad patch left the counter.
            if self.on_connected:
                self.on_connected()
        if state in (GstWebRTC.WebRTCPeerConnectionState.FAILED,
                     GstWebRTC.WebRTCPeerConnectionState.CLOSED):
            self.stop(f"peer connection {state.value_nick}")
            self.on_dead()

    def _on_bus_error(self, _bus, msg) -> None:
        err, dbg = msg.parse_error()
        log.error("pipeline error: %s (%s)", err, dbg)
        self.stop("pipeline error")
        self.on_dead()

    def _fail(self) -> bool:
        self.stop("whip failed")
        self.on_dead()
        return False


# --------------------------------------------------------------------------
# optional ROS2 side: ZED frames in, cmd_vel out
# --------------------------------------------------------------------------

class RosBridge:
    """ROS2 side: camera frames in, cmd_vel out, and — when CONTROL=ros —
    the start/stop command that BC's existing MQTT bridge republishes."""

    def __init__(self, agent, want_video: bool, want_control: bool) -> None:
        import rclpy
        from geometry_msgs.msg import Twist
        from rclpy.qos import qos_profile_sensor_data
        from sensor_msgs.msg import CompressedImage, Image

        self.agent = agent
        self.Twist = Twist
        rclpy.init(args=None)
        self.node = rclpy.create_node("livestream_agent")
        self.caps = None
        # Which appsrc the caps above have been applied to. Every session
        # builds a new pipeline and therefore a new appsrc, and caps set on
        # the previous one mean nothing to it: pushing a buffer into an
        # appsrc with no caps fails immediately with "not-negotiated", which
        # looks like the start being ignored.
        self._capped_src = None
        self.frames = 0
        self.last_frame = 0.0
        self._arrivals: deque = deque(maxlen=30)
        self._warned_encoding = False
        self._warned_size = False

        if want_video:
            # Camera publishers almost always offer BEST_EFFORT. Subscribing
            # with the default RELIABLE profile is a QoS mismatch: ROS reports
            # "incompatible QoS" and delivers nothing, which looks exactly like
            # a dead topic. Best-effort is also correct for video — a dropped
            # frame should be dropped, not retransmitted.
            if ZED_TOPIC_COMPRESSED:
                self.node.create_subscription(CompressedImage, ZED_TOPIC,
                                              self._on_compressed,
                                              qos_profile_sensor_data)
            else:
                self.node.create_subscription(Image, ZED_TOPIC, self._on_image,
                                              qos_profile_sensor_data)
            log.info("ROS2: subscribing %s (%s)", ZED_TOPIC,
                     "compressed JPEG" if ZED_TOPIC_COMPRESSED else "raw")

        self.cmd_pub = self.node.create_publisher(Twist, CMD_VEL_TOPIC, 1)

        if want_control:
            self._subscribe_control()
        self._rclpy = rclpy
        self._spin = threading.Thread(target=self._spin_loop, daemon=True)
        self._spin.start()

    def _spin_loop(self) -> None:
        try:
            self._rclpy.spin(self.node)
        except Exception as exc:  # noqa: BLE001
            # spin() raises when the context is shut down from another thread,
            # which is the normal way this ends. Logged at debug so a genuine
            # fault is still recoverable from the log without making an
            # ordinary shutdown look like one.
            log.debug("ROS spin ended: %s", exc)

    def stop(self) -> None:
        """Shut ROS down before the process exits. Leaving the executor running
        while the interpreter tears down aborts in native code."""
        try:
            self._rclpy.shutdown()
        except Exception as exc:  # noqa: BLE001
            # Already shut down, or never initialised. Either way the process
            # is on its way out and there is nothing left to repair.
            log.debug("ROS shutdown: %s", exc)
        self._spin.join(timeout=2)

    def measured_fps(self) -> Optional[float]:
        """The rate frames are actually arriving at, or None if too few have
        been seen to say. Trusted over the configured FPS when sizing the
        keyframe interval, because the configured value describes what was
        asked for and this describes what is happening."""
        if len(self._arrivals) < 3:
            return None
        span = self._arrivals[-1] - self._arrivals[0]
        if span <= 0:
            return None
        return (len(self._arrivals) - 1) / span

    # Any of these can carry a start/stop command; the type is whatever the
    # vehicle's bridge publishes, so it is detected rather than assumed.
    _CONTROL_TYPES = {
        "std_msgs/msg/Bool": ("std_msgs.msg", "Bool"),
        "std_msgs/msg/Int32": ("std_msgs.msg", "Int32"),
        "std_msgs/msg/Int8": ("std_msgs.msg", "Int8"),
        "std_msgs/msg/UInt8": ("std_msgs.msg", "UInt8"),
        "std_msgs/msg/String": ("std_msgs.msg", "String"),
    }

    def _detect_control_type(self, timeout: float = 5.0) -> str:
        """Ask the graph what type the control topic carries. The bridge may
        not have advertised it yet at boot, so this polls briefly."""
        if CONTROL_TYPE:
            return CONTROL_TYPE
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for name, types in self.node.get_topic_names_and_types():
                if name == CONTROL_TOPIC and types:
                    return types[0]
            time.sleep(0.25)
        return "std_msgs/msg/Bool"

    def _subscribe_control(self) -> None:
        import importlib

        tname = self._detect_control_type()
        mod_name, cls_name = self._CONTROL_TYPES.get(
            tname, ("std_msgs.msg", "Bool"))
        if tname not in self._CONTROL_TYPES:
            log.warning("control topic %s has type %s, which is not one this "
                        "agent understands — assuming Bool", CONTROL_TOPIC, tname)
        msg_cls = getattr(importlib.import_module(mod_name), cls_name)
        self.node.create_subscription(msg_cls, CONTROL_TOPIC, self._on_control, 10)
        log.info("ROS2: livestream control on %s (%s)", CONTROL_TOPIC, tname)

    @staticmethod
    def _truthy(value) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        return str(value).strip().lower() in ("1", "true", "start", "on", "enable")

    _RESTART_WORDS = ("restart", "reload", "reconnect")

    def _on_control(self, msg) -> None:
        """start / stop / restart on one topic. Numeric and boolean payloads
        also work, so a bridge publishing 1/0 or true/false needs no change."""
        raw = getattr(msg, "data", True)
        word = str(raw).strip().lower()

        if word in self._RESTART_WORDS:
            log.info("control: restart")
            GLib.idle_add(once(self._restart))
        elif self._truthy(raw):
            log.info("control: start (%r)", raw)
            GLib.idle_add(once(self.agent.enable, WHIP_URL or None, PUBLISH_TOKEN))
        else:
            log.info("control: stop (%r)", raw)
            GLib.idle_add(once(self.agent.disable))

    def _restart(self) -> bool:
        """Tear down, pause, publish again. The pause matters: the WHIP session
        has to close server-side before a new one claims the same ingress, or
        the second attempt collides with the first and neither connects."""
        self.agent.disable()
        GLib.timeout_add(RESTART_GAP_MS, once(self._restart_start))
        return False

    def _restart_start(self) -> bool:
        log.info("control: restarting publish")
        self.agent.enable(WHIP_URL or None, PUBLISH_TOKEN)
        return False

    def _on_compressed(self, msg) -> None:
        self.last_frame = time.monotonic()
        self._arrivals.append(self.last_frame)
        """JPEG frames go straight into the pipeline; jpegparse reads the
        dimensions out of the JPEG header, so no caps negotiation is needed."""
        sess = self.agent.session
        if not sess or not sess.appsrc:
            return
        if "jpeg" not in msg.format.lower():
            log.warning("unsupported compressed format %r — expected jpeg", msg.format)
            return
        if self.caps is None:
            self.caps = Gst.Caps.from_string(f"image/jpeg,framerate={FPS}/1")
        if sess.appsrc is not self._capped_src:
            sess.appsrc.set_property("caps", self.caps)
            self._capped_src = sess.appsrc
        sess.appsrc.emit("push-buffer", Gst.Buffer.new_wrapped(bytes(msg.data)))

    # format -> bytes per pixel, needed to check the row stride
    _ROS_TO_GST = {"bgra8": ("BGRA", 4), "rgba8": ("RGBA", 4),
                   "bgr8": ("BGR", 3), "rgb8": ("RGB", 3),
                   "mono8": ("GRAY8", 1)}

    def _on_image(self, msg) -> None:
        self.frames += 1
        self.last_frame = time.monotonic()
        self._arrivals.append(self.last_frame)
        sess = self.agent.session
        if not sess or not sess.appsrc:
            return  # not publishing — drop the frame, don't queue it

        mapped = self._ROS_TO_GST.get(msg.encoding)
        if mapped is None:
            if not self._warned_encoding:
                log.warning("unsupported image encoding %s", msg.encoding)
                self._warned_encoding = True
            return
        fmt, bpp = mapped

        # A ROS image may be padded: msg.step is the real row length and can be
        # larger than width*bpp. The caps below describe a tightly packed frame,
        # so a padded buffer must be repacked — handing GStreamer a buffer whose
        # layout does not match its caps makes it read past the end and the
        # process dies with SIGSEGV rather than an error.
        row = msg.width * bpp
        data = bytes(msg.data)
        if msg.step != row:
            data = b"".join(data[i * msg.step:i * msg.step + row]
                            for i in range(msg.height))

        want = row * msg.height
        if len(data) != want:
            if not self._warned_size:
                log.error("frame is %d bytes, expected %d for %dx%d %s — dropping",
                          len(data), want, msg.width, msg.height, msg.encoding)
                self._warned_size = True
            return

        if self.caps is None:
            self.caps = Gst.Caps.from_string(
                f"video/x-raw,format={fmt},width={msg.width},height={msg.height},"
                f"framerate={FPS}/1")
            log.info("first frame: %dx%d %s (step %d)",
                     msg.width, msg.height, msg.encoding, msg.step)
        if sess.appsrc is not self._capped_src:
            sess.appsrc.set_property("caps", self.caps)
            self._capped_src = sess.appsrc

        sess.appsrc.emit("push-buffer", Gst.Buffer.new_wrapped(data))

    def publish_cmd_vel(self, lin: float, ang: float) -> None:
        t = self.Twist()
        t.linear.x = lin
        t.angular.z = ang
        self.cmd_pub.publish(t)


# --------------------------------------------------------------------------
# agent
# --------------------------------------------------------------------------

class Agent:
    def __init__(self) -> None:
        self.session: Optional[WhipSession] = None
        self.ros: Optional[RosBridge] = None
        self.loop = GLib.MainLoop()
        self._last_teleop = 0.0
        self._lock = threading.Lock()
        self._cached_publish: Optional[tuple] = None
        self._camera_fault = False
        self._stopping = False
        # True while a start is in progress, so a dead endpoint discovered
        # during one can be replaced immediately rather than failing the start.
        self._starting = False
        self._reprovisioned = False
        self._retries = 0
        self._last_publish: Optional[tuple] = None

    # -- MQTT --------------------------------------------------------------

    def on_connect(self, client, *_args) -> None:
        client.subscribe([(TOPIC_LIVESTREAM, 1), (TOPIC_TELEOP, 0)])
        log.info("MQTT connected, subscribed to %s and %s", TOPIC_LIVESTREAM, TOPIC_TELEOP)

    def on_message(self, _client, _userdata, msg) -> None:
        try:
            payload = json.loads(msg.payload.decode())
        except Exception:  # noqa: BLE001
            log.warning("undecodable payload on %s", msg.topic)
            return

        if msg.topic == TOPIC_LIVESTREAM:
            kind = payload.get("type")
            if kind == "CMD_LIVESTREAM_ENABLE":
                # BC's start command carries no payload: the vehicle already
                # knows where to publish, from its provisioned config. A
                # whip_url in the message still wins if one is sent.
                GLib.idle_add(once(self.enable,
                                   payload.get("whip_url") or WHIP_URL,
                                   payload.get("publish_token") or PUBLISH_TOKEN))
            elif kind == "CMD_LIVESTREAM_DISABLE":
                GLib.idle_add(once(self.disable))
            else:
                log.warning("unknown livestream command %r", kind)
        elif msg.topic == TOPIC_TELEOP:
            self.teleop(payload)

    # -- livestream --------------------------------------------------------

    def report_fault(self, code: str, detail: str = "") -> None:
        """Tell the server what only the vehicle can see. Best effort: a fault
        report that fails must never take the stream down with it."""
        if not (STREAM_API_URL and PROVISION_TOKEN):
            return
        path = "clear" if code == "clear" else ""
        url = f"{STREAM_API_URL}/{VID}/livestream/fault"
        if path:
            url += "/" + path
        try:
            requests.post(
                url,
                json=None if path else {"code": code, "detail": detail},
                headers={"Authorization": f"Bearer {PROVISION_TOKEN}"},
                timeout=5)
        except Exception as exc:  # noqa: BLE001
            log.warning("could not report fault %s: %s", code, exc)

    def _frame_watchdog(self) -> bool:
        """Frames stop but the session stays up — the one failure WebRTC cannot
        signal, because nothing is wrong with the connection."""
        if not self.session or not self.ros:
            return True
        last = self.ros.last_frame
        if last == 0.0:
            return True
        stalled = time.monotonic() - last
        if stalled > FRAME_STALL_S and not self._camera_fault:
            self._camera_fault = True
            log.error("no camera frame for %.0fs — reporting ERR-001", stalled)
            detail = f"no frame on {ZED_TOPIC} for {stalled:.0f}s"
            threading.Thread(target=self.report_fault,
                             args=("ERR-001", detail), daemon=True).start()
        elif stalled <= FRAME_STALL_S and self._camera_fault:
            self._camera_fault = False
            log.info("camera frames resumed")
            threading.Thread(target=self.report_fault,
                             args=("clear",), daemon=True).start()
        return True

    def forget_publish(self) -> None:
        """The endpoint we were using has gone. Drop it and, if this happened
        while trying to start, ask the server for a new one straight away.

        The server ends an unwatched stream by removing the ingress, which
        leaves the vehicle holding a URL that no longer resolves. Without the
        retry, the next start posts to the dead endpoint, fails, and only the
        start after that works — so every stream following an automatic close
        needs the command sent twice."""
        if self._cached_publish:
            log.info("the publish endpoint has gone — forgetting it")
        self._cached_publish = None

        if self._starting and not self._reprovisioned:
            self._reprovisioned = True
            log.info("asking the server for a new endpoint and starting again")
            GLib.idle_add(once(self.enable, None, ""))

    def resolve_publish(self) -> tuple:
        """Return (whip_url, token). Configured values win; otherwise ask the
        Stream API for this vehicle's own endpoint. Cached after the first
        success so a later API outage does not stop a stream starting."""
        if WHIP_URL:
            return WHIP_URL, PUBLISH_TOKEN
        if self._cached_publish:
            return self._cached_publish
        if not (STREAM_API_URL and PROVISION_TOKEN):
            return None, ""
        url = f"{STREAM_API_URL}/{VID}/livestream/provision"
        try:
            r = requests.post(
                url, headers={"Authorization": f"Bearer {PROVISION_TOKEN}"}, timeout=10)
            r.raise_for_status()
            data = r.json()
            self._cached_publish = (data["whip_url"], data.get("publish_token", ""))
            log.info("provisioned from %s", STREAM_API_URL)
            return self._cached_publish
        except Exception as exc:  # noqa: BLE001
            log.error("could not provision from %s: %s", url, exc)
            return None, ""

    def enable(self, whip_url: Optional[str], token: str, _waited: float = 0.0) -> bool:
        # A ROS source has no caps until a frame arrives. On a slow topic that
        # can be half a second or more, and negotiating before then leaves
        # webrtcbin with nothing to offer.
        # Three frames rather than one: enough to measure the rate the camera
        # is really running at, which sizes the keyframe interval. At 3 fps
        # that is one extra second; getting the interval wrong costs a viewer
        # ten seconds of blank screen on every join.
        if SOURCE == "ros" and self.ros is not None and self.ros.frames < 3:
            if _waited < FIRST_FRAME_WAIT_S:
                if _waited == 0.0:
                    log.info("waiting for the first frames on %s", ZED_TOPIC)
                GLib.timeout_add(250, once(self.enable, whip_url, token,
                                           _waited + 0.25))
                return False
            if self.ros.frames == 0:
                log.error("no frame on %s after %.0fs — is it publishing?",
                          ZED_TOPIC, FIRST_FRAME_WAIT_S)
                return False
            log.warning("only %d frame(s) on %s in %.0fs — the camera is very "
                        "slow; continuing", self.ros.frames, ZED_TOPIC,
                        FIRST_FRAME_WAIT_S)

        if not whip_url:
            whip_url, token = self.resolve_publish()
        if not whip_url:
            log.error("ENABLE but no publish URL. Set WHIP_URL, or set "
                      "STREAM_API_URL and PROVISION_TOKEN so this vehicle can "
                      "fetch its own.")
            self._starting = False
            return False
        self._starting = True
        with self._lock:
            if self.session and self.session.whip_url == whip_url:
                # Already doing exactly this. Tearing the session down and
                # rebuilding it would interrupt everyone watching, to arrive
                # back where we started — and a duplicate start is far more
                # likely to be a retry than a real request to restart.
                log.info("already publishing to %s — nothing to do", whip_url)
                return False
            if self.session:
                log.info("already publishing — restarting against the new URL")
                self.session.stop("re-enable")
                self.session = None
            log.info("ENABLE -> %s", whip_url)
            self.session = WhipSession(whip_url, token, self._session_died,
                                       self.forget_publish,
                                       self._publish_holding)
            self._last_publish = (whip_url, token)
            try:
                self.session.start(self.ros.measured_fps() if self.ros else None)
            except Exception as exc:  # noqa: BLE001
                log.exception("failed to start session: %s", exc)
                self.session = None
                self._starting = False
        return False

    def disable(self) -> bool:
        with self._lock:
            # Marks the teardown as asked-for, so the death of the session
            # below is not mistaken for a dropped uplink and retried.
            self._stopping = True
            self._starting = False
            self._reprovisioned = False
            self._retries = 0
            self._last_publish = None
            if self.session:
                self.session.stop("CMD_LIVESTREAM_DISABLE")
                self.session = None
            else:
                log.info("DISABLE with nothing publishing — no-op")
            self._stopping = False
        return False

    def _publish_holding(self) -> None:
        if self._retries:
            log.info("publish re-established after %d attempt(s)", self._retries)
        self._retries = 0
        self._starting = False
        self._reprovisioned = False

    def _session_died(self) -> None:
        """The publish ended without being asked to. Retry with a widening gap
        rather than waiting for a human: on a vehicle the usual cause is the
        uplink dropping for a few seconds, and nobody is there to notice.

        Backing off matters as much as retrying. A server that is down comes
        back faster if ten vehicles are not hammering it every second, and a
        SIM with no signal is not helped by trying again immediately."""
        with self._lock:
            self.session = None
            deliberate = self._stopping
            target = self._last_publish

        if deliberate or not target:
            log.info("idle again, waiting for the next ENABLE")
            return

        # Ask the server whether it still wants this stream before retrying.
        # The server ends an unwatched stream by removing the ingress, which
        # reaches the vehicle as a dropped session and is indistinguishable
        # from a lost uplink. Retrying one of those is right; retrying the
        # other re-provisions a fresh ingress and brings back the stream the
        # server just closed, which would defeat the idle timeout entirely.
        threading.Thread(target=self._retry_if_wanted, daemon=True).start()

    def _retry_if_wanted(self) -> None:
        if STREAM_API_URL and PROVISION_TOKEN:
            try:
                r = requests.get(
                    f"{STREAM_API_URL}/{VID}/livestream/state",
                    headers={"Authorization": f"Bearer {PROVISION_TOKEN}"},
                    timeout=5)
                if r.ok and r.json().get("state") == "DISABLED":
                    log.info("the server has ended this stream — going idle "
                             "rather than reconnecting")
                    self._retries = 0
                    self.forget_publish()
                    return
            except Exception as exc:  # noqa: BLE001
                # Unreachable is not the same as "stopped". If the server is
                # down, the vehicle should keep trying to come back.
                log.info("could not reach the server to check (%s) — "
                         "treating this as a dropped link", exc)

        GLib.idle_add(once(self._schedule_retry))

    def _schedule_retry(self) -> None:
        target = self._last_publish
        if not target:
            return
        self._retries += 1
        if self._retries > RECONNECT_MAX_TRIES:
            log.error("publish failed %d times — giving up until told to start "
                      "again", RECONNECT_MAX_TRIES)
            self._retries = 0
            threading.Thread(target=self.report_fault,
                             args=("ERR-003", "publish lost and could not be "
                                   "re-established"), daemon=True).start()
            return

        delay = RECONNECT_BASE_S * (2 ** (self._retries - 1))
        log.warning("publish ended unexpectedly — retrying in %.0fs (%d/%d)",
                    delay, self._retries, RECONNECT_MAX_TRIES)
        whip_url, token = target
        GLib.timeout_add(int(delay * 1000),
                         once(self.enable, whip_url, token))

    # -- teleop ------------------------------------------------------------

    def teleop(self, payload: dict) -> None:
        lin = payload.get("lin", payload.get("linear_x", payload.get("linear", 0.0)))
        ang = payload.get("ang", payload.get("angular_z", payload.get("angular", 0.0)))
        try:
            lin = max(-MAX_LIN, min(MAX_LIN, float(lin)))
            ang = max(-MAX_ANG, min(MAX_ANG, float(ang)))
        except (TypeError, ValueError):
            return
        self._last_teleop = time.monotonic()
        if self.ros:
            self.ros.publish_cmd_vel(lin, ang)

    def _deadman(self) -> bool:
        """No teleop message within DEADMAN_S -> publish a zero Twist."""
        if self.ros and self._last_teleop and \
                time.monotonic() - self._last_teleop > DEADMAN_S:
            self.ros.publish_cmd_vel(0.0, 0.0)
            self._last_teleop = 0.0
        return True

    # -- run ---------------------------------------------------------------

    def run(self) -> None:
        Gst.init(None)

        want_video = SOURCE == "ros"
        want_control = CONTROL == "ros" and not AUTOSTART
        if want_video or want_control:
            self.ros = RosBridge(self, want_video, want_control)

        if AUTOSTART:
            # Benchmark / bench-test mode: publish immediately, no MQTT and no
            # BC. WHIP_URL must be set, or STREAM_API_URL + PROVISION_TOKEN.
            log.info("AUTOSTART — publishing without waiting for a command")
            GLib.idle_add(once(self.enable, WHIP_URL or None, PUBLISH_TOKEN))
            if not (WHIP_URL or (STREAM_API_URL and PROVISION_TOKEN)):
                log.error("AUTOSTART needs WHIP_URL (or STREAM_API_URL + PROVISION_TOKEN)")
            signal.signal(signal.SIGINT, lambda *_: (self.disable(), self.loop.quit()))
            signal.signal(signal.SIGTERM, lambda *_: (self.disable(), self.loop.quit()))
            try:
                self.loop.run()
            finally:
                pass
            return

        if CONTROL == "ros":
            # The vehicle's existing bridge owns the broker connection and
            # republishes BC's command onto CONTROL_TOPIC. Opening a second
            # connection here would duplicate that and risk a client-id clash.
            GLib.timeout_add(int(DEADMAN_S * 1000 / 2), self._deadman)
            GLib.timeout_add_seconds(1, self._frame_watchdog)

            def shutdown_ros(signum=None, _frame=None):
                # Name the signal and who is still around to have sent it.
                # "shutting down" on its own says the process is leaving but
                # not why, and a stop that quietly ends the whole agent looks
                # from the outside like a command being ignored: ROS drops a
                # message published while nothing is subscribed, so the next
                # start lands in the gap and appears to have done nothing.
                name = signal.Signals(signum).name if signum else "request"
                log.info("shutting down on %s (parent pid %d)",
                         name, os.getppid())
                self.disable()
                self.loop.quit()

            signal.signal(signal.SIGINT, shutdown_ros)
            signal.signal(signal.SIGTERM, shutdown_ros)
            log.info("vehicle agent idle — vid=%s source=%s control=%s",
                     VID, SOURCE, CONTROL_TOPIC)
            try:
                self.loop.run()
            finally:
                if self.ros:
                    self.ros.stop()
            return

        import paho.mqtt.client as mqtt   # only needed for CONTROL=mqtt

        # Must differ from the vehicle stack's own MQTT_CLIENT_ID — a broker
        # disconnects the older session when a second one claims the same id.
        cid = f"{MQTT_CLIENT_ID}-livestream"
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=cid) \
            if hasattr(mqtt, "CallbackAPIVersion") else mqtt.Client(client_id=cid)
        if MQTT_USER:
            client.username_pw_set(MQTT_USER, MQTT_PASS)
        if MQTT_TLS:
            client.tls_set()          # system CA store; the broker uses a real cert
        client.on_connect = self.on_connect
        client.on_message = self.on_message
        client.will_set(f"{VID}/status/livestream", json.dumps({"online": False}), retain=True)
        client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=30)
        client.loop_start()

        GLib.timeout_add(int(DEADMAN_S * 1000 / 2), self._deadman)

        def shutdown(signum=None, _frame=None):
            name = signal.Signals(signum).name if signum else "request"
            log.info("shutting down on %s (parent pid %d)", name, os.getppid())
            self.disable()
            self.loop.quit()

        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)

        log.info("vehicle agent idle — vid=%s source=%s broker=%s:%s",
                 VID, SOURCE, MQTT_HOST, MQTT_PORT)
        try:
            self.loop.run()
        finally:
            client.loop_stop()


if __name__ == "__main__":
    sys.exit(Agent().run())
