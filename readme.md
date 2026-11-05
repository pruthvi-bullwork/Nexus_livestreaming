# bullwork-livestream

Publishes a vehicle's camera to the fleet streaming server over WHIP. Runs
continuously under supervisor and idles until commanded.

## What is here

| | |
|---|---|
| `vehicle_agent.py` | the agent |
| `packaging/bullwork-livestream` | launcher: layers config, sources ROS, starts the agent |
| `packaging/fleet.env` | fleet-wide settings, installed to `/etc/bullwork-livestream/` |

Supervisor configuration belongs to the platform, not to this package. The
platform's program should run `/usr/bin/bullwork-livestream`.

## Control

Start and stop arrive on a ROS topic, republished from MQTT by the vehicle's
existing bridge:

    ros2 topic pub --once /livestream_control std_msgs/String "data: 'start'"
    ros2 topic pub --once /livestream_control std_msgs/String "data: 'stop'"
    ros2 topic pub --once /livestream_control std_msgs/String "data: 'restart'"

`1`/`0` and `true`/`false` also work.

**Supervisor must not start and stop this program on a livestream command.**
The agent is always up and moves itself in and out of a publish session. A
program that gets cycled instead takes 20-30 seconds to return, and any
command published during that window is dropped, because ROS discards a
message with no subscriber. This was a real failure: a stop killed the agent
and the next start appeared to be ignored.

## Configuration

Nothing is per-vehicle. `fleet.env` carries the fleet-wide values and the
vehicle id comes from `VEHICLE_ID` in the unit's own `.env_vars`; the agent
asks the server for its own publish URL using it.

One value is not in the repository and must be set at install time:

    sudo sed -i "s|^PROVISION_TOKEN=.*|PROVISION_TOKEN=$TOKEN|" \
         /etc/bullwork-livestream/fleet.env

It can start, stop and view every vehicle, so it does not belong in git —
history is permanent, and removing it in a later commit does not remove it
from a clone.

## Package dependencies

Whatever builds the `.deb` must declare these. Two are not obvious:

    python3, python3-gi, python3-requests,
    gstreamer1.0-plugins-base, gstreamer1.0-plugins-good,
    gstreamer1.0-plugins-bad, gstreamer1.0-nice,
    gir1.2-gst-plugins-bad-1.0

Recommends: `gstreamer1.0-plugins-ugly` (software encode fallback),
`python3-paho-mqtt` (only needed if `CONTROL=mqtt`).

**`gstreamer1.0-nice`** supplies `nicesrc`. Without it `webrtcbin` fails with a
caps error that says nothing about ICE — this cost time on three separate
Jetsons.

**`gir1.2-gst-plugins-bad-1.0`** is the Python binding and is a separate
package from the plugin itself.

`nvv4l2h264enc` comes from JetPack and cannot be a dependency. Without it the
agent falls back to software encode, which works but uses noticeably more CPU.

## Installed layout

    /usr/bin/bullwork-livestream          launcher
    /opt/bullwork-livestream/vehicle_agent.py
    /etc/bullwork-livestream/fleet.env    configuration

## Measured

On an Orin with the ZED relay at ~10 fps, publishing to a server in
`us-central1`:

| | |
|---|---|
| Cost above idle | +3.2% CPU, +0.57 W |
| Encode | NVENC, hardware |
| Delivered | 960x600, ~10 fps, 0.00-0.05% loss |
| Data | ~0.8 GB/hour per viewer |
| Start to connected | ~4 s |

The encoder runs in hardware, so the CPU figure is the ROS subscription and
buffer handling, not compression.
