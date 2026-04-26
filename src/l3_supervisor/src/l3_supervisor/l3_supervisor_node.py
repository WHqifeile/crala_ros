from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from importlib import import_module
from typing import Any, Optional

from autoware_vehicle_msgs.msg import ControlModeReport
from autoware_vehicle_msgs.msg import VelocityReport
from l3_interfaces.msg import HazardContext
import rclpy
from geometry_msgs.msg import Vector3Stamped
from rclpy.executors import ExternalShutdownException
from rclpy.node import Node
from rosgraph_msgs.msg import Clock
from std_msgs.msg import Bool
from std_msgs.msg import String
from tier4_vehicle_msgs.msg import ActuationCommandStamped


class L3State(str, Enum):
    MANUAL = "MANUAL"
    STANDBY = "STANDBY"
    AVAILABLE = "AVAILABLE"
    ACTIVE = "ACTIVE"
    TOR = "TOR"
    MRM = "MRM"
    FAULT = "FAULT"


@dataclass
class DriverSnapshot:
    available: bool = False
    drowsy: bool = False
    distracted: bool = False
    action_label: str = "unknown"
    head_pose_stamp_sec: Optional[float] = None
    gaze_stamp_sec: Optional[float] = None


class L3SupervisorNode(Node):
    def __init__(self) -> None:
        super().__init__("l3_supervisor")
        self._declare_parameters()

        self.role_name = self._get_str("role_name")
        self.publish_rate_hz = self._get_float("publish_rate_hz")
        self.driver_timeout_sec = self._get_float("driver_timeout_sec")
        self.carla_timeout_sec = self._get_float("carla_timeout_sec")
        self.scenario_timeout_sec = self._get_float("scenario_timeout_sec")
        self.driver_ready_hold_sec = self._get_float("driver_ready_hold_sec")
        self.driver_unready_grace_sec = self._get_float("driver_unready_grace_sec")
        self.scenario_td_hold_sec = self._get_float("scenario_td_hold_sec")
        self.tor_timeout_sec = self._get_float("tor_timeout_sec")
        self.takeover_steer_threshold = self._get_float("takeover_steer_threshold")
        self.takeover_pedal_threshold = self._get_float("takeover_pedal_threshold")
        self.manual_cmd_timeout_sec = self._get_float("manual_cmd_timeout_sec")
        self.takeover_event_hold_sec = self._get_float("takeover_event_hold_sec")
        self.min_l3_speed_mps = self._get_float("min_l3_speed_mps")
        self.standstill_speed_threshold_mps = self._get_float("standstill_speed_threshold_mps")
        self.mrm_brake = self._get_float("mrm_brake")
        self.mrm_throttle = self._get_float("mrm_throttle")
        self.mrm_steer = self._get_float("mrm_steer")
        self.mrm_hand_brake_at_standstill = self._get_bool("mrm_hand_brake_at_standstill")
        self.mrm_force_control_mode = self._get_bool("mrm_force_control_mode")
        self.active_uses_autoware = self._get_bool("active_uses_autoware")
        self.tor_keeps_autoware = self._get_bool("tor_keeps_autoware")
        self.autoware_backend = self._get_str("autoware_backend")
        self.autoware_engage_topic = self._get_str("autoware_engage_topic")
        self.autoware_operation_mode_state_topic = self._get_str("autoware_operation_mode_state_topic")
        self.autoware_change_to_autonomous_service = self._get_str("autoware_change_to_autonomous_service")
        self.autoware_change_to_stop_service = self._get_str("autoware_change_to_stop_service")
        self.autoware_enable_control_service = self._get_str("autoware_enable_control_service")
        self.autoware_disable_control_service = self._get_str("autoware_disable_control_service")
        self.require_automation_request = self._get_bool("require_automation_request")
        self.require_driver_monitoring = self._get_bool("require_driver_monitoring")
        self.auto_engage = self._get_bool("auto_engage")
        self.auto_activate_on_request = self._get_bool("auto_activate_on_request")
        self.default_system_available = self._get_bool("default_system_available")

        self.driver = DriverSnapshot()
        self.vehicle_speed_mps_value = 0.0
        self.control_mode = ControlModeReport()

        self.system_available = self.default_system_available
        self.automation_request = False
        self.scenario_td_request = False
        self.force_mrm = False
        self.reset_mrm_request = False

        self.latest_driver_stamp_sec: Optional[float] = None
        self.latest_status_stamp_sec: Optional[float] = None
        self.latest_vehicle_status_stamp_sec: Optional[float] = None
        self.latest_control_mode_stamp_sec: Optional[float] = None
        self.latest_system_available_stamp_sec: Optional[float] = None
        self.latest_automation_request_stamp_sec: Optional[float] = None
        self.latest_td_request_stamp_sec: Optional[float] = None
        self.latest_force_mrm_stamp_sec: Optional[float] = None
        self.latest_reset_mrm_stamp_sec: Optional[float] = None
        self.latest_scene_available_stamp_sec: Optional[float] = None
        self.latest_hazard_stamp_sec: Optional[float] = None

        self.state = L3State.MANUAL
        self.state_reason = "startup"
        self.tor_reason = "idle"
        self.state_enter_time = self._now_sec()
        self.ready_since: Optional[float] = None
        self.unready_since: Optional[float] = None
        self.td_since: Optional[float] = None
        self.takeover_completed_until = 0.0
        self.autoware_enabled = False
        self.autoware_status_summary = "unconfigured"
        self.autoware_state_mode = "unknown"
        self.autoware_state_in_transition = False
        self.autoware_state_control_enabled = False
        self.autoware_control_available = False
        self._autoware_msg_cls = None
        self._autoware_mode_state_cls = None
        self._autoware_change_srv_cls = None
        self._autoware_engage_pub = None
        self._autoware_mode_state_sub = None
        self._autoware_enable_control_client = None
        self._autoware_disable_control_client = None
        self._autoware_change_to_autonomous_client = None
        self._autoware_change_to_stop_client = None
        self._autoware_pending_future = None
        self._autoware_pending_label: Optional[str] = None
        self.scene_available = False
        self.hazard_takeover_required = False
        self.hazard_reason_code = "CLEAR"
        self.hazard_distance_m: Optional[float] = None
        self.hazard_ttc_s: Optional[float] = None

        self.automation_allowed_pub = self.create_publisher(Bool, "/l3/automation_allowed", 10)
        self.driver_ready_pub = self.create_publisher(Bool, "/l3/driver_ready", 10)
        self.automation_engaged_pub = self.create_publisher(Bool, "/l3/automation_engaged", 10)
        self.system_available_pub = self.create_publisher(Bool, "/l3/system_available", 10)
        self.td_request_pub = self.create_publisher(Bool, "/l3/td_request", 10)
        self.takeover_completed_pub = self.create_publisher(Bool, "/l3/takeover_completed", 10)
        self.mrm_request_pub = self.create_publisher(Bool, "/l3/mrm_request", 10)
        self.state_pub = self.create_publisher(String, "/l3/state", 10)
        self.state_reason_pub = self.create_publisher(String, "/l3/state_reason", 10)
        self.tor_reason_pub = self.create_publisher(String, "/l3/tor_reason", 10)
        self.hmi_status_pub = self.create_publisher(String, "/hmi/status_text", 10)
        self.mrm_control_pub = self.create_publisher(
            ActuationCommandStamped,
            "/control/command/actuation_cmd",
            10,
        )

        self.create_subscription(Bool, "/driver/available", self._on_driver_available, 10)
        self.create_subscription(Bool, "/driver/drowsy", self._on_driver_drowsy, 10)
        self.create_subscription(Bool, "/driver/distracted", self._on_driver_distracted, 10)
        self.create_subscription(String, "/driver/action_label", self._on_driver_action_label, 10)
        self.create_subscription(Vector3Stamped, "/driver/head_pose", self._on_driver_head_pose, 10)
        self.create_subscription(Vector3Stamped, "/driver/gaze_vector", self._on_driver_gaze, 10)
        self.create_subscription(Clock, "/clock", self._on_clock, 10)
        self.create_subscription(Bool, "/carla/scene/available", self._on_carla_scene_available, 10)
        self.create_subscription(HazardContext, "/carla/hazard/context", self._on_hazard_context, 10)
        self.create_subscription(VelocityReport, "/vehicle/status/velocity_status", self._on_velocity_status, 10)
        self.create_subscription(ControlModeReport, "/vehicle/status/control_mode", self._on_control_mode, 10)
        self.create_subscription(Bool, "/scenario/system_available", self._on_system_available, 10)
        self.create_subscription(Bool, "/scenario/automation_request", self._on_automation_request, 10)
        self.create_subscription(Bool, "/scenario/td_request", self._on_scenario_td_request, 10)
        self.create_subscription(Bool, "/scenario/force_mrm", self._on_force_mrm, 10)
        self.create_subscription(Bool, "/scenario/reset_mrm", self._on_reset_mrm, 10)

        self._setup_autoware_interface()
        self.timer = self.create_timer(1.0 / max(self.publish_rate_hz, 1.0), self._tick)
        self.get_logger().info("L3 supervisor started.")

    def _declare_parameters(self) -> None:
        self.declare_parameter("role_name", "ego_vehicle")
        self.declare_parameter("publish_rate_hz", 10.0)
        self.declare_parameter("driver_timeout_sec", 1.0)
        self.declare_parameter("carla_timeout_sec", 1.0)
        self.declare_parameter("scenario_timeout_sec", 1.0)
        self.declare_parameter("driver_ready_hold_sec", 2.0)
        self.declare_parameter("driver_unready_grace_sec", 1.5)
        self.declare_parameter("scenario_td_hold_sec", 0.5)
        self.declare_parameter("tor_timeout_sec", 5.0)
        self.declare_parameter("takeover_steer_threshold", 0.05)
        self.declare_parameter("takeover_pedal_threshold", 0.05)
        self.declare_parameter("manual_cmd_timeout_sec", 0.5)
        self.declare_parameter("takeover_event_hold_sec", 1.0)
        self.declare_parameter("min_l3_speed_mps", 0.0)
        self.declare_parameter("standstill_speed_threshold_mps", 0.2)
        self.declare_parameter("mrm_brake", 0.35)
        self.declare_parameter("mrm_throttle", 0.0)
        self.declare_parameter("mrm_steer", 0.0)
        self.declare_parameter("mrm_hand_brake_at_standstill", True)
        self.declare_parameter("mrm_force_control_mode", False)
        self.declare_parameter("active_uses_autoware", True)
        self.declare_parameter("tor_keeps_autoware", True)
        self.declare_parameter("autoware_backend", "auto")
        self.declare_parameter("autoware_engage_topic", "/autoware/engage")
        self.declare_parameter("autoware_operation_mode_state_topic", "/api/operation_mode/state")
        self.declare_parameter("autoware_change_to_autonomous_service", "/api/operation_mode/change_to_autonomous")
        self.declare_parameter("autoware_change_to_stop_service", "/api/operation_mode/change_to_stop")
        self.declare_parameter("autoware_enable_control_service", "/api/operation_mode/enable_autoware_control")
        self.declare_parameter("autoware_disable_control_service", "/api/operation_mode/disable_autoware_control")
        self.declare_parameter("require_automation_request", False)
        self.declare_parameter("require_driver_monitoring", False)
        self.declare_parameter("auto_engage", True)
        self.declare_parameter("auto_activate_on_request", True)
        self.declare_parameter("default_system_available", True)

    def _get_bool(self, name: str) -> bool:
        return bool(self.get_parameter(name).value)

    def _get_float(self, name: str) -> float:
        return float(self.get_parameter(name).value)

    def _get_str(self, name: str) -> str:
        return str(self.get_parameter(name).value)

    def _now_sec(self) -> float:
        return self.get_clock().now().nanoseconds / 1e9

    def _mark_driver_update(self) -> None:
        self.latest_driver_stamp_sec = self._now_sec()

    def _mark_scenario_update(self, field: str) -> None:
        now_sec = self._now_sec()
        setattr(self, field, now_sec)

    def _on_driver_available(self, msg: Bool) -> None:
        self.driver.available = msg.data
        self._mark_driver_update()

    def _on_driver_drowsy(self, msg: Bool) -> None:
        self.driver.drowsy = msg.data
        self._mark_driver_update()

    def _on_driver_distracted(self, msg: Bool) -> None:
        self.driver.distracted = msg.data
        self._mark_driver_update()

    def _on_driver_action_label(self, msg: String) -> None:
        self.driver.action_label = msg.data or "unknown"
        self._mark_driver_update()

    def _on_driver_head_pose(self, msg: Vector3Stamped) -> None:
        self.driver.head_pose_stamp_sec = self._now_sec()
        self._mark_driver_update()

    def _on_driver_gaze(self, msg: Vector3Stamped) -> None:
        self.driver.gaze_stamp_sec = self._now_sec()
        self._mark_driver_update()

    def _on_clock(self, msg: Clock) -> None:
        self.latest_status_stamp_sec = self._now_sec()

    def _on_carla_scene_available(self, msg: Bool) -> None:
        self.scene_available = msg.data
        self.latest_scene_available_stamp_sec = self._now_sec()

    def _on_hazard_context(self, msg: HazardContext) -> None:
        self.hazard_takeover_required = bool(msg.takeover_required)
        self.hazard_reason_code = msg.reason_code or "CLEAR"
        self.hazard_distance_m = float(msg.hazard_distance_m) if msg.hazard_distance_m == msg.hazard_distance_m else None
        self.hazard_ttc_s = float(msg.hazard_ttc_s) if msg.hazard_ttc_s == msg.hazard_ttc_s else None
        self.latest_hazard_stamp_sec = self._now_sec()

    def _on_velocity_status(self, msg: VelocityReport) -> None:
        self.vehicle_speed_mps_value = float(msg.longitudinal_velocity)
        self.latest_vehicle_status_stamp_sec = self._now_sec()

    def _on_control_mode(self, msg: ControlModeReport) -> None:
        self.control_mode = msg
        self.latest_control_mode_stamp_sec = self._now_sec()

    def _on_system_available(self, msg: Bool) -> None:
        self.system_available = msg.data
        self._mark_scenario_update("latest_system_available_stamp_sec")

    def _on_automation_request(self, msg: Bool) -> None:
        self.automation_request = msg.data
        self._mark_scenario_update("latest_automation_request_stamp_sec")

    def _on_scenario_td_request(self, msg: Bool) -> None:
        self.scenario_td_request = msg.data
        self._mark_scenario_update("latest_td_request_stamp_sec")

    def _on_force_mrm(self, msg: Bool) -> None:
        self.force_mrm = msg.data
        self._mark_scenario_update("latest_force_mrm_stamp_sec")

    def _on_reset_mrm(self, msg: Bool) -> None:
        self.reset_mrm_request = msg.data
        self._mark_scenario_update("latest_reset_mrm_stamp_sec")

    def _setup_autoware_interface(self) -> None:
        backend = self.autoware_backend.strip().lower()
        if backend in ("auto", "adapi"):
            if self._setup_autoware_adapi():
                return
            if backend == "adapi":
                self.autoware_status_summary = "adapi_unavailable"
                self.get_logger().warning("Autoware AD API requested but not available in the sourced environment.")
                return
        if backend in ("auto", "engage"):
            if self._setup_autoware_engage_topic():
                return
            if backend == "engage":
                self.autoware_status_summary = "engage_unavailable"
                self.get_logger().warning("Autoware engage topic requested but message package is not available.")
                return
        self.autoware_status_summary = "autoware_interface_unavailable"
        self.get_logger().warning(
            "No Autoware control interface detected. Source an Autoware workspace to enable official control handoff."
        )

    def _setup_autoware_adapi(self) -> bool:
        try:
            msg_module = import_module("autoware_adapi_v1_msgs.msg")
            srv_module = import_module("autoware_adapi_v1_msgs.srv")
        except ModuleNotFoundError:
            return False

        self._autoware_mode_state_cls = getattr(msg_module, "OperationModeState", None)
        self._autoware_change_srv_cls = getattr(srv_module, "ChangeOperationMode", None)
        if self._autoware_mode_state_cls is None or self._autoware_change_srv_cls is None:
            return False

        self._autoware_mode_state_sub = self.create_subscription(
            self._autoware_mode_state_cls,
            self.autoware_operation_mode_state_topic,
            self._on_autoware_mode_state,
            10,
        )
        self._autoware_enable_control_client = self.create_client(
            self._autoware_change_srv_cls,
            self.autoware_enable_control_service,
        )
        self._autoware_disable_control_client = self.create_client(
            self._autoware_change_srv_cls,
            self.autoware_disable_control_service,
        )
        self._autoware_change_to_autonomous_client = self.create_client(
            self._autoware_change_srv_cls,
            self.autoware_change_to_autonomous_service,
        )
        self._autoware_change_to_stop_client = self.create_client(
            self._autoware_change_srv_cls,
            self.autoware_change_to_stop_service,
        )
        self.autoware_status_summary = "adapi"
        self.get_logger().info("Autoware AD API integration enabled.")
        return True

    def _setup_autoware_engage_topic(self) -> bool:
        for module_name in ("autoware_vehicle_msgs.msg", "autoware_auto_vehicle_msgs.msg"):
            try:
                msg_module = import_module(module_name)
            except ModuleNotFoundError:
                continue
            engage_cls = getattr(msg_module, "Engage", None)
            if engage_cls is None:
                continue
            self._autoware_msg_cls = engage_cls
            self._autoware_engage_pub = self.create_publisher(engage_cls, self.autoware_engage_topic, 10)
            self.autoware_status_summary = f"engage_topic:{module_name}"
            self.get_logger().info(f"Autoware engage-topic integration enabled via {module_name}.")
            return True
        return False

    def _on_autoware_mode_state(self, msg: Any) -> None:
        mode_value = getattr(msg, "mode", None)
        self.autoware_state_mode = self._autoware_mode_to_string(mode_value)
        self.autoware_state_in_transition = bool(getattr(msg, "is_in_transition", False))
        self.autoware_state_control_enabled = bool(getattr(msg, "is_autoware_control_enabled", False))
        self.autoware_control_available = True

    def _autoware_mode_to_string(self, value: Any) -> str:
        if value is None or self._autoware_mode_state_cls is None:
            return "unknown"
        mode_constants = (
            ("STOP", "STOP"),
            ("AUTONOMOUS", "AUTONOMOUS"),
            ("LOCAL", "LOCAL"),
            ("REMOTE", "REMOTE"),
        )
        for attr_name, label in mode_constants:
            if hasattr(self._autoware_mode_state_cls, attr_name) and value == getattr(self._autoware_mode_state_cls, attr_name):
                return label
        return str(value)

    def _driver_data_fresh(self, now_sec: float) -> bool:
        return self.latest_driver_stamp_sec is not None and (now_sec - self.latest_driver_stamp_sec) <= self.driver_timeout_sec

    def _carla_clock_fresh(self, now_sec: float) -> bool:
        return self.latest_status_stamp_sec is not None and (now_sec - self.latest_status_stamp_sec) <= self.carla_timeout_sec

    def _carla_scene_fresh(self, now_sec: float) -> bool:
        return self.latest_scene_available_stamp_sec is not None and (now_sec - self.latest_scene_available_stamp_sec) <= self.carla_timeout_sec

    def _hazard_data_fresh(self, now_sec: float) -> bool:
        return self.latest_hazard_stamp_sec is not None and (now_sec - self.latest_hazard_stamp_sec) <= self.scenario_timeout_sec

    def _carla_data_fresh(self, now_sec: float) -> bool:
        return self._carla_clock_fresh(now_sec) or self._carla_scene_fresh(now_sec)

    def _scenario_flag_fresh(self, stamp_sec: Optional[float], now_sec: float) -> bool:
        return stamp_sec is not None and (now_sec - stamp_sec) <= self.scenario_timeout_sec

    def _carla_running(self, now_sec: float) -> bool:
        if self._carla_scene_fresh(now_sec):
            return self.scene_available
        return self._carla_clock_fresh(now_sec)

    def _vehicle_speed_mps(self) -> float:
        return float(self.vehicle_speed_mps_value)

    def _vehicle_standstill(self) -> bool:
        return self._vehicle_speed_mps() <= self.standstill_speed_threshold_mps

    def _driver_ready(self, now_sec: float) -> bool:
        if not self.require_driver_monitoring:
            return True
        return (
            self._driver_data_fresh(now_sec)
            and self.driver.available
            and not self.driver.drowsy
            and not self.driver.distracted
        )

    def _automation_requested(self, now_sec: float) -> bool:
        if not self.require_automation_request:
            return True
        return self.automation_request and self._scenario_flag_fresh(self.latest_automation_request_stamp_sec, now_sec)

    def _system_available(self, now_sec: float) -> bool:
        if self._scenario_flag_fresh(self.latest_system_available_stamp_sec, now_sec):
            return self.system_available
        return self.default_system_available

    def _scenario_td_active(self, now_sec: float) -> bool:
        manual_td = self.scenario_td_request and self._scenario_flag_fresh(self.latest_td_request_stamp_sec, now_sec)
        hazard_td = self.hazard_takeover_required and self._hazard_data_fresh(now_sec)
        return manual_td or hazard_td

    def _force_mrm_active(self, now_sec: float) -> bool:
        return self.force_mrm and self._scenario_flag_fresh(self.latest_force_mrm_stamp_sec, now_sec)

    def _reset_mrm_active(self, now_sec: float) -> bool:
        return self.reset_mrm_request and self._scenario_flag_fresh(self.latest_reset_mrm_stamp_sec, now_sec)

    def _manual_takeover_detected(self, now_sec: float) -> bool:
        if self.latest_control_mode_stamp_sec is None:
            return False
        if (now_sec - self.latest_control_mode_stamp_sec) > self.manual_cmd_timeout_sec:
            return False
        return int(self.control_mode.mode) == int(ControlModeReport.MANUAL)

    def _automation_allowed(self, now_sec: float, driver_ready: bool) -> bool:
        return (
            self._carla_running(now_sec)
            and self._system_available(now_sec)
            and driver_ready
            and self._vehicle_speed_mps() >= self.min_l3_speed_mps
        )

    def _transition_demand_reason(self, now_sec: float) -> str:
        if self.hazard_takeover_required and self._hazard_data_fresh(now_sec):
            return self.hazard_reason_code or "hazard_takeover_required"
        return "scenario_transition_demand"

    def _set_state(self, new_state: L3State, reason: str) -> None:
        if self.state == new_state and self.state_reason == reason:
            return
        old_state = self.state
        self.state = new_state
        self.state_reason = reason
        self.state_enter_time = self._now_sec()
        if new_state == L3State.MANUAL and old_state in (L3State.TOR, L3State.MRM):
            self.takeover_completed_until = self.state_enter_time + self.takeover_event_hold_sec
        self.get_logger().info(f"State -> {new_state.value}: {reason}")

    def _tick(self) -> None:
        now_sec = self._now_sec()
        driver_ready = self._driver_ready(now_sec)
        system_available = self._system_available(now_sec)
        automation_allowed = self._automation_allowed(now_sec, driver_ready)
        automation_requested = self._automation_requested(now_sec)
        scenario_td_active = self._scenario_td_active(now_sec)
        force_mrm_active = self._force_mrm_active(now_sec)

        if driver_ready:
            if self.ready_since is None:
                self.ready_since = now_sec
            self.unready_since = None
        else:
            self.ready_since = None
            if self.unready_since is None:
                self.unready_since = now_sec

        if scenario_td_active:
            if self.td_since is None:
                self.td_since = now_sec
        else:
            self.td_since = None

        td_confirmed = self.td_since is not None and (now_sec - self.td_since) >= self.scenario_td_hold_sec
        manual_takeover = self._manual_takeover_detected(now_sec)
        carla_running = self._carla_running(now_sec)
        reset_mrm_active = self._reset_mrm_active(now_sec)

        if not self._carla_data_fresh(now_sec):
            self._set_state(L3State.FAULT, "sim_clock_timeout")
        elif self.state == L3State.FAULT:
            if reset_mrm_active and carla_running:
                self._clear_scenario_requests()
                self._set_state(L3State.MANUAL, "fault_reset")
            elif carla_running:
                self._set_state(L3State.MANUAL, "sim_clock_recovered")
        elif self.state == L3State.MRM and reset_mrm_active:
            self._clear_scenario_requests()
            target = L3State.MANUAL if manual_takeover else L3State.STANDBY
            self._set_state(target, "mrm_reset")
        elif force_mrm_active:
            self.tor_reason = "forced_mrm"
            self._set_state(L3State.MRM, "scenario_force_mrm")
        elif self.state == L3State.MANUAL:
            if automation_allowed:
                self._set_state(L3State.AVAILABLE if automation_requested else L3State.STANDBY, "automation ready")
            else:
                self._set_state(L3State.STANDBY, self._availability_reason(now_sec, driver_ready, system_available, carla_running))
        elif self.state == L3State.STANDBY:
            if automation_allowed and automation_requested:
                self._set_state(L3State.AVAILABLE, "automation requested and ready")
            elif automation_allowed and not self.require_automation_request:
                self._set_state(L3State.AVAILABLE, "automation ready")
            else:
                self.state_reason = self._availability_reason(now_sec, driver_ready, system_available, carla_running)
        elif self.state == L3State.AVAILABLE:
            if not automation_allowed:
                self._set_state(L3State.STANDBY, self._availability_reason(now_sec, driver_ready, system_available, carla_running))
            elif self.auto_engage and self.ready_since is not None and (now_sec - self.ready_since) >= self.driver_ready_hold_sec:
                self._set_state(L3State.ACTIVE, "auto_engage_after_ready_hold")
            elif self.auto_activate_on_request and automation_requested:
                self._set_state(L3State.ACTIVE, "automation_request")
        elif self.state == L3State.ACTIVE:
            if manual_takeover:
                self._set_state(L3State.MANUAL, "manual_override_in_active")
            elif td_confirmed:
                self.tor_reason = self._transition_demand_reason(now_sec)
                self._set_state(L3State.TOR, self.tor_reason)
            elif not system_available:
                self.tor_reason = "system_unavailable"
                self._set_state(L3State.TOR, "system_unavailable")
            elif self.unready_since is not None and (now_sec - self.unready_since) >= self.driver_unready_grace_sec:
                self.tor_reason = "driver_unready"
                self._set_state(L3State.TOR, "driver_unready")
        elif self.state == L3State.TOR:
            if manual_takeover:
                self.scenario_td_request = False
                self.force_mrm = False
                self._set_state(L3State.MANUAL, "takeover_completed")
            elif (now_sec - self.state_enter_time) >= self.tor_timeout_sec:
                self._set_state(L3State.MRM, f"tor_timeout:{self.tor_reason}")
        elif self.state == L3State.MRM:
            if manual_takeover:
                self._clear_scenario_requests()
                self._set_state(L3State.MANUAL, "manual_override_in_mrm")
            elif self._vehicle_standstill():
                self.state_reason = "mrm_vehicle_standstill"

        takeover_completed = now_sec <= self.takeover_completed_until
        td_request = self.state == L3State.TOR
        mrm_request = self.state == L3State.MRM
        automation_engaged = self.state in (L3State.ACTIVE, L3State.TOR, L3State.MRM)
        autoware_enabled = self._desired_autoware_enabled()

        self._publish_bool(self.automation_allowed_pub, automation_allowed)
        self._publish_bool(self.driver_ready_pub, driver_ready)
        self._publish_bool(self.automation_engaged_pub, automation_engaged)
        self._publish_bool(self.system_available_pub, system_available)
        self._publish_bool(self.td_request_pub, td_request)
        self._publish_bool(self.takeover_completed_pub, takeover_completed)
        self._publish_bool(self.mrm_request_pub, mrm_request)
        self._publish_string(self.state_pub, self.state.value)
        self._publish_string(self.state_reason_pub, self.state_reason)
        self._publish_string(self.tor_reason_pub, self.tor_reason if td_request or mrm_request else "idle")
        self._publish_string(
            self.hmi_status_pub,
            self._build_hmi_text(
                now_sec=now_sec,
                driver_ready=driver_ready,
                automation_allowed=automation_allowed,
                system_available=system_available,
                autoware_enabled=autoware_enabled,
            ),
        )
        self._step_autoware_control(autoware_enabled)
        if mrm_request:
            self._publish_mrm_control()

    def _availability_reason(
        self,
        now_sec: float,
        driver_ready: bool,
        system_available: bool,
        carla_running: bool,
    ) -> str:
        if not carla_running:
            return "sim_not_running"
        if not system_available:
            return "system_unavailable"
        if self.require_driver_monitoring:
            if not self._driver_data_fresh(now_sec):
                return "driver_topic_timeout"
            if not driver_ready:
                if not self.driver.available:
                    return "driver_unavailable"
                if self.driver.drowsy:
                    return "driver_drowsy"
                if self.driver.distracted:
                    return "driver_distracted"
        if self._vehicle_speed_mps() < self.min_l3_speed_mps:
            return "speed_below_threshold"
        if self.require_automation_request and not self._automation_requested(now_sec):
            return "waiting_for_automation_request"
        return "ready"

    def _build_hmi_text(
        self,
        now_sec: float,
        driver_ready: bool,
        automation_allowed: bool,
        system_available: bool,
        autoware_enabled: bool,
    ) -> str:
        reasons = []
        if self.require_driver_monitoring and not self._driver_data_fresh(now_sec):
            reasons.append("driver_topic_timeout")
        if not self._carla_data_fresh(now_sec):
            reasons.append("sim_clock_timeout")
        if self.require_driver_monitoring and not self.driver.available:
            reasons.append("driver_unavailable")
        if self.require_driver_monitoring and self.driver.drowsy:
            reasons.append("driver_drowsy")
        if self.require_driver_monitoring and self.driver.distracted:
            reasons.append("driver_distracted")
        if not system_available:
            reasons.append("system_unavailable")
        if self._scenario_td_active(now_sec):
            reasons.append("scenario_td_request")
        if self._force_mrm_active(now_sec):
            reasons.append("scenario_force_mrm")
        if self._reset_mrm_active(now_sec):
            reasons.append("scenario_reset_mrm")

        summary = ",".join(reasons) if reasons else "nominal"
        hazard_summary = self.hazard_reason_code if self._hazard_data_fresh(now_sec) else "unavailable"
        if self._hazard_data_fresh(now_sec):
            if self.hazard_distance_m is not None:
                hazard_summary += f"@{self.hazard_distance_m:.1f}m"
            if self.hazard_ttc_s is not None:
                hazard_summary += f"/{self.hazard_ttc_s:.1f}s"
        return (
            f"state={self.state.value} "
            f"allowed={str(automation_allowed).lower()} "
            f"engaged={str(self.state in (L3State.ACTIVE, L3State.TOR, L3State.MRM)).lower()} "
            f"autoware={str(autoware_enabled).lower()} "
            f"backend={self.autoware_status_summary} "
            f"driver_ready={str(driver_ready).lower()} "
            f"driver_monitoring={str(self.require_driver_monitoring).lower()} "
            f"system_available={str(system_available).lower()} "
            f"action={self.driver.action_label} "
            f"hazard={hazard_summary} "
            f"reason={summary}"
        )

    @staticmethod
    def _publish_bool(publisher, value: bool) -> None:
        msg = Bool()
        msg.data = value
        publisher.publish(msg)

    @staticmethod
    def _publish_string(publisher, value: str) -> None:
        msg = String()
        msg.data = value
        publisher.publish(msg)

    def _clear_scenario_requests(self) -> None:
        self.scenario_td_request = False
        self.force_mrm = False
        self.reset_mrm_request = False

    def _desired_autoware_enabled(self) -> bool:
        if self.state == L3State.ACTIVE:
            return self.active_uses_autoware
        if self.state == L3State.TOR:
            return self.tor_keeps_autoware
        return False

    def _step_autoware_control(self, enabled: bool) -> None:
        if self._autoware_pending_future is not None:
            if self._autoware_pending_future.done():
                self._handle_autoware_future_done()
            else:
                return

        if self._autoware_change_srv_cls is not None:
            self._step_autoware_adapi(enabled)
        elif self._autoware_engage_pub is not None:
            self._publish_autoware_engage(enabled)

    def _step_autoware_adapi(self, enabled: bool) -> None:
        if enabled:
            if not self.autoware_state_control_enabled:
                self._call_autoware_service(self._autoware_enable_control_client, "enable_autoware_control")
                return
            if self.autoware_state_mode != "AUTONOMOUS":
                self._call_autoware_service(self._autoware_change_to_autonomous_client, "change_to_autonomous")
                return
            if not self.autoware_enabled:
                self.autoware_enabled = True
                self.autoware_status_summary = "adapi:enabled"
                self.get_logger().info("Autoware -> true")
            return

        if self.autoware_state_mode == "AUTONOMOUS":
            self._call_autoware_service(self._autoware_change_to_stop_client, "change_to_stop")
            return
        if self.autoware_state_control_enabled:
            self._call_autoware_service(self._autoware_disable_control_client, "disable_autoware_control")
            return
        if self.autoware_enabled:
            self.autoware_enabled = False
            self.autoware_status_summary = "adapi:disabled"
            self.get_logger().info("Autoware -> false")

    def _call_autoware_service(self, client: Any, label: str) -> None:
        if client is None:
            return
        if not client.service_is_ready():
            self.autoware_status_summary = f"{label}:waiting_service"
            return
        request = self._autoware_change_srv_cls.Request()
        self._autoware_pending_label = label
        self._autoware_pending_future = client.call_async(request)
        self.autoware_status_summary = f"{label}:pending"

    def _handle_autoware_future_done(self) -> None:
        future = self._autoware_pending_future
        label = self._autoware_pending_label or "unknown"
        self._autoware_pending_future = None
        self._autoware_pending_label = None
        try:
            response = future.result()
        except Exception as exc:  # pragma: no cover - defensive logging
            self.autoware_status_summary = f"{label}:error"
            self.get_logger().warning(f"Autoware service {label} failed: {exc}")
            return

        success = bool(getattr(getattr(response, "status", None), "success", True))
        self.autoware_status_summary = f"{label}:{'ok' if success else 'failed'}"
        if not success:
            message = getattr(getattr(response, "status", None), "message", "")
            self.get_logger().warning(f"Autoware service {label} returned failure: {message}")

    def _publish_autoware_engage(self, enabled: bool) -> None:
        if self.autoware_enabled == enabled:
            return
        self.autoware_enabled = enabled
        msg = self._autoware_msg_cls()
        if hasattr(msg, "engage"):
            msg.engage = enabled
        elif hasattr(msg, "data"):
            msg.data = enabled
        else:
            self.get_logger().warning("Autoware engage message does not expose a supported boolean field.")
            return
        self._autoware_engage_pub.publish(msg)
        self.autoware_status_summary = f"engage_topic:{'enabled' if enabled else 'disabled'}"
        self.get_logger().info(f"Autoware -> {str(enabled).lower()}")

    def _publish_mrm_control(self) -> None:
        msg = ActuationCommandStamped()
        msg.header.stamp = self.get_clock().now().to_msg()
        msg.header.frame_id = "base_link"
        msg.actuation.accel_cmd = float(self.mrm_throttle)
        msg.actuation.brake_cmd = float(self.mrm_brake)
        msg.actuation.steer_cmd = float(self.mrm_steer)
        self.mrm_control_pub.publish(msg)


def main(args=None) -> None:
    rclpy.init(args=args)
    node = L3SupervisorNode()
    try:
        rclpy.spin(node)
    except (KeyboardInterrupt, ExternalShutdownException):
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()
