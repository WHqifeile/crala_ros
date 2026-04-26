from __future__ import annotations

import json
import time
from typing import Dict, Optional, Tuple

from .runtime_deps import inject_site_packages

try:
    import cv2
    import numpy as np
    from openvino.runtime import Core
except ModuleNotFoundError:
    inject_site_packages()
    import cv2
    import numpy as np
    from openvino.runtime import Core

import rclpy
from geometry_msgs.msg import Vector3Stamped
from rclpy.node import Node
from sensor_msgs.msg import Image
from std_msgs.msg import Bool, String


class ExternalDmsPoseNode(Node):
    def __init__(self) -> None:
        super().__init__("external_dms_pose")

        self._declare_parameters()

        self.device_path = str(self.get_parameter("device_path").value)
        self.frame_id = str(self.get_parameter("frame_id").value)
        self.width = int(self.get_parameter("width").value)
        self.height = int(self.get_parameter("height").value)
        self.fps = float(self.get_parameter("fps").value)
        self.face_confidence_threshold = float(self.get_parameter("face_confidence_threshold").value)
        self.closed_eye_class_index = int(self.get_parameter("closed_eye_class_index").value)
        self.closed_eye_threshold = float(self.get_parameter("closed_eye_threshold").value)
        self.drowsy_eye_threshold = float(self.get_parameter("drowsy_eye_threshold").value)
        self.yaw_distraction_threshold = float(self.get_parameter("yaw_distraction_threshold").value)
        self.pitch_distraction_threshold = float(self.get_parameter("pitch_distraction_threshold").value)
        self.publish_debug_image = bool(self.get_parameter("publish_debug_image").value)
        self.min_process_interval_sec = float(self.get_parameter("min_process_interval_sec").value)

        # 联调模式：只判断“睁眼”
        self.open_eyes_only_mode = bool(self.get_parameter("open_eyes_only_mode").value)
        self.open_eye_max_closed_prob = float(self.get_parameter("open_eye_max_closed_prob").value)
        self.eye_state_confirm_frames = int(self.get_parameter("eye_state_confirm_frames").value)
        self.eyes_closed_unavailable_sec = float(self.get_parameter("eyes_closed_unavailable_sec").value)
        self.no_face_hold_sec = float(self.get_parameter("no_face_hold_sec").value)

        self.last_process_time = 0.0
        self.last_log_time = 0.0

        # 平滑状态
        self.eye_open_streak = 0
        self.eye_closed_streak = 0
        self.last_stable_eyes_open = True
        self.eyes_closed_since_monotonic: Optional[float] = None
        self.last_face_seen_monotonic = time.monotonic()
        self.last_stable_available = True
        self.last_action_label = "normal"

        self.available_publisher = self.create_publisher(Bool, str(self.get_parameter("available_topic").value), 10)
        self.drowsy_publisher = self.create_publisher(Bool, str(self.get_parameter("drowsy_topic").value), 10)
        self.distracted_publisher = self.create_publisher(
            Bool, str(self.get_parameter("distracted_topic").value), 10
        )
        self.action_label_publisher = self.create_publisher(
            String, str(self.get_parameter("action_label_topic").value), 10
        )
        self.head_pose_publisher = self.create_publisher(
            Vector3Stamped, str(self.get_parameter("head_pose_topic").value), 10
        )
        self.gaze_publisher = self.create_publisher(
            Vector3Stamped, str(self.get_parameter("gaze_topic").value), 10
        )
        self.debug_image_publisher = None
        if self.publish_debug_image:
            self.debug_image_publisher = self.create_publisher(
                Image, str(self.get_parameter("debug_image_topic").value), 10
            )

        core = Core()
        self.face_model = core.compile_model(core.read_model(str(self.get_parameter("face_model").value)), "CPU")
        self.landmarks_model = core.compile_model(
            core.read_model(str(self.get_parameter("landmarks_model").value)), "CPU"
        )
        self.head_pose_model = core.compile_model(
            core.read_model(str(self.get_parameter("head_pose_model").value)), "CPU"
        )
        self.eye_model = core.compile_model(core.read_model(str(self.get_parameter("eye_model").value)), "CPU")
        self.gaze_model = core.compile_model(core.read_model(str(self.get_parameter("gaze_model").value)), "CPU")

        self.capture = cv2.VideoCapture(self.device_path, cv2.CAP_V4L2)
        self.capture.set(cv2.CAP_PROP_FRAME_WIDTH, self.width)
        self.capture.set(cv2.CAP_PROP_FRAME_HEIGHT, self.height)
        self.capture.set(cv2.CAP_PROP_FPS, self.fps)
        self.capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)

        if not self.capture.isOpened():
            raise RuntimeError(f"Failed to open camera device {self.device_path}")

        self.timer = self.create_timer(1.0 / max(1.0, self.fps), self._tick)
        self.get_logger().info(
            f"External DMS camera node started on {self.device_path}, publishing /driver/* topics. "
            f"open_eyes_only_mode={self.open_eyes_only_mode}"
        )

    def _declare_parameters(self) -> None:
        self.declare_parameter("device_path", "/dev/video1")
        self.declare_parameter("available_topic", "/driver/available")
        self.declare_parameter("drowsy_topic", "/driver/drowsy")
        self.declare_parameter("distracted_topic", "/driver/distracted")
        self.declare_parameter("head_pose_topic", "/driver/head_pose")
        self.declare_parameter("gaze_topic", "/driver/gaze_vector")
        self.declare_parameter("action_label_topic", "/driver/action_label")
        self.declare_parameter("debug_image_topic", "/dms/debug_image")
        self.declare_parameter("frame_id", "dms_camera")
        self.declare_parameter("fps", 8.0)
        self.declare_parameter("width", 640)
        self.declare_parameter("height", 480)
        self.declare_parameter("publish_debug_image", True)
        self.declare_parameter("min_process_interval_sec", 0.10)
        self.declare_parameter("face_confidence_threshold", 0.60)
        self.declare_parameter("closed_eye_class_index", 1)
        self.declare_parameter("closed_eye_threshold", 0.65)
        self.declare_parameter("drowsy_eye_threshold", 0.75)
        self.declare_parameter("yaw_distraction_threshold", 55.0)
        self.declare_parameter("pitch_distraction_threshold", 35.0)

        # 联调模式参数：只要睁眼就认为 normal
        self.declare_parameter("open_eyes_only_mode", True)
        self.declare_parameter("open_eye_max_closed_prob", 0.55)
        self.declare_parameter("eye_state_confirm_frames", 3)
        self.declare_parameter("eyes_closed_unavailable_sec", 3.0)
        self.declare_parameter("no_face_hold_sec", 2.0)

        self.declare_parameter(
            "face_model",
            "/home/vci/models/openvino/intel/face-detection-adas-0001/FP16/face-detection-adas-0001.xml",
        )
        self.declare_parameter(
            "landmarks_model",
            "/home/vci/models/openvino/intel/facial-landmarks-35-adas-0002/FP16/facial-landmarks-35-adas-0002.xml",
        )
        self.declare_parameter(
            "head_pose_model",
            "/home/vci/models/openvino/intel/head-pose-estimation-adas-0001/FP16/head-pose-estimation-adas-0001.xml",
        )
        self.declare_parameter(
            "eye_model",
            "/home/vci/models/openvino/public/open-closed-eye-0001/FP16/open-closed-eye-0001.xml",
        )
        self.declare_parameter(
            "gaze_model",
            "/home/vci/models/openvino/intel/gaze-estimation-adas-0002/FP16/gaze-estimation-adas-0002.xml",
        )

    def _tick(self) -> None:
        ok, frame = self.capture.read()
        if not ok or frame is None:
            self._publish_driver_topics(
                available=False,
                drowsy=False,
                distracted=False,
                head_pose={"yaw": 0.0, "pitch": 0.0, "roll": 0.0},
                gaze_vector=None,
                action_label="unavailable",
            )
            return

        now = time.monotonic()
        if now - self.last_process_time < self.min_process_interval_sec:
            return
        self.last_process_time = now

        result = self._analyze_driver(frame)
        self._publish_driver_topics(
            available=bool(result["available"]),
            drowsy=bool(result["drowsy"]),
            distracted=bool(result["distracted"]),
            head_pose=result["head_pose"],
            gaze_vector=result["gaze_vector"],
            action_label=result["action_label"],
        )

        if self.debug_image_publisher is not None:
            debug_frame = self._build_debug_frame(frame.copy(), result)
            self.debug_image_publisher.publish(self._to_rgb_image_msg(debug_frame))

    def _analyze_driver(self, frame: np.ndarray) -> Dict[str, object]:
        detections = self.face_model({"data": self._prepare(frame, (672, 384))})[
            self.face_model.output("detection_out")
        ][0][0]
        frame_h, frame_w = frame.shape[:2]

        best_face = None
        best_area = -1.0
        for detection in detections:
            confidence = float(detection[2])
            if confidence < self.face_confidence_threshold:
                continue
            x1 = max(0, int(detection[3] * frame_w))
            y1 = max(0, int(detection[4] * frame_h))
            x2 = min(frame_w, int(detection[5] * frame_w))
            y2 = min(frame_h, int(detection[6] * frame_h))
            area = max(0, x2 - x1) * max(0, y2 - y1)
            if area > best_area:
                best_area = area
                best_face = (x1, y1, x2, y2, confidence)

        if best_face is None:
            no_face_duration = time.monotonic() - self.last_face_seen_monotonic
            if no_face_duration <= self.no_face_hold_sec:
                return {
                    "available": bool(self.last_stable_available),
                    "drowsy": False,
                    "distracted": False,
                    "head_pose": {"yaw": 0.0, "pitch": 0.0, "roll": 0.0},
                    "gaze_vector": None,
                    "action_label": self.last_action_label,
                    "face_bbox": None,
                    "landmarks": [],
                    "eye_boxes": {},
                }
            self.last_stable_available = False
            self.last_action_label = "no_face"
            return {
                "available": False,
                "drowsy": False,
                "distracted": False,
                "head_pose": {"yaw": 0.0, "pitch": 0.0, "roll": 0.0},
                "gaze_vector": None,
                "action_label": "no_face",
                "face_bbox": None,
                "landmarks": [],
                "eye_boxes": {},
            }

        x1, y1, x2, y2, confidence = best_face
        now = time.monotonic()
        self.last_face_seen_monotonic = now

        face_crop = frame[y1:y2, x1:x2]
        if face_crop.size == 0:
            return {
                "available": False,
                "drowsy": False,
                "distracted": False,
                "head_pose": {"yaw": 0.0, "pitch": 0.0, "roll": 0.0},
                "gaze_vector": None,
                "action_label": "invalid_face",
                "face_bbox": None,
                "landmarks": [],
                "eye_boxes": {},
            }

        landmarks = self._estimate_landmarks(face_crop, (x1, y1, x2, y2))
        head_pose = self._estimate_head_pose(face_crop)
        eye_boxes = self._estimate_eye_boxes((x1, y1, x2, y2), landmarks, frame_w, frame_h)
        eye_state = self._estimate_eye_state(frame, eye_boxes)
        gaze_vector = self._estimate_gaze(frame, eye_boxes, head_pose)

        fatigue_level = max(float(eye_state["left"]), float(eye_state["right"]))

        if self.open_eyes_only_mode:
            raw_eyes_open = (
                float(eye_state["left"]) < self.open_eye_max_closed_prob
                and float(eye_state["right"]) < self.open_eye_max_closed_prob
            )

            if raw_eyes_open:
                self.eye_open_streak += 1
                self.eye_closed_streak = 0
            else:
                self.eye_closed_streak += 1
                self.eye_open_streak = 0

            if self.eye_open_streak >= self.eye_state_confirm_frames:
                stable_eyes_open = True
            elif self.eye_closed_streak >= self.eye_state_confirm_frames:
                stable_eyes_open = False
            else:
                stable_eyes_open = self.last_stable_eyes_open

            self.last_stable_eyes_open = stable_eyes_open

            if stable_eyes_open:
                self.eyes_closed_since_monotonic = None
            elif self.eyes_closed_since_monotonic is None:
                self.eyes_closed_since_monotonic = now

            eyes_closed_too_long = (
                self.eyes_closed_since_monotonic is not None
                and (now - self.eyes_closed_since_monotonic) >= self.eyes_closed_unavailable_sec
            )

            available = not eyes_closed_too_long
            drowsy = False
            distracted = False
            action_label = "normal" if available else "eyes_closed"
        else:
            drowsy = fatigue_level >= self.drowsy_eye_threshold and bool(eye_state["both_closed"])
            distracted = (
                abs(float(head_pose["yaw"])) >= self.yaw_distraction_threshold
                or abs(float(head_pose["pitch"])) >= self.pitch_distraction_threshold
            )

            available = True
            action_label = "normal"
            if drowsy:
                action_label = "drowsy"
            elif distracted:
                action_label = "distracted"

        self.last_stable_available = bool(available)
        self.last_action_label = action_label

        return {
            "available": bool(available),
            "drowsy": bool(drowsy),
            "distracted": bool(distracted),
            "head_pose": {key: round(float(value), 3) for key, value in head_pose.items()},
            "gaze_vector": gaze_vector,
            "action_label": action_label,
            "face_bbox": {"x1": x1, "y1": y1, "x2": x2, "y2": y2, "confidence": round(confidence, 4)},
            "landmarks": [[round(float(point[0]), 1), round(float(point[1]), 1)] for point in landmarks],
            "eye_boxes": {
                "left": {
                    "x1": eye_boxes["left"][0],
                    "y1": eye_boxes["left"][1],
                    "x2": eye_boxes["left"][2],
                    "y2": eye_boxes["left"][3],
                },
                "right": {
                    "x1": eye_boxes["right"][0],
                    "y1": eye_boxes["right"][1],
                    "x2": eye_boxes["right"][2],
                    "y2": eye_boxes["right"][3],
                },
            },
        }

    def _estimate_landmarks(self, face_crop: np.ndarray, face_bbox: Tuple[int, int, int, int]) -> np.ndarray:
        outputs = self.landmarks_model({"data": self._prepare(face_crop, (60, 60))})
        normalized = outputs[self.landmarks_model.output("align_fc3")].reshape(-1, 2)
        x1, y1, x2, y2 = face_bbox
        width = max(1.0, float(x2 - x1))
        height = max(1.0, float(y2 - y1))
        landmarks = normalized.copy()
        landmarks[:, 0] = x1 + landmarks[:, 0] * width
        landmarks[:, 1] = y1 + landmarks[:, 1] * height
        return landmarks

    def _estimate_head_pose(self, face_crop: np.ndarray) -> Dict[str, float]:
        outputs = self.head_pose_model({"data": self._prepare(face_crop, (60, 60))})
        return {
            "yaw": float(outputs[self.head_pose_model.output("angle_y_fc")].reshape(-1)[0]),
            "pitch": float(outputs[self.head_pose_model.output("angle_p_fc")].reshape(-1)[0]),
            "roll": float(outputs[self.head_pose_model.output("angle_r_fc")].reshape(-1)[0]),
        }

    def _estimate_eye_boxes(
        self, face_bbox: Tuple[int, int, int, int], landmarks: np.ndarray, frame_w: int, frame_h: int
    ) -> Dict[str, Tuple[int, int, int, int]]:
        x1, y1, x2, y2 = face_bbox
        face_h = max(1.0, float(y2 - y1))
        upper_face = landmarks[landmarks[:, 1] < (y1 + face_h * 0.62)]
        if upper_face.shape[0] < 4:
            upper_face = landmarks

        median_x = np.median(upper_face[:, 0])
        left_points = upper_face[upper_face[:, 0] <= median_x]
        right_points = upper_face[upper_face[:, 0] > median_x]
        left_box = self._eye_box_from_points(left_points, face_bbox, 0.32, 0.38, frame_w, frame_h)
        right_box = self._eye_box_from_points(right_points, face_bbox, 0.68, 0.38, frame_w, frame_h)
        return {"left": left_box, "right": right_box}

    def _eye_box_from_points(
        self,
        points: np.ndarray,
        face_bbox: Tuple[int, int, int, int],
        fallback_x: float,
        fallback_y: float,
        frame_w: int,
        frame_h: int,
    ) -> Tuple[int, int, int, int]:
        x1, y1, x2, y2 = face_bbox
        face_w = max(1.0, float(x2 - x1))
        face_h = max(1.0, float(y2 - y1))

        if points.shape[0] >= 2:
            center_x = float(np.median(points[:, 0]))
            center_y = float(np.median(points[:, 1]))
            span_x = max(8.0, float(np.max(points[:, 0]) - np.min(points[:, 0])))
            span_y = max(6.0, float(np.max(points[:, 1]) - np.min(points[:, 1])))
            size = max(span_x * 2.0, span_y * 3.0, face_w * 0.16)
        else:
            center_x = x1 + face_w * fallback_x
            center_y = y1 + face_h * fallback_y
            size = face_w * 0.18

        ex1 = max(0, int(center_x - size / 2.0))
        ey1 = max(0, int(center_y - size / 2.0))
        ex2 = min(frame_w, int(center_x + size / 2.0))
        ey2 = min(frame_h, int(center_y + size / 2.0))
        return ex1, ey1, ex2, ey2

    def _estimate_eye_state(
        self, frame: np.ndarray, eye_boxes: Dict[str, Tuple[int, int, int, int]]
    ) -> Dict[str, object]:
        result = {"left": 0.0, "right": 0.0, "both_closed": False}
        for key in ("left", "right"):
            x1, y1, x2, y2 = eye_boxes[key]
            eye_crop = frame[y1:y2, x1:x2]
            if eye_crop.size == 0:
                continue
            outputs = self.eye_model({"input.1": self._prepare(eye_crop, (32, 32))})
            logits = outputs[self.eye_model.output("19")].reshape(-1)
            probabilities = self._softmax(logits)
            result[key] = float(probabilities[self.closed_eye_class_index])
        result["both_closed"] = bool(
            result["left"] >= self.closed_eye_threshold and result["right"] >= self.closed_eye_threshold
        )
        return result

    def _estimate_gaze(
        self, frame: np.ndarray, eye_boxes: Dict[str, Tuple[int, int, int, int]], head_pose: Dict[str, float]
    ) -> Optional[Dict[str, float]]:
        left_box = eye_boxes["left"]
        right_box = eye_boxes["right"]
        left_eye = frame[left_box[1] : left_box[3], left_box[0] : left_box[2]]
        right_eye = frame[right_box[1] : right_box[3], right_box[0] : right_box[2]]
        if left_eye.size == 0 or right_eye.size == 0:
            return None

        outputs = self.gaze_model(
            {
                "left_eye_image": self._prepare(left_eye, (60, 60)),
                "right_eye_image": self._prepare(right_eye, (60, 60)),
                "head_pose_angles": np.array(
                    [[head_pose["yaw"], head_pose["pitch"], head_pose["roll"]]], dtype=np.float32
                ),
            }
        )
        gaze = outputs[self.gaze_model.output("gaze_vector")].reshape(-1)
        return {"x": round(float(gaze[0]), 4), "y": round(float(gaze[1]), 4), "z": round(float(gaze[2]), 4)}

    def _prepare(self, image: np.ndarray, size_wh: Tuple[int, int]) -> np.ndarray:
        width, height = size_wh
        resized = cv2.resize(image, (width, height), interpolation=cv2.INTER_LINEAR)
        return resized.transpose(2, 0, 1)[None].astype(np.float32)

    def _softmax(self, values: np.ndarray) -> np.ndarray:
        array = values.astype(np.float32)
        array -= np.max(array)
        exp = np.exp(array)
        return exp / np.sum(exp)

    def _to_rgb_image_msg(self, frame_bgr: np.ndarray) -> Image:
        frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
        msg = Image()
        msg.header.stamp = self.get_clock().now().to_msg()
        msg.header.frame_id = self.frame_id
        msg.height = int(frame_rgb.shape[0])
        msg.width = int(frame_rgb.shape[1])
        msg.encoding = "rgb8"
        msg.step = int(frame_rgb.shape[1] * 3)
        msg.data = frame_rgb.tobytes()
        return msg

    def _build_debug_frame(self, frame: np.ndarray, result: Dict[str, object]) -> np.ndarray:
        face = result.get("face_bbox")
        if face:
            cv2.rectangle(
                frame,
                (int(face["x1"]), int(face["y1"])),
                (int(face["x2"]), int(face["y2"])),
                (0, 255, 255),
                2,
            )

        for point in result.get("landmarks", []):
            cv2.circle(frame, (int(point[0]), int(point[1])), 1, (255, 150, 0), -1)

        for key, color in (("left", (255, 180, 0)), ("right", (0, 180, 255))):
            box = result.get("eye_boxes", {}).get(key)
            if box:
                cv2.rectangle(
                    frame,
                    (int(box["x1"]), int(box["y1"])),
                    (int(box["x2"]), int(box["y2"])),
                    color,
                    1,
                )

        pose = result.get("head_pose", {"yaw": 0.0, "pitch": 0.0, "roll": 0.0})
        lines = [
            f"available={result.get('available', False)} action={result.get('action_label', 'none')}",
            f"yaw={pose.get('yaw', 0.0):.1f} pitch={pose.get('pitch', 0.0):.1f} roll={pose.get('roll', 0.0):.1f}",
            f"drowsy={result.get('drowsy', False)} distracted={result.get('distracted', False)}",
            f"open_eyes_only={self.open_eyes_only_mode}",
        ]

        y = 24
        for line in lines:
            cv2.putText(frame, line, (10, y), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (255, 255, 255), 2, cv2.LINE_AA)
            cv2.putText(frame, line, (10, y), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (20, 20, 20), 1, cv2.LINE_AA)
            y += 24
        return frame

    def _publish_driver_topics(
        self,
        *,
        available: bool,
        drowsy: bool,
        distracted: bool,
        head_pose: Dict[str, float],
        gaze_vector: Optional[Dict[str, float]],
        action_label: str,
    ) -> None:
        msg = Bool()
        msg.data = bool(available)
        self.available_publisher.publish(msg)

        msg = Bool()
        msg.data = bool(drowsy)
        self.drowsy_publisher.publish(msg)

        msg = Bool()
        msg.data = bool(distracted)
        self.distracted_publisher.publish(msg)

        msg = String()
        msg.data = action_label
        self.action_label_publisher.publish(msg)

        pose_msg = Vector3Stamped()
        pose_msg.header.stamp = self.get_clock().now().to_msg()
        pose_msg.header.frame_id = self.frame_id
        pose_msg.vector.x = float(head_pose["yaw"])
        pose_msg.vector.y = float(head_pose["pitch"])
        pose_msg.vector.z = float(head_pose["roll"])
        self.head_pose_publisher.publish(pose_msg)

        gaze_msg = Vector3Stamped()
        gaze_msg.header = pose_msg.header
        if gaze_vector is not None:
            gaze_msg.vector.x = float(gaze_vector["x"])
            gaze_msg.vector.y = float(gaze_vector["y"])
            gaze_msg.vector.z = float(gaze_vector["z"])
        self.gaze_publisher.publish(gaze_msg)

        now = time.monotonic()
        if now - self.last_log_time > 2.0:
            self.last_log_time = now
            self.get_logger().info(
                json.dumps(
                    {
                        "available": available,
                        "drowsy": drowsy,
                        "distracted": distracted,
                        "head_pose": head_pose,
                        "gaze_vector": gaze_vector,
                        "action_label": action_label,
                        "open_eyes_only_mode": self.open_eyes_only_mode,
                        "eye_open_streak": self.eye_open_streak,
                        "eye_closed_streak": self.eye_closed_streak,
                    },
                    ensure_ascii=False,
                )
            )

    def destroy_node(self) -> bool:
        if hasattr(self, "capture") and self.capture is not None:
            self.capture.release()
        return super().destroy_node()


def main(args=None) -> None:
    rclpy.init(args=args)
    node = ExternalDmsPoseNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        if rclpy.ok():
            node.destroy_node()
            rclpy.shutdown()


if __name__ == "__main__":
    main()
