import rclpy
from rclpy.node import Node
import importlib
from rclpy.serialization import serialize_message
from RTMPHandler import RTMPHandler

class ROS2RTMPBridge(Node):
    def __init__(self):
        super().__init__('ros2rtmp_bridge')

        # 환경 변수로부터 설정 읽기
        self.platform = os.getenv('PLATFORM', '')
        self.ros2rtmp_ros_topic = os.getenv('ROS2RTMP_ROS_TOPIC', '')
        self.ros2rtmp_ros_type = os.getenv('ROS2RTMP_ROS_TYPE', '')
        self.rtmp_url = "rtmp://203.250.148.119/edge/"+self.ros2rtmp_ros_topic
        self.mode = os.getenv('MODE', '')

        # RTMP 핸들러 인스턴스 생성
        self.rtmp_handler = RTMPHandler(self.rtmp_url)

        # ROS 메시지 타입 동적 로드
        self.ros_msg_type = self.get_ros_msg_type(self.ros2rtmp_ros_type)

        if self.ros_msg_type is None:
            self.get_logger().error(f"ROS 메시지 타입을 찾을 수 없습니다: {self.ros2rtmp_ros_type}")
            return

        # 플랫폼 및 모드 설정에 따른 분기 처리
        if self.platform == 'edge':
            if self.mode == 'pub':
                self.get_logger().info("모드: pub - RTMP로 송신, ROS 구독")
                self.start_rtmp_to_ros();
 
            elif self.mode == 'sub':
                self.get_logger().info("모드: sub - ROS로 퍼블리시, RTMP 수신")
                self.ros_subscription = self.create_subscription(
                    self.ros_msg_type,
                    self.ros2rtmp_ros_topic,
                    self.ros_to_rtmp_callback,
                    10
                )
            else:
                self.get_logger().error("올바르지 않은 모드 설정. 'pub' 또는 'sub'만 허용됩니다.")
                return
            
        elif self.platform == 'user':
            if self.mode == 'pub':
                self.get_logger().info("모드: pub - RTMP로 송신, ROS 구독")
                self.ros_subscription = self.create_subscription(
                    self.ros_msg_type,
                    self.ros2rtmp_ros_topic,
                    self.ros_to_rtmp_callback,
                    10
                )
            elif self.mode == 'sub':
                self.get_logger().info("모드: sub - ROS로 퍼블리시, RTMP 수신")
                self.start_rtmp_to_ros()
            else:
                self.get_logger().error("올바르지 않은 모드 설정. 'pub' 또는 'sub'만 허용됩니다.")
                return
        else:
            self.get_logger().error("올바르지 않은 플랫폼 설정. 'edge' 또는 'user'만 허용됩니다.")
            return

    def get_ros_msg_type(self, ros_type_name):
        """ROS 메시지 타입을 동적으로 로드하는 함수."""
        try:
            package_name, msg_name = ros_type_name.split('/')
            module = importlib.import_module(f"{package_name}.msg")
            return getattr(module, msg_name)
        except (ImportError, AttributeError) as e:
            self.get_logger().error(f"Failed to import ROS message type: {ros_type_name}. Error: {e}")
            return None

    def ros_to_rtmp_callback(self, msg):
        """ROS에서 수신된 데이터를 RTMP로 전송하는 콜백 함수."""
        self.get_logger().info(f"ROS에서 데이터 수신: {msg}")
        
        # ROS 메시지를 직렬화
        serialized_msg = serialize_message(msg)
        
        # RTMP로 데이터 전송
        self.rtmp_handler.send_to_rtmp(serialized_msg)

    def start_rtmp_to_ros(self):
        """RTMP에서 데이터를 수신하여 ROS로 퍼블리시하는 함수"""
        process = self.rtmp_handler.receive_from_rtmp()
        if process is None:
            self.get_logger().error("RTMP 수신에 실패했습니다.")
            return

        # RTMP로부터 수신된 데이터를 ROS로 퍼블리시
        self.ros_publisher = self.create_publisher(
            self.ros_msg_type,
            self.ros2rtmp_ros_topic,
            10
        )

        while process.poll() is None:
            data = process.stdout.read()
            if data:
                # 수신된 데이터를 ROS 메시지로 변환하고 퍼블리시합니다.
                self.ros_publisher.publish(data)

def main(args=None):
    rclpy.init(args=args)
    ros2rtmp_bridge = ROS2RTMPBridge()

    try:
        rclpy.spin(ros2rtmp_bridge)
    except KeyboardInterrupt:
        pass
    finally:
        ros2rtmp_bridge.destroy_node()
        rclpy.shutdown()

if __name__ == '__main__':
    main()