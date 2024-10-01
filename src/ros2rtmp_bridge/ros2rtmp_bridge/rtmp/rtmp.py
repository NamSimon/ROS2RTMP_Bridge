import subprocess
import time
import logging

class RTMPHandler:
    def __init__(self, rtmp_url):
        self.rtmp_url = rtmp_url
        self.logger = logging.getLogger('RTMPHandler')
        logging.basicConfig(level=logging.INFO)

    def send_to_rtmp(self, data):
        """RTMP로 데이터를 전송하는 함수"""
        self.logger.info(f"RTMP로 데이터 전송 중: {self.rtmp_url}")
        
        # FFmpeg 명령어 설정
        ffmpeg_cmd = [
            "ffmpeg",
            "-re",  # 실시간 모드
            "-i", "-",  # 표준 입력을 사용하여 FFmpeg에 데이터 전달
            "-f", "flv",  # 플래시 비디오 (FLV) 포맷으로 RTMP로 전송
            self.rtmp_url
        ]
        
        process = subprocess.Popen(ffmpeg_cmd, stdin=subprocess.PIPE)
        process.stdin.write(data)
        process.stdin.close()
        process.wait()
        
    def receive_from_rtmp(self):
        """RTMP에서 데이터를 수신하는 함수, 최대 10초간 연결 시도"""
        self.logger.info("RTMP에서 데이터를 수신하려고 시도합니다.")
        
        start_time = time.time()
        success = False

        while time.time() - start_time < 10:
            ffmpeg_cmd = [
                "ffmpeg",
                "-i", self.rtmp_url,
                "-f", "rawvideo",
                "-pix_fmt", "yuv420p",
                "-"
            ]
            
            process = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE)
            if process.poll() is None:  # 프로세스가 정상적으로 시작되었는지 확인
                self.logger.info("RTMP 연결에 성공했습니다.")
                success = True
                break
            else:
                self.logger.warning("RTMP 연결 실패. 재시도 중...")
                time.sleep(1)  # 1초 후에 다시 시도

        if not success:
            self.logger.error("10초간 연결 시도 후 RTMP 연결에 실패했습니다.")
            return None

        return process