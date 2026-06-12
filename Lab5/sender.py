import cv2
import json
import socket
import base64
import time
from datetime import datetime



VIDEO_PATH = "data/test_video.mp4"

HOST = "localhost"
PORT = 6100

FPS_LIMIT = 5

# ==========================


def frame_to_base64(frame):
    """
    OpenCV frame
        ↓
    JPEG
        ↓
    Base64 string
    """

    success, buffer = cv2.imencode(
        ".jpg",
        frame,
        [cv2.IMWRITE_JPEG_QUALITY, 80]
    )

    if not success:
        return None

    return base64.b64encode(
        buffer.tobytes()
    ).decode("utf-8")


def create_message(frame, frame_id):

    image_base64 = frame_to_base64(frame)

    if image_base64 is None:
        return None

    return {
        "timestamp": datetime.now().strftime(
            "%Y%m%d_%H%M%S_%f"
        ),
        "frame_id": frame_id,
        "image": image_base64
    }


def main():

    print("=" * 50)
    print("PERSON COUNTING - SENDER")
    print("=" * 50)

    cap = cv2.VideoCapture(VIDEO_PATH)

    if not cap.isOpened():
        print(f"Cannot open video: {VIDEO_PATH}")
        return

    server_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    server_socket.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1
    )

    server_socket.bind(
        (HOST, PORT)
    )

    server_socket.listen(1)

    print(f"Waiting Spark connection on {HOST}:{PORT}")

    conn, addr = server_socket.accept()

    print(f"Connected from {addr}")

    frame_id = 0

    try:

        while True:

            ret, frame = cap.read()

            # hết video
            if not ret:

                print(
                    "Video finished. Restarting..."
                )

                cap.set(
                    cv2.CAP_PROP_POS_FRAMES,
                    0
                )

                continue

            frame_id += 1

            message = create_message(
                frame,
                frame_id
            )

            if message is None:
                continue

            conn.sendall(
                (
                    json.dumps(message)
                    + "\n"
                ).encode("utf-8")
            )

            print(
                f"Sent frame_id={frame_id}"
            )

            time.sleep(
                1 / FPS_LIMIT
            )

    except KeyboardInterrupt:

        print("\nSender stopped")

    finally:

        cap.release()

        conn.close()

        server_socket.close()

        print("Socket closed")


if __name__ == "__main__":
    main()