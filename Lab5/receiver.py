import os
import cv2
import json
import base64
import numpy as np
from detector import PersonDetector
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext


# ====================================
# CONFIG
# ====================================

STREAM_HOST = "localhost"
STREAM_PORT = 6100

OUTPUT_DIR = "output"

# ====================================

os.makedirs(OUTPUT_DIR, exist_ok=True)

def draw_bboxes(frame, bboxes):

    for box in bboxes:

        x = box["x"]
        y = box["y"]
        w = box["w"]
        h = box["h"]

        cv2.rectangle(
            frame,
            (x, y),
            (x + w, y + h),
            (0, 255, 0),
            2
        )

        cv2.putText(
            frame,
            f"{box['score']:.2f}",
            (x, y - 10),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.5,
            (0, 255, 0),
            2
        )

    return frame


def decode_frame(base64_string):
    """
    Base64
        ↓
    bytes
        ↓
    numpy array
        ↓
    OpenCV image
    """

    image_bytes = base64.b64decode(base64_string)

    image_np = np.frombuffer(
        image_bytes,
        dtype=np.uint8
    )

    frame = cv2.imdecode(
        image_np,
        cv2.IMREAD_COLOR
    )

    return frame

def process_partition(iterator):

    detector = PersonDetector()

    for item in iterator:

        try:

            frame_id = item["frame_id"]
            timestamp = item["timestamp"]

            frame = decode_frame(
                item["image"]
            )

            rgb_frame = cv2.cvtColor(
                frame,
                cv2.COLOR_BGR2RGB
            )

            bboxes = detector.detect(
                rgb_frame
            )

            print(
                f"Frame={frame_id} "
                f"Persons={len(bboxes)}"
            )

            result = {
                "frame_id": frame_id,
                "timestamp": timestamp,
                "person_count": len(bboxes),
                "bboxes": bboxes
            }

            json_path = (
                f"{OUTPUT_DIR}/{frame_id}.json"
            )

            with open(
                json_path,
                "w",
                encoding="utf-8"
            ) as f:

                json.dump(
                    result,
                    f,
                    indent=4,
                    ensure_ascii=False
                )

            output_frame = draw_bboxes(
                frame.copy(),
                bboxes
            )

            image_path = (
                f"{OUTPUT_DIR}/{frame_id}.jpg"
            )

            cv2.imwrite(
                image_path,
                output_frame
            )

        except Exception as e:

            print(
                f"Processing error: {e}"
            )

    detector.close()


# ====================================
# SPARK
# ====================================

spark = (
    SparkSession.builder
    .appName("PersonCountingStreaming")
    .getOrCreate()
)

sc = spark.sparkContext

sc.setLogLevel("ERROR")

ssc = StreamingContext(sc, 1)

stream = ssc.socketTextStream(
    STREAM_HOST,
    STREAM_PORT
)

json_stream = stream.map(
    lambda x: json.loads(x)
)

json_stream.foreachRDD(
    lambda rdd: rdd.foreachPartition(
        process_partition
    )
)

print(
    f"Connecting to "
    f"{STREAM_HOST}:{STREAM_PORT}"
)

ssc.start()
ssc.awaitTermination()