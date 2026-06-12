import mediapipe as mp

BaseOptions = mp.tasks.BaseOptions
ObjectDetector = mp.tasks.vision.ObjectDetector
ObjectDetectorOptions = mp.tasks.vision.ObjectDetectorOptions
VisionRunningMode = mp.tasks.vision.RunningMode


class PersonDetector:

    def __init__(self):

        options = ObjectDetectorOptions(
            base_options=BaseOptions(
                model_asset_path="model/efficientdet_lite0.tflite"
            ),
            score_threshold=0.5,
            max_results=20,
            running_mode=VisionRunningMode.IMAGE
        )

        self.detector = ObjectDetector.create_from_options(
            options
        )

    def detect(self, rgb_frame):

        mp_image = mp.Image(
            image_format=mp.ImageFormat.SRGB,
            data=rgb_frame
        )

        detection_result = self.detector.detect(
            mp_image
        )

        bboxes = []

        for detection in detection_result.detections:

            category = detection.categories[0]

            category_name = category.category_name

            # Chỉ lấy người
            if category_name != "person":
                continue

            bbox = detection.bounding_box

            bboxes.append({
                "x": int(bbox.origin_x),
                "y": int(bbox.origin_y),
                "w": int(bbox.width),
                "h": int(bbox.height),
                "score": round(
                    float(category.score),
                    3
                )
            })

        return bboxes

    def close(self):
        self.detector.close()