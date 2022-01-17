import cv2
import matplotlib.pyplot as plt
import neptune.new as neptune

if __name__ == "__main__":
    run = neptune.init()

    img_bgr = cv2.imread("opencv_logo.png", cv2.IMREAD_UNCHANGED)
    img_rgb = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)

    plt.imshow(img_bgr)
    plt.imshow(img_rgb)  # correct image

    run["img_bgr"].upload(neptune.types.File.as_image(img_bgr))
    run["img_rgb"].upload(neptune.types.File.as_image(img_rgb))
    run["img_bgr_scaled"].upload(neptune.types.File.as_image(img_bgr / 255))
    run["img_rgb_scaled"].upload(neptune.types.File.as_image(img_rgb / 255))

    run.stop()
