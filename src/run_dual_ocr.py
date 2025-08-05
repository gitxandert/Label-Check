import os
import argparse
import easyocr
import PIL
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import cv2

# Initialize the EasyOCR reader
reader = easyocr.Reader(['en'], gpu=True)

def preprocess_image_for_ocr(image_np):
    """
    Applies grayscale and binarization to an image to improve OCR accuracy.
    The final image is converted back to 3 channels.
    """
    # 1. Convert image to grayscale
    # OpenCV expects BGR, but PIL opens as RGB, so convert RGB to Gray
    gray_image = cv2.cvtColor(image_np, cv2.COLOR_RGB2GRAY)

    # 2. Apply binarization using Otsu's thresholding
    # This automatically determines the best threshold value
    _, binary_image = cv2.threshold(gray_image, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

    # 3. Convert the single-channel binary image back to a 3-channel image
    # EasyOCR can work with 3-channel images, and this preserves the processed result
    # in a standard format.
    three_channel_image = cv2.cvtColor(binary_image, cv2.COLOR_GRAY2RGB)
    
    return three_channel_image

def process_single_image(patient, label_subdir, macro_subdir, csv_path):
    """Process a single patient's label and macro images with preprocessing."""
    label_path = os.path.join(label_subdir, f"{patient}_label.png")
    macro_path = os.path.join(macro_subdir, f"{patient}_macro.png")

    # Check if the files exist
    if not os.path.exists(label_path):
        print(f"Label image not found for {patient}. Skipping.")
        return

    if not os.path.exists(macro_path):
        print(f"Macro image not found for {patient}. Skipping.")
        return
    
    # Read the images with PIL
    image_label_pil = PIL.Image.open(label_path)
    image_macro_pil = PIL.Image.open(macro_path)
    
    # Perform existing image manipulations
    img_macro_pil = image_macro_pil.rotate(-90, expand=True)
    width, height = img_macro_pil.size
    if width > height:
        img_macro_pil = img_macro_pil.crop((0, 0, width / 2, height))
    else:
        img_macro_pil = img_macro_pil.crop((0, 0, width, height / 2))
    
    # Convert PIL images to NumPy arrays for processing
    image_label_np = np.array(image_label_pil)
    image_macro_np = np.array(img_macro_pil)

    # **Apply new image processing steps**
    processed_label_image = preprocess_image_for_ocr(image_label_np)
    processed_macro_image = preprocess_image_for_ocr(image_macro_np)
    
    # Process the preprocessed images with OCR
    label_text = " ".join([text[1] for text in reader.readtext(processed_label_image)])
    macro_text = " ".join([text[1] for text in reader.readtext(processed_macro_image, rotation_info=[0, 90, 180, 270])])

    # Save the results
    combined_text = f"Label_Path:{label_path};Macro_Path:{macro_path};Label: {label_text};Macro: {macro_text}"
    with open(csv_path, "a") as f:
        f.write(f"{patient};{combined_text}\n")
    print(f"Processed {patient}: Label and Macro images with preprocessing.")

def process_image_files(input_dir, csv_path, num_workers=4):
    """Process all label and macro images in parallel."""
    label_subdir = os.path.join(input_dir, "label")
    macro_subdir = os.path.join(input_dir, "macro")

    # Get patient slide names
    patient_slide_names = [
        os.path.basename(image).split("_label.png")[0]
        for image in os.listdir(label_subdir)
        if image.endswith("_label.png")
    ]

    # Process images in parallel
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(process_single_image, patient, label_subdir, macro_subdir, csv_path)
            for patient in patient_slide_names
        ]

        # Wait for all tasks to complete
        for future in futures:
            future.result()

# Configuration for the script
input_dir = r"2025-07-20"
output_path = r"2025-07-20\output-ocr-processed.csv"
workers = 20

# Ensure the output CSV file is ready for writing
with open(output_path, "w") as f:
     f.write("Patient;Combined Text\n")

process_image_files(input_dir, output_path, workers)

# The commented-out command-line argument parser is retained for future use.
# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(
#         description="Extract text from images using OCR with preprocessing."
#     )
#     parser.add_argument(
#         "--input-dir", required=True, help="Input directory containing 'label' and 'macro' subdirectories."
#     )
#     parser.add_argument(
#         "--output-path", required=True, help="Output path for the CSV file containing the OCR text."
#     )
#     parser.add_argument(
#         "--workers", type=int, default=4, help="Number of worker threads for parallel processing."
#     )
#
#     args = parser.parse_args()
#
#     # Ensure the CSV file is empty before starting
#     with open(args.output_path, "w") as f:
#         f.write("Patient;Combined Text\n")
#
#     process_image_files(args.input_dir, args.output_path, args.workers)