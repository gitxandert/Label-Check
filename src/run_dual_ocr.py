import os
import argparse
import easyocr
import PIL
import numpy as np
from concurrent.futures import ThreadPoolExecutor

reader = easyocr.Reader(['en'], gpu=True)

def process_single_image(patient, label_subdir, macro_subdir, csv_path):
    """Process a single patient's label and macro images."""
    label_path = os.path.join(label_subdir, f"{patient}_label.png")
    macro_path = os.path.join(macro_subdir, f"{patient}_macro.png")

    # Read the images
    # Check if the files exist
    if not os.path.exists(label_path):
        print(f"Label image not found for {patient}. Skipping.")
        return

    if not os.path.exists(macro_path):
        print(f"Macro image not found for {patient}. Skipping.")
        return
    
    # Read the images
    image_label = PIL.Image.open(label_path)
    image_macro = PIL.Image.open(macro_path)
    img_macro = image_macro.rotate(-90, expand=True)
    # Cut the image by half on the longer side
    width, height = img_macro.size
    if width > height:
        img_macro = img_macro.crop((0, 0, width / 2, height))
    else:
        img_macro = img_macro.crop((0, 0, width, height / 2))
    
    image_label = np.array(image_label)
    image_macro = np.array(img_macro)
    
     # Process the images (e.g., run OCR)
    label_text = " ".join([text[1] for text in reader.readtext(image_label)])
    macro_text = " ".join([text[1] for text in reader.readtext(image_macro, rotation_info=[0, 90, 180, 270])])

    combined_text = f"Label_Path:{label_path};Macro_Path:{macro_path};Label: {label_text};Macro: {macro_text}"
    with open(csv_path, "a") as f:
        f.write(f"{patient};{combined_text}\n")
    print(f"Processed {patient}: Label and Macro images saved.")

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


input_dir = r"2025-07-20"
output_path = r"2025-07-20\output-ocr.csv"
workers=20
process_image_files(input_dir, output_path, workers)
# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(
#         description="Extract macro/thumbnail images from SVS files"
#     )
#     parser.add_argument(
#         "--input-dir", required=True, help="Input directory containing SVS files"
#     )
#     parser.add_argument(
#         "--output-path", required=True, help="Output for the csv containing the text from ocr for files and combined text"
#     )
#     parser.add_argument(
#         "--workers", type=int, default=4, help="Number of worker threads"
#     )

#     args = parser.parse_args()

# Ensure the CSV file is empty before starting
    # with open(args.output_path, "w") as f:
    #     f.write("Patient,Label Text,Macro Text,Combined Text\n")

    # process_image_files(args.input_dir, args.output_path, args.output_path, args.workers)
