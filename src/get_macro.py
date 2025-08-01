import os
import csv
import openslide
from openslide.deepzoom import DeepZoomGenerator
import argparse
from concurrent.futures import ThreadPoolExecutor
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def extract_macro_image(svs_path, output_dir, maintain_structure=True):
    """Extract macro/thumbnail/label images from an SVS file and save them in distinct folders."""
    try:
        # Open the SVS file
        slide = openslide.OpenSlide(svs_path)

        # Get the associated images (thumbnail, macro, label)
        associated_images = slide.associated_images

        # Determine relative path for output if maintaining structure
        if maintain_structure:
            rel_path = os.path.dirname(os.path.relpath(svs_path, args.input_dir))
            macro_dir = os.path.join(output_dir, "macro", rel_path)
            label_dir = os.path.join(output_dir, "label", rel_path)
            thumbnail_dir = os.path.join(output_dir, "thumbnail", rel_path)
        else:
            macro_dir = os.path.join(output_dir, "macro")
            label_dir = os.path.join(output_dir, "label")
            thumbnail_dir = os.path.join(output_dir, "thumbnail")

        # Create directories if they don't exist
        os.makedirs(macro_dir, exist_ok=True)
        os.makedirs(label_dir, exist_ok=True)
        os.makedirs(thumbnail_dir, exist_ok=True)

        # Base filename without extension
        base_name = os.path.splitext(os.path.basename(svs_path))[0]

        # Save macro image
        if "macro" in associated_images:
            macro_path = os.path.join(macro_dir, f"{base_name}_macro.png")
            associated_images["macro"].save(macro_path)
            logger.info(f"Extracted macro image from {svs_path}")
        else:
            macro_path = None

        # Save label image
        if "label" in associated_images:
            label_path = os.path.join(label_dir, f"{base_name}_label.png")
            associated_images["label"].save(label_path)
            logger.info(f"Extracted label image from {svs_path}")
        else:
            label_path = None

        # Save thumbnail image
        if "thumbnail" in associated_images:
            thumbnail_path = os.path.join(thumbnail_dir, f"{base_name}_thumbnail.png")
            associated_images["thumbnail"].save(thumbnail_path)
            logger.info(f"Extracted thumbnail image from {svs_path}")
        else:
            # If no thumbnail, generate one from the base image
            logger.warning(
                f"No thumbnail found in {svs_path}, generating from base image"
            )
            dz = DeepZoomGenerator(slide)
            thumbnail = dz.get_thumbnail((300, 300))
            thumbnail_path = os.path.join(thumbnail_dir, f"{base_name}_thumbnail.png")
            thumbnail.save(thumbnail_path)

        # Close the slide to free resources
        slide.close()

        return {
            "macro": macro_path,
            "label": label_path,
            "thumbnail": thumbnail_path,
            "original": svs_path,
        }
    except Exception as e:
        logger.error(f"Error processing {svs_path}: {str(e)}")
        return None


def process_svs_files(input_dir, output_dir, csv_path, num_workers=4):
    """Process all SVS files in the input directory and subdirectories."""
    # Find all SVS files
    svs_files = []
    for root, _, files in os.walk(input_dir):
        for file in files:
            if file.lower().endswith(".svs"):
                svs_files.append(os.path.join(root, file))

    logger.info(f"Found {len(svs_files)} SVS files to process")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Process files with thread pool to minimize server load
    results = []
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(extract_macro_image, svs_path, output_dir)
            for svs_path in svs_files
        ]

        for future in futures:
            result = future.result()
            if result:  # If macro extraction was successful
                results.append(result)

    # Write CSV mapping
    with open(csv_path, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(
            ["macro_path", "label_path", "thumbnail_path", "original_slide_location"]
        )
        for result in results:
            csv_writer.writerow(
                [
                    result["macro"],
                    result["label"],
                    result["thumbnail"],
                    result["original"],
                ]
            )

    logger.info(f"Successfully processed {len(results)} out of {len(svs_files)} files")
    logger.info(f"CSV mapping saved to {csv_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract macro/thumbnail images from SVS files"
    )
    parser.add_argument(
        "--input-dir", required=True, help="Input directory containing SVS files"
    )
    parser.add_argument(
        "--output-dir", required=True, help="Output directory for macro images"
    )
    parser.add_argument(
        "--csv-path",
        default="svs_mapping.csv",
        help="Path to save the CSV mapping file",
    )
    parser.add_argument(
        "--workers", type=int, default=1, help="Number of worker threads"
    )

    args = parser.parse_args()

    process_svs_files(args.input_dir, args.output_dir, args.csv_path, args.workers)
