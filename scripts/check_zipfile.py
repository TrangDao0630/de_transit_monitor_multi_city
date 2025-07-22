import zipfile
import os

# Ensure you are in your project directory
project_path = os.getcwd() # Should already be de_transit_monitor_multi_city

zip_files_to_check = ["mta_nyc_data.zip", "bart_sf_data.zip"] # Use the exact names you confirmed

for zip_filename in zip_files_to_check:
    full_zip_path = os.path.join(project_path, zip_filename)
    print(f"\n--- Contents of {zip_filename} ---")
    if os.path.exists(full_zip_path):
        try:
            with zipfile.ZipFile(full_zip_path, 'r') as z:
                # Get a list of all files/paths within the zip
                namelist = z.namelist()
                if not namelist:
                    print("  (Zip file appears empty or corrupted)")
                else:
                    for name in namelist:
                        print(f"  - {name}")
        except zipfile.BadZipFile:
            print(f"  Error: {zip_filename} is a bad or corrupted zip file.")
        except Exception as e:
            print(f"  An unexpected error occurred: {e}")
    else:
        print(f"  Error: {zip_filename} not found at {full_zip_path}. Please ensure the .zip file is in your project directory.")

print("\n--- Zip file content check complete ---")