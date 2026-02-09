import kagglehub
import os
import glob

def download_dataset(slug):
    # This downloads the latest version and returns the cache path
    print(f"Kagglehub is fetching: {slug}")
    download_path = kagglehub.dataset_download(slug)
    
    # Search for any CSV file inside the downloaded folder
    csv_files = glob.glob(os.path.join(download_path, "*.csv"))
    
    if not csv_files:
        raise Exception("No CSV found in the downloaded dataset!")
    
    # Return the first CSV found
    return csv_files[0]