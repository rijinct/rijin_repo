import os
import hashlib

def get_file_checksum(file_path):
    """
    Returns the SHA-256 checksum of a file.
    """
    with open(file_path, 'rb') as f:
        file_data = f.read()
        return hashlib.sha256(file_data).hexdigest()

def delete_duplicate_screenshots(folder_path):
    """
    Deletes duplicate screenshots in a folder.
    """
    # Create a dictionary to store checksums and file paths.
    checksum_dict = {}

    # Iterate over files in the folder.
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)

        # Ignore directories and non-image files.
        if os.path.isdir(file_path) or not file_name.lower().endswith('.png'):
            continue

        # Get the checksum of the file.
        file_checksum = get_file_checksum(file_path)
        print(file_checksum)
        # Check if the checksum already exists in the dictionary.
        if file_checksum in checksum_dict:
            # Delete the duplicate file.
            os.remove(file_path)
            print(f'Deleted {file_name} (duplicate of {checksum_dict[file_checksum]})')
        else:
            # Add the checksum and file path to the dictionary.
            checksum_dict[file_checksum] = file_name

            # Print a message for new files.
            print(f'Found {file_name}')

# Example usage:
delete_duplicate_screenshots(r'C:\Users\rijin.thomas\OneDrive - Gold Corporation\Desktop\screenshots')