import os
import re
import requests
from concurrent.futures import ThreadPoolExecutor

def download_pdb(pdb_id, output_dir):
    # Try .pdb first, then .cif
    for ext in ['pdb', 'cif']:
        url = f"https://files.rcsb.org/download/{pdb_id}.{ext}"
        output_path = os.path.join(output_dir, f"{pdb_id}.{ext}")
        
        # Also check if the other format already exists to avoid double downloads
        if any(os.path.exists(os.path.join(output_dir, f"{pdb_id}.{e}")) for e in ['pdb', 'cif']):
            print(f"Skipping {pdb_id}, structure file already exists.")
            return
        
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                print(f"Downloaded {pdb_id}.{ext}")
                return # Successfully downloaded
            elif response.status_code == 404:
                continue # Try next extension
            else:
                print(f"Failed to download {pdb_id}.{ext}: Status {response.status_code}")
        except Exception as e:
            print(f"Error downloading {pdb_id}.{ext}: {e}")
    
    print(f"Failed to find structure for {pdb_id} in PDB or CIF formats.")

def main():
    target_url = "https://predictioncenter.org/casp15/targetlist.cgi"
    output_dir = "casp15"
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    print(f"Fetching CASP15 target list from {target_url}...")
    try:
        response = requests.get(target_url, timeout=30)
        response.raise_for_status()
    except Exception as e:
        print(f"Error fetching the webpage: {e}")
        return

    # Use regex to find structure/XXXX or structureId=XXXX
    pdb_ids = set()
    pdb_ids.update(re.findall(r'structure/([a-zA-Z0-9]{4})', response.text))
    pdb_ids.update(re.findall(r'structureId=([a-zA-Z0-9]{4})', response.text))
    
    # Normalize to lowercase
    pdb_ids = {pid.lower() for pid in pdb_ids}
    
    print(f"Found {len(pdb_ids)} unique PDB IDs.")
    
    # Use ThreadPoolExecutor for faster downloads
    with ThreadPoolExecutor(max_workers=10) as executor:
        for pdb_id in sorted(pdb_ids):
            executor.submit(download_pdb, pdb_id, output_dir)

if __name__ == "__main__":
    main()
