import os
import arxiv
import pyarrow.parquet as pq
import time

parquet_file_path = input("Enter the path to the Parquet file: ")

table = pq.read_table(parquet_file_path)
paper_ids = table.column('id').to_pylist()

download_dir = "arXiv_pdf"
os.makedirs(download_dir, exist_ok=True)

log_file = "failed_arXiv.txt"

with open(log_file, 'w') as log:
    for index, paper_id in enumerate(paper_ids):
        try:
            paper = next(arxiv.Client().results(arxiv.Search(id_list=[paper_id])))

            file_path = os.path.join(download_dir, f"{paper_id}.pdf")

            paper.download_pdf(filename=file_path)

            print(f"Downloaded {paper_id} at index {index}")
        
        except Exception as e:
            log.write(f"Failed to download {paper_id} at index {index}: {e}\n")
            print(f"Failed to download {paper_id} at index {index}: {e}")
        
        time.sleep(4)
