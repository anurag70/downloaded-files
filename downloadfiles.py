import concurrent.futures
import requests

def download_pages(start_page, end_page):
    #if there are more pages we have to increase the no of pages in a chunks 
    #right now assuming this as object storage(files-stored-in-chunk as bucket)
    url = f'https://s3.amazonaws.com/files-stored-in-chunk/file?start={start_page}&end={end_page}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.content 
    else:
        raise Exception(f"Failed to download page {end_page-start_page+1}")

def comibine_downloaded_chuks(chunks):
    combined_file = b''.join(chunks)
    return combined_file

def download_file_in_parallel(total_pages, pages_per_chunk=2):
    total_downloaded_files = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Generate download tasks for each chunk in parallel
        pages = [
            executor.submit(download_pages, i, min(i + pages_per_chunk - 1, total_pages))
            for i in range(1, total_pages + 1, pages_per_chunk)
        ]
        
        # collect the results as they complete
        for future in concurrent.futures.as_completed(pages):
            total_downloaded_files.append(future.result())
    
    # Combine all the parts to form the full file
    return comibine_downloaded_chuks(total_downloaded_files)

total_pages = 60  
downloaded_file = download_file_in_parallel(total_pages)

#temporaly write the file in local storage
with open('final_downloaded_file.txt', 'wb') as f:
        f.write(downloaded_file)

