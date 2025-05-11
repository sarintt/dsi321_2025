import json
from minio import Minio
from minio.error import S3Error
from io import BytesIO
from PyPDF2 import PdfReader
import re
from config.logging_config.modern_log import LoggingConfig
from config.config import BUCKET_NAME, DATA_DIR, FILE_EXTRACT
from tqdm import tqdm
import subprocess
import os

# ---------------------------------------------------------------------------- #
#                                LOGGING CONFIG                                #
# ---------------------------------------------------------------------------- #
logger = LoggingConfig(level="WARNING").get_logger()
# ---------------------------------------------------------------------------- #

def clean_text(text: str) -> str:
    """
    Clean text by replacing special characters and normalizing whitespace.
    
    Args:
        text (str): Input text
    
    Returns:
        str: Cleaned text
    """
    
    mapping = {
        0xf700: 'ฐ', 0xf701: 'ิ', 0xf702: 'ี', 0xf703: 'ึ', 0xf704: 'ื',
        0xf705: '่', 0xf706: '้', 0xf707: '๊', 0xf708: '๋', 0xf709: '์',
        0xf70a: '่', 0xf70b: '้', 0xf70c: '๊', 0xf70d: '๋', 0xf70e: '์',
        0xf70f: 'ญ', 0xf710: 'ั', 0xf711: 'ํ', 0xf712: '็', 0xf713: '่',
        0xf714: '้', 0xf715: '๊', 0xf716: '๋', 0xf717: '์', 0xf718: 'ุ',
        0xf719: 'ู', 0xf71a: 'ฺ', 0xf71b: 'ฎ', 0xf71c: 'ฏ', 0xf71d: 'ฬ',
        0xf880: '๏', 0xf881: 'ฤ', 0xf882: 'ฦ'
    }
    if text:
        text = text.replace("\n", " ")
        text = re.sub(r'\s+', ' ', text)
        return " ".join(text.split()[1:]).strip().translate(mapping)
    return ""

class MinioExtract:
    def __init__(self):
        """
        Initialize the Minio client and set the bucket name.

        The Minio client is created with the default values for the Minio
        container, and the bucket name is set to the value specified in the
        configuration file.

        :param: None
        :return: None
        """
        self.minio_client = Minio(
            endpoint="localhost:9000",
            access_key="minio",
            secret_key="minio123",
            secure=False
        )
        self.bucket_name = BUCKET_NAME

    def object_count_bucket(self) -> int:
        """
        Count the number of objects in the Minio bucket.

        This method uses the Minio client to execute a Docker command that
        retrieves metadata about the specified bucket. It then extracts and
        returns the count of objects in the bucket.

        Returns:
            int: The number of objects in the bucket if successful, otherwise logs an error.
        """
        alias_name = 'minio'
        subprocess.run([
            'docker', 'exec', 'minio', 'mc', 'alias', 'set', alias_name,
            os.getenv('MINIO_ENDPOINT', 'http://localhost:9000'),
            os.getenv('MINIO_ROOT_USER', 'minio'),
            os.getenv('MINIO_ROOT_PASSWORD', 'minio123')
        ], capture_output=True, text=True)
        result = subprocess.run([
            'docker', 'exec', 'minio', 'mc', 'stat', f'{alias_name}/document'
        ], capture_output=True, text=True)

        if result.returncode == 0:
            match = re.search(r"Objects count:\s*(\d+)", result.stdout)
            if match:
                objects_count = match.group(1)
                return int(objects_count)
            else:
                logger.error("Objects count not found.")
        else:
            logger.error(f"Error occurred: {result.stderr}")

    def run(self) -> None:
        """
        Execute the extraction and processing of objects from the Minio bucket.

        This method counts the number of objects in the bucket and retrieves
        their metadata. It then processes each object, extracting text content
        from PDFs and writing the metadata and extracted content to a JSONL
        file.

        The process includes:
        - Counting objects in the specified Minio bucket.
        - Listing objects along with their metadata.
        - Extracting text from each PDF object.
        - Saving metadata and extracted content to a file.

        Utilizes the tqdm library to provide a progress bar for processing
        objects.

        Logs:
            Information about the saving of results to a specific file path.
        """

        object_count = self.object_count_bucket()

        logger.info(f"📂 Listing objects in bucket: '{self.bucket_name}'")
        objects = self.minio_client.list_objects(self.bucket_name, recursive=True)

        with open(FILE_EXTRACT, "w", encoding="utf-8") as f:
            for obj in tqdm(objects, desc="Processing PDFs", unit="file", total=object_count, colour='green'):
                stat = self.minio_client.stat_object(self.bucket_name, obj.object_name)
                file_name = obj.object_name.split('/')[-1]
                metadata = {
                    "file_name": file_name,
                    "author_name": stat.metadata.get('x-amz-meta-author_name', 'unknown'),
                    "author_email": stat.metadata.get('x-amz-meta-author_email', 'unknown'),
                    "author_profile": stat.metadata.get('x-amz-meta-author_profile', 'unknown'),
                    "uploaded_date": stat.metadata.get('x-amz-meta-uploaded_date', 'unknown'),
                    "created_date": stat.metadata.get('x-amz-meta-created_date', 'unknown'),
                    "size": stat.size,
                    "filetype": stat.content_type,
                    "location": obj.object_name,
                    "modified_by_name": stat.metadata.get('x-amz-meta-modified_by_name', 'unknown'),
                    "modified_by_email": stat.metadata.get('x-amz-meta-modified_by_email', 'unknown'),
                    "modified_profile": stat.metadata.get('x-amz-meta-modified_profile', 'unknown'),
                    "modified_time": stat.metadata.get('x-amz-meta-modified_time', 'unknown')
                }

                content = self.extract_text_from_pdf_in_minio(self.bucket_name, obj.object_name, metadata)
                if content:
                    for item in content:
                        json.dump(item, f, ensure_ascii=False)
                        f.write("\n")

        logger.info(f"✅ All results saved to '{DATA_DIR}/extracted_text_results.jsonl'")

    # ---------------------------------------------------------------------------- #
    #                                  Extraction                                  #
    # ---------------------------------------------------------------------------- #
    def extract_text_from_pdf_in_minio(self, bucket_name: str, object_name: str, metadata: dict) -> list:
        """
        Extracts text from a PDF stored in Minio.

        Given a Minio bucket name, object name, and metadata about the PDF, this
        function extracts the text from the PDF and returns it. The text is
        returned as a list of JSON objects, each containing the page number and
        the text content of that page.

        Args:
            bucket_name (str): The name of the Minio bucket containing the PDF.
            object_name (str): The name of the object in the Minio bucket.
            metadata (dict): A dictionary of metadata about the PDF.

        Returns:
            list: A list of JSON objects, each containing the page number and
            the text content of that page.

        Logs:
            Error message if an error occurs while extracting text from the PDF.
        """
        try:
            response = self.minio_client.get_object(bucket_name, object_name)
            pdf_stream = BytesIO(response.read())
            response.close()
            response.release_conn()

            content_per_page = self.extract_pdf(
                pdf_stream=pdf_stream,
                metadata=metadata
            )
            return content_per_page
        except Exception as e:
            logger.error(f"❌ Error reading PDF from MinIO: {e}")
            return []

    def extract_pdf(self, pdf_stream: BytesIO, metadata: dict) -> list:
        """
        Extracts text from a PDF stored in a BytesIO object.

        Given a BytesIO object containing a PDF and metadata about the PDF, this
        function extracts the text from the PDF and returns it. The text is
        returned as a list of JSON objects, each containing the page number and
        the text content of that page.

        Args:
            pdf_stream (BytesIO): A BytesIO object containing a PDF.
            metadata (dict): A dictionary of metadata about the PDF.

        Returns:
            list: A list of JSON objects, each containing the page number and
            the text content of that page.

        Logs:
            A success message if the text is extracted successfully.
        """
        reader = PdfReader(pdf_stream)

        content_per_page = []
        for page_num, page in enumerate(reader.pages):
            text = page.extract_text()
            cleaned_text = clean_text(text)
            if cleaned_text.strip() and "/uni0E" not in cleaned_text:
                content_per_page.append({
                    **metadata,
                    "content": cleaned_text,
                    "page": page_num + 1,
                })
        logger.info(f"✅ Extracted text from: '{metadata['file_name']}'")
        return content_per_page
    
if __name__ == "__main__":
    minio_embed = MinioExtract()
    minio_embed.run()