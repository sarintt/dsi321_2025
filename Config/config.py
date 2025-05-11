import os

CODE_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.dirname(CODE_DIR)
DATA_DIR = os.path.join(ROOT_DIR, "data")

# ---------------------------------- Config ---------------------------------- #
CONFIG_DIR = os.path.join(ROOT_DIR, "config")
CREDENTIALS_DIR = os.path.join(CONFIG_DIR, "credentials")
FILE_CREDENTIALS = os.path.join(CREDENTIALS_DIR, "oauth-client-id.json")

FILE_EXTRACT = os.path.join(DATA_DIR, "extract_data.jsonl")
COLLECTION_NAME = "qdrant_collection"
BUCKET_NAME = "document"

BUCKET_POLICY = """
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": ["*"]
            },
            "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
            "Resource": ["arn:aws:s3:::document"]
        },
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": ["*"]
            },
            "Action": ["s3:GetObject"],
            "Resource": ["arn:aws:s3:::document/*"]
        }
    ]
}
"""