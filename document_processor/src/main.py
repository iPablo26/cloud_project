import os
import sys
import base64
import json
import logging
import re
from datetime import datetime, timezone
from collections import Counter

from flask import Flask, request, jsonify

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Ensure project root is in sys.path and compile Protobuf dynamically
def compile_proto_if_needed():
    src_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(src_dir)
    proto_path = os.path.join(project_dir, "schema.proto")
    output_pb2 = os.path.join(project_dir, "schema_pb2.py")
    
    if project_dir not in sys.path:
        sys.path.insert(0, project_dir)
        
    if not os.path.exists(output_pb2):
        logging.info("Compiling schema.proto dynamically...")
        try:
            import grpc_tools.protoc
            grpc_tools.protoc.main([
                "grpc_tools.protoc",
                f"-I{project_dir}",
                f"--python_out={project_dir}",
                proto_path
            ])
            logging.info("Protobuf compilation completed successfully.")
        except Exception as e:
            logging.error(f"Error compiling proto dynamically: {e}")
            sys.exit(1)

compile_proto_if_needed()

# Import the compiled schema and GCP libraries
import schema_pb2
from google.cloud import storage
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types
from google.protobuf import descriptor_pb2

app = Flask(__name__)

# Initialize GCP clients lazily to prevent errors if credentials aren't set during module load (e.g. in tests)
_storage_client = None
_bq_write_client = None

def get_storage_client():
    global _storage_client
    if _storage_client is None:
        _storage_client = storage.Client()
    return _storage_client

def get_bq_write_client():
    global _bq_write_client
    if _bq_write_client is None:
        _bq_write_client = bigquery_storage_v1.BigQueryWriteClient()
    return _bq_write_client


def process_document(file_bytes, filename):
    """
    Simulates OCR processing of the file:
    - For text files: parses text, counts words, extracts tags, and detects dates.
    - For binary files: estimates counts/tags based on size.
    """
    basename = os.path.basename(filename)
    suffix = os.path.splitext(basename)[1].lower()
    
    word_count = 0
    tags = []
    detected_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    if suffix == ".txt":
        try:
            text = file_bytes.decode("utf-8")
        except UnicodeDecodeError:
            text = file_bytes.decode("latin-1", errors="ignore")
        
        # Word count
        words = re.findall(r"\b\w+\b", text)
        word_count = len(words)
        
        # Generate tags: top 5 words of len > 4, ignoring stop words
        stop_words = {
            "the", "and", "of", "to", "in", "is", "that", "it", "on", "for", "as",
            "with", "was", "at", "by", "an", "be", "this", "are", "from", "or", "you"
        }
        candidate_tags = [w.lower() for w in words if len(w) > 4 and w.lower() not in stop_words]
        tag_counts = Counter(candidate_tags)
        tags = [t for t, count in tag_counts.most_common(5)]
        
        # Date detection
        # Format 1: YYYY-MM-DD
        date_match = re.search(r"\b(\d{4})[-/](\d{2})[-/](\d{2})\b", text)
        if date_match:
            detected_date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
        else:
            # Format 2: DD/MM/YYYY
            date_match2 = re.search(r"\b(\d{2})/(\d{2})/(\d{4})\b", text)
            if date_match2:
                detected_date = f"{date_match2.group(3)}-{date_match2.group(2)}-{date_match2.group(1)}"
    else:
        # Binary simulated OCR
        file_size = len(file_bytes)
        word_count = max(10, file_size // 100)
        
        # Generate tags based on extension and filename
        tags = ["ocr", suffix.replace(".", ""), "binary"]
        name_clean = re.sub(r"[^a-zA-Z]", " ", basename).lower()
        name_words = [w for w in name_clean.split() if len(w) > 4]
        tags.extend(name_words)
        tags = list(set(tags))[:5]
        
    return {
        "filename": filename,
        "date": detected_date,
        "tags": tags,
        "word_count": word_count
    }


def stream_to_bigquery(metadata):
    """
    Streams row to BigQuery using the Storage Write API default stream.
    """
    project_id = os.environ.get("PROJECT_ID")
    dataset_id = os.environ.get("DATASET_ID")
    table_id = os.environ.get("TABLE_ID")
    
    if not all([project_id, dataset_id, table_id]):
        raise ValueError("Environment variables PROJECT_ID, DATASET_ID, and TABLE_ID must be set.")
        
    client = get_bq_write_client()
    write_stream = f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}/_default"
    
    # 1. Create DescriptorProto
    proto_descriptor = descriptor_pb2.DescriptorProto()
    schema_pb2.DocumentMetadata.DESCRIPTOR.CopyToProto(proto_descriptor)
    proto_schema = types.ProtoSchema(proto_descriptor=proto_descriptor)
    
    # 2. Serialize Row
    row = schema_pb2.DocumentMetadata()
    row.filename = metadata["filename"]
    row.date = metadata["date"]
    row.tags.extend(metadata["tags"])
    row.word_count = metadata["word_count"]
    
    proto_rows = types.ProtoRows()
    proto_rows.serialized_rows.append(row.SerializeToString())
    
    # 3. Create request template with schema on first record (since we only send one record here, we include the schema)
    request = types.AppendRowsRequest()
    request.write_stream = write_stream
    request.proto_rows = types.AppendRowsRequest.ProtoData(
        writer_schema=proto_schema,
        rows=proto_rows
    )
    
    # 4. Append rows
    response_stream = client.append_rows(requests=iter([request]))
    
    # Consume the response to confirm write success
    for response in response_stream:
        if response.error.code != 0:
            raise RuntimeError(f"Storage Write API append failed: {response.error.message} (Code: {response.error.code})")
        logging.info(f"Storage Write API successfully inserted row: {metadata['filename']}")
        break


@app.route("/", methods=["POST"])
def handle_webhook():
    """
    Accepts Pub/Sub Push subscription payloads, processes document and stores metadata in BigQuery.
    """
    envelope = request.get_json(silent=True)
    if not envelope:
        logging.error("No JSON payload received.")
        return jsonify({"error": "Bad Request: No JSON payload"}), 400

    if not isinstance(envelope, dict) or "message" not in envelope:
        logging.error("Invalid Pub/Sub message format.")
        return jsonify({"error": "Bad Request: Invalid Pub/Sub message format"}), 400

    pubsub_message = envelope["message"]
    if not isinstance(pubsub_message, dict) or "data" not in pubsub_message:
        logging.error("No data field in Pub/Sub message.")
        return jsonify({"error": "Bad Request: No data field"}), 400

    try:
        data_str = base64.b64decode(pubsub_message["data"]).decode("utf-8")
        data = json.loads(data_str)
    except Exception as e:
        logging.error(f"Failed to decode base64 payload: {e}")
        return jsonify({"error": "Bad Request: Invalid base64/JSON payload"}), 400

    bucket = data.get("bucket")
    filename = data.get("name")

    if not bucket or not filename:
        logging.warning("Decoded message does not contain storage object info (missing bucket/name).")
        return jsonify({"status": "Ignored: missing bucket or name"}), 200

    logging.info(f"Received processing request for file: gs://{bucket}/{filename}")

    try:
        # Download document from Cloud Storage
        storage_client = get_storage_client()
        bucket_obj = storage_client.bucket(bucket)
        blob = bucket_obj.blob(filename)
        
        if not blob.exists():
            logging.error(f"File gs://{bucket}/{filename} not found.")
            return jsonify({"error": "File not found"}), 404
            
        file_bytes = blob.download_as_bytes()
        
        # Simulate OCR / extraction
        metadata = process_document(file_bytes, filename)
        logging.info(f"Metadata extracted: {metadata}")
        
        # Stream results to BigQuery
        stream_to_bigquery(metadata)
        
        return jsonify({"status": "Success", "metadata": metadata}), 200

    except Exception as e:
        logging.exception(f"Failed to process document gs://{bucket}/{filename}: {e}")
        return jsonify({"error": f"Internal Server Error: {str(e)}"}), 500


@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"status": "healthy"}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
