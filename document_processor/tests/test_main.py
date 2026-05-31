import os
import sys
import base64
import json
import pytest

# Ensure document_processor root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Compile Protobuf if needed before importing main
import src.main as main

@pytest.fixture
def client():
    main.app.config["TESTING"] = True
    with main.app.test_client() as client:
        yield client


def test_healthz(client):
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.get_json() == {"status": "healthy"}


def test_process_document_text_file():
    text_content = (
        "This is a sample document for testing the OCR pipeline.\n"
        "The date of this report is 2026-05-30.\n"
        "Let's see if the word count works correctly, and if tags are extracted."
    )
    file_bytes = text_content.encode("utf-8")
    
    metadata = main.process_document(file_bytes, "docs/report.txt")
    
    assert metadata["filename"] == "docs/report.txt"
    assert metadata["date"] == "2026-05-30"
    assert metadata["word_count"] > 10
    assert "report" in metadata["tags"] or "testing" in metadata["tags"] or "document" in metadata["tags"]


def test_process_document_text_file_alternative_date():
    text_content = (
        "Some random document.\n"
        "Created on 15/08/2025 by John Doe."
    )
    file_bytes = text_content.encode("utf-8")
    
    metadata = main.process_document(file_bytes, "docs/info.txt")
    
    assert metadata["filename"] == "docs/info.txt"
    assert metadata["date"] == "2025-08-15"
    assert metadata["word_count"] == 11


def test_process_document_binary_file():
    # 500 bytes of dummy binary data
    file_bytes = b"\x00\x01\x02\x03" * 125
    metadata = main.process_document(file_bytes, "uploads/invoice.pdf")
    
    assert metadata["filename"] == "uploads/invoice.pdf"
    assert metadata["word_count"] == 10  # 500 // 100 = 5, but min is 10
    assert "pdf" in metadata["tags"]
    assert "invoice" in metadata["tags"]


def test_handle_webhook_invalid_json(client):
    response = client.post("/", data="not-json", content_type="text/plain")
    assert response.status_code == 400
    assert "error" in response.get_json()


def test_handle_webhook_invalid_format(client):
    response = client.post("/", json={"invalid": "payload"})
    assert response.status_code == 400
    assert "error" in response.get_json()


def test_handle_webhook_missing_data(client):
    response = client.post("/", json={"message": {}})
    assert response.status_code == 400


def test_handle_webhook_ignored_event(client):
    # A message with data but no bucket/name
    payload = {
        "message": {
            "data": base64.b64encode(json.dumps({"some_key": "some_value"}).encode("utf-8")).decode("utf-8")
        }
    }
    response = client.post("/", json=payload)
    assert response.status_code == 200
    assert "Ignored" in response.get_json()["status"]


def test_handle_webhook_file_not_found(client, mocker):
    # Mock storage client to raise exception / not exist
    mock_storage = mocker.patch("src.main.get_storage_client")
    mock_bucket = mocker.MagicMock()
    mock_blob = mocker.MagicMock()
    mock_blob.exists.return_value = False
    mock_bucket.blob.return_value = mock_blob
    mock_storage.return_value.bucket.return_value = mock_bucket

    payload = {
        "message": {
            "data": base64.b64encode(json.dumps({
                "bucket": "test-bucket",
                "name": "missing.txt"
            }).encode("utf-8")).decode("utf-8")
        }
    }
    
    response = client.post("/", json=payload)
    assert response.status_code == 404
    assert "File not found" in response.get_json()["error"]


def test_handle_webhook_success(client, mocker):
    # Mock storage client
    mock_storage = mocker.patch("src.main.get_storage_client")
    mock_bucket = mocker.MagicMock()
    mock_blob = mocker.MagicMock()
    mock_blob.exists.return_value = True
    mock_blob.download_as_bytes.return_value = b"Hello world! This is a test file for the serverless pipeline. Date is 2026-05-30."
    mock_bucket.blob.return_value = mock_blob
    mock_storage.return_value.bucket.return_value = mock_bucket

    # Mock BigQuery stream write
    mock_stream = mocker.patch("src.main.stream_to_bigquery")

    payload = {
        "message": {
            "data": base64.b64encode(json.dumps({
                "bucket": "test-bucket",
                "name": "test.txt"
            }).encode("utf-8")).decode("utf-8")
        }
    }
    
    response = client.post("/", json=payload)
    assert response.status_code == 200
    res_data = response.get_json()
    assert res_data["status"] == "Success"
    assert res_data["metadata"]["filename"] == "test.txt"
    assert res_data["metadata"]["word_count"] == 16
    assert res_data["metadata"]["date"] == "2026-05-30"
    
    # Verify BigQuery stream was called with matching metadata
    mock_stream.assert_called_once_with(res_data["metadata"])
