from pipeline.validate import validate_flow

def make_valid_flow():
    return {
        "id": "1",
        "dur": "0.121",
        "proto": "tcp",
        "service": "-",
        "state": "FIN",
        "spkts": "6",
        "dpkts": "4",
        "sbytes": "258",
        "dbytes": "172",
        "attack_cat": "Normal",
        "label": "0",
    }

def test_valid_flow():
    payload = make_valid_flow()

    record, errors = validate_flow(payload)

    assert errors == []
    assert record["dur"] == 0.121
    assert record["label"] == 0
    assert record["sbytes"] == 258

def test_negative_duration():
    payload = make_valid_flow()
    payload["dur"] = "-5.2"

    record, errors = validate_flow(payload)

    assert record is None
    assert "dur must be >= 0" in errors

def test_invalid_label():
    payload = make_valid_flow()
    payload["label"] = "3"

    record, errors = validate_flow(payload)

    assert record is None
    assert "label must be 0 or 1" in errors

def test_multiple_errors():
    payload = make_valid_flow()

    payload["dur"] = "-5.2"
    payload["sbytes"] = "INVALID"
    payload["label"] = "3"

    record, errors = validate_flow(payload)

    assert record is None
    assert len(errors) == 3

def test_nonfinite_duration():
    payload = make_valid_flow()
    payload["dur"] = "NaN"

    record, errors = validate_flow(payload)

    assert record is None
    assert "dur must be finite" in errors
