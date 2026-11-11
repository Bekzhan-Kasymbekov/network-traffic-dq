import math

def parse_integer(value, field, errors, minimum=None):
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError):
        errors.append(f"{field} must be an integer")
        return None

    if minimum is not None and result < minimum:
        errors.append(f"{field} must >= {minimum}")

    return result

def parse_float(value, field, errors, minimum=None):
    try:
        result = float(value)
    except(TypeError, ValueError, OverflowError):
        errors.append(f"{field} must be numeric")
        return None

    if not math.isfinite(result):
        errors.append(f"{field} must be finite")
        return None

    if minimum is not None and result < minimum:
        errors.append(f"{field} must be >= {minimum}")

    return result

def validate_flow(payload):
    errors = []

    flow_id = parse_integer(
        payload.get("id"), "id", errors, minimum=1    
    )

    dur = parse_float(
        payload.get("dur"), "dur", errors, minimum=0
    )

    spkts = parse_integer(
        payload.get("spkts"), "spkts", errors, minimum=0
    )

    dpkts = parse_integer(
        payload.get("dpkts"), "dpkts", errors, minimum=0
    )

    sbytes = parse_integer(
        payload.get("sbytes"), "sbytes", errors, minimum=0
    )

    dbytes = parse_integer(
        payload.get("dbytes"), "dbytes", errors, minimum=0
    )

    label = parse_integer(
        payload.get("label"), "label", errors
    )

    if label is not None and label not in (0, 1):
        errors.append("label must be 0 or 1")

    proto = payload.get("proto")
    state = payload.get("state")

    if not isinstance(proto, str) or not proto.strip():
        errors.append("proto is required")

    if not isinstance(state, str) or not state.strip():
        errors.append("state is required")

    if errors:
        return None, errors

    clean_record = {
        "flow_id": flow_id,
        "dur": dur,
        "proto": proto,
        "service": payload.get("service"),
        "state": state,
        "spkts": spkts,
        "dpkts": dpkts,
        "sbytes": sbytes,
        "dbytes": dbytes,
        "attack_cat": payload.get("attack_cat"),
        "label": label,
    }

    return clean_record, []
