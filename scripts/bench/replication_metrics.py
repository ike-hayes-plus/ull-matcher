def required_standby_acks(mode: str, standby_count: int) -> int:
    if mode == "any":
        return min(1, standby_count)
    if mode == "quorum":
        return 0 if standby_count == 0 else (standby_count // 2) + 1
    if mode == "all":
        return standby_count
    raise ValueError(f"unsupported standby commit mode: {mode}")


def mode_watermark(mode: str, standby_payloads: list[dict], field: str = "lastDurableSequence") -> int:
    if not standby_payloads:
        return 0
    required = required_standby_acks(mode, len(standby_payloads))
    if required == 0:
        return 0
    values = sorted((int(item.get(field, 0)) for item in standby_payloads), reverse=True)
    return values[required - 1]


def all_standbys_watermark(standby_payloads: list[dict], field: str = "lastDurableSequence") -> int:
    if not standby_payloads:
        return 0
    return min(int(item.get(field, 0)) for item in standby_payloads)


def mode_ready(mode: str, standby_payloads: list[dict], target_sequence: int) -> bool:
    required = required_standby_acks(mode, len(standby_payloads))
    if required == 0:
        return True
    ready = 0
    for item in standby_payloads:
        if (
            int(item.get("lastDurableSequence", 0)) >= target_sequence
            and int(item.get("lastAppliedSequence", 0)) >= target_sequence
        ):
            ready += 1
    return ready >= required


def watermark_delta(before: int, after: int) -> int:
    return max(0, int(after) - int(before))
