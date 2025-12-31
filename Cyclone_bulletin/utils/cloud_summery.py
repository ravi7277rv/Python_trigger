import re


def extract_cloud_summaries(text):
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    bay_summary = None
    arab_summary = None
    for i, ln in enumerate(lines):
        up = ln.upper()
        if "BAY OF BENGAL" in up and bay_summary is None:
            snippet = " ".join(lines[i + 1 : i + 4])
            bay_summary = snippet
        if "ARABIAN SEA" in up and arab_summary is None:
            snippet = " ".join(lines[i + 1 : i + 4])
            arab_summary = snippet
        if bay_summary and arab_summary:
            break
    if not bay_summary:
        m = re.search(
            r"BAY OF BENGAL[:\s]*(.+?)(?:ARABIAN|PROBABILITY|REMARKS|$)",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if m:
            bay_summary = " ".join(m.group(1).strip().splitlines())[:800]
    if not arab_summary:
        m = re.search(
            r"ARABIAN SEA[:\s]*(.+?)(?:BAY|PROBABILITY|REMARKS|$)",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if m:
            arab_summary = " ".join(m.group(1).strip().splitlines())[:800]
    if not bay_summary:
        bay_summary = "N/A"
    if not arab_summary:
        arab_summary = "N/A"
    return bay_summary, arab_summary


def normalize_prob_token(tok):
    if not tok:
        return None
    t = tok.strip().upper()
    if t in ("NIL", "NONE", "0%", "0"):
        return "NIL"
    if t in ("LOW", "L0W"):
        return "LOW"
    if t in ("MID", "MODERATE", "MOD"):
        return "MID"
    if t in ("HIGH", "HGH"):
        return "HIGH"
    for key in ("NIL", "LOW", "MID", "MODERATE", "HIGH"):
        if key in t:
            if key == "MODERATE":
                return "MID"
            if key == "MID":
                return "MID"
            return key
    return None


def _split_table_line_to_cells(line):
    """
    Attempt to split a table line into columns.
    Prefer splitting on 2+ spaces (common when PDF extracts table)
    Also split on '|' if present.
    """
    if "|" in line:
        parts = [p.strip() for p in line.split("|") if p.strip()]
        if len(parts) > 1:
            return parts
    parts = [p.strip() for p in re.split(r"\s{2,}", line) if p.strip()]
    if len(parts) > 1:
        return parts
    return [p.strip() for p in line.split() if p.strip()]


def parse_probabilities_from_text(text, logger):
    """
    Table-oriented parser. Return bay_tokens (7) and arab_tokens (7) normalized to NIL/LOW/MID/HIGH.
    Falls back to global token extraction if table header not found.
    """
    up = text.upper()

    # Look for the big header line containing the 7 windows
    header_patterns = [
        r"24\s+HOURS\s+24-48\s+HOURS\s+48-72\s+HOURS\s+72-96\s+HOURS\s+96-120\s+HOURS\s+120-144\s+HOURS\s+144-168\s+HOURS",
        r"24\s+HOURS\s+24-48\s+HOURS\s+48-72\s+HOURS\s+72-96\s+HOURS",  # fallback
    ]
    header_match = None
    for pat in header_patterns:
        m = re.search(pat, up)
        if m:
            header_match = m
            break

    def pad7(lst):
        out = [normalize_prob_token(t) or "NIL" for t in lst]
        out = [("MID" if x == "MODERATE" else x) for x in out]
        while len(out) < 7:
            out.append("NIL")
        return out[:7]

    # If header not found, fallback to global extraction but log block
    if not header_match:
        logger.warning(
            "Probability table header not found. Falling back to global token search."
        )
        global_matches = re.findall(
            r"\b(?:NIL|LOW|MID|MODERATE|MOD|HIGH|[0-9]{1,3}%?)\b", up
        )
        tokens = []
        for m in global_matches:
            p = normalize_prob_token(m)
            if p:
                tokens.append(p)
            if len(tokens) >= 14:
                break
        while len(tokens) < 14:
            tokens.append("NIL")
        return tokens[:7], tokens[7:14]

    # We have header; parse next chunk
    idx = header_match.end()
    chunk = up[idx : idx + 1200]  # take a reasonable window after header
    logger.info(
        "Parsing probability chunk (first 400 chars): %s",
        chunk[:400].replace("\n", " "),
    )

    lines = [ln.strip() for ln in chunk.splitlines() if ln.strip()]
    # Try to find the row(s) that contain 7 cells
    bay_tokens = []
    for take in range(1, 4):
        cand = " ".join(lines[:take])
        cells = _split_table_line_to_cells(cand)
        if len(cells) >= 7:
            bay_tokens = [cells[i] for i in range(7)]
            break
    if not bay_tokens:
        for ln in lines[:10]:
            cells = _split_table_line_to_cells(ln)
            if len(cells) >= 7:
                bay_tokens = [cells[i] for i in range(7)]
                break

    # Find Arabian section; parse similarly
    arab_tokens = []
    arab_idx = up.find("ARABIAN SEA")
    if arab_idx != -1:
        right_after_arab = up[arab_idx : arab_idx + 1200]
        logger.info(
            "Arab chunk (first 300 chars): %s",
            right_after_arab[:300].replace("\n", " "),
        )
        lines_arab = [ln.strip() for ln in right_after_arab.splitlines() if ln.strip()]
        for take in range(1, 4):
            cand = " ".join(lines_arab[1 : 1 + take]) if len(lines_arab) > 1 else ""
            cells = _split_table_line_to_cells(cand)
            if len(cells) >= 7:
                arab_tokens = [cells[i] for i in range(7)]
                break
        if not arab_tokens:
            for ln in lines_arab[:10]:
                cells = _split_table_line_to_cells(ln)
                if len(cells) >= 7:
                    arab_tokens = [cells[i] for i in range(7)]
                    break

    bay_norm = pad7(bay_tokens)
    arab_norm = pad7(arab_tokens)

    logger.info("Table-method parsed Bay tokens: %s", bay_norm)
    logger.info("Table-method parsed Arab tokens: %s", arab_norm)

    # debug probe
    combined = bay_norm + arab_norm
    if all(x == "NIL" for x in combined):
        others = re.findall(r"\b(?:LOW|MID|HIGH|MODERATE)\b", up)
        if others:
            logger.info("Global probe found non-NIL words elsewhere: %s", others[:20])

    return bay_norm, arab_norm
