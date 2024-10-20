import re

__all__ = ["extract_last_braket_string", "sanitize_category"]

def extract_last_braket_string(text: str) -> str | None:
    """extract units from indicator label sting"""
    match = re.search(r'\(([^)]*)\)$', text)
    
    if match:
        return match.group(1).strip()
    
    return None


def sanitize_category(s):
    """sanitize column names"""
    s = s.split(":")[0]
    s = s.lower()
    s = re.sub(r"[()\[\]]", "", s)
    s = re.sub(r"[\s\W]+", "_", s)
    s = s.strip("_")
    return s