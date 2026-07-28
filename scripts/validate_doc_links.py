"""
scripts/validate_doc_links.py
A small, genuinely useful CI check: parses all markdown files in docs/
and README.md for relative links, confirms every linked file exists.
This is a real safeguard against exactly the kind of documentation rot
this whole project has been designed to avoid.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LINK_PATTERN = re.compile(r"\[.*?\]\((?!http)([^)]+\.md)\)")


def main():
    broken = []
    for md_file in list(ROOT.glob("*.md")) + list((ROOT / "docs").glob("*.md")):
        content = md_file.read_text()
        for match in LINK_PATTERN.finditer(content):
            linked_path = (md_file.parent / match.group(1)).resolve()
            if not linked_path.exists():
                broken.append(f"{md_file}: broken link to {match.group(1)}")

    if broken:
        print("Broken documentation links found:")
        for b in broken:
            print(f"  - {b}")
        sys.exit(1)
    print("All documentation links resolve correctly.")


if __name__ == "__main__":
    main()