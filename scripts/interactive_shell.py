#!/usr/bin/env python3
"""
Interactive CLI Explorer for The Real-Time Event Streaming & Ingestion Handbook
"""

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
ARCH_DIR = BASE_DIR / "architectures"
RUNBOOKS_DIR = BASE_DIR / "runbooks"
INTERVIEW_DIR = BASE_DIR / "interview"

def list_all():
    print("\n📚 The Real-Time Event Streaming & Ingestion Handbook: Architecture & Incident Catalog\n" + "=" * 80)
    for p, title in [(ARCH_DIR, "Deep-Dive Architectures"), (RUNBOOKS_DIR, "Production Incident Runbooks"), (INTERVIEW_DIR, "Interview Guides")]:
        print(f"\n📂 {title}:")
        for f in sorted(list(p.glob("*.md"))):
            print(f"   ⚡ {f.name}")
    print("\n" + "=" * 80)

def list_runbooks():
    print("\n🚨 Production Incident Runbooks:\n" + "=" * 80)
    for rb in sorted(list(RUNBOOKS_DIR.glob("*.md"))):
        print(f"   🚨 {rb.name}")
    print("\n" + "=" * 80)

def list_architectures():
    print("\n🏛️ Deep-Dive Architectures:\n" + "=" * 80)
    for arch in sorted(list(ARCH_DIR.glob("*.md"))):
        print(f"   🏛️ {arch.name}")
    print("\n" + "=" * 80)

def list_interviews():
    print("\n🎓 4-Tier Interview Preparation Matrix:\n" + "=" * 80)
    for iv in sorted(list(INTERVIEW_DIR.glob("*.md"))):
        print(f"   🎓 {iv.name}")
    print("\n" + "=" * 80)

def list_ecosystem():
    print("""
🌐 Modern Data Engineering Ecosystem Cross-Navigation:
================================================================================
Central Platform: https://moderndataengineer.uk (11,950+ verified assets)
Sister Repositories:
• Databricks Handbook:     https://github.com/santoshjammi29/databricks-handbook
• PySpark Handbook:        https://github.com/santoshjammi29/pyspark-handbook
• SQL Handbook:            https://github.com/santoshjammi29/sql-handbook
• Azure Enterprise:        https://github.com/santoshjammi29/azure-cheat-sheet
• dbt Handbook:            https://github.com/santoshjammi29/dbt-handbook
• Streaming Handbook:      https://github.com/santoshjammi29/streaming-handbook
• Data System Design:      https://github.com/santoshjammi29/data-system-design-handbook
• DataOps & Platform:      https://github.com/santoshjammi29/dataops-cicd-handbook
================================================================================
""")

def find_file(query: str):
    clean_q = query.lower().replace(" ", "_").replace("-", "_")
    for search_dir in [ARCH_DIR, RUNBOOKS_DIR, INTERVIEW_DIR]:
        for doc in search_dir.glob("*.md"):
            if clean_q in doc.name.lower() or query.lower() in doc.read_text(encoding="utf-8").lower():
                return doc
    return None

def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print("Usage: python scripts/interactive_shell.py <query>")
        print("       python scripts/interactive_shell.py --list")
        print("       python scripts/interactive_shell.py --runbooks")
        print("       python scripts/interactive_shell.py --architectures")
        print("       python scripts/interactive_shell.py --interview")
        print("       python scripts/interactive_shell.py --ecosystem")
        sys.exit(0)

    arg = sys.argv[1]
    if arg == "--list":
        list_all()
        sys.exit(0)
    elif arg == "--runbooks":
        list_runbooks()
        sys.exit(0)
    elif arg == "--architectures":
        list_architectures()
        sys.exit(0)
    elif arg == "--interview":
        list_interviews()
        sys.exit(0)
    elif arg == "--ecosystem":
        list_ecosystem()
        sys.exit(0)

    match = find_file(arg)
    if not match:
        print(f"❌ No matching document found for: '{arg}'")
        sys.exit(1)

    print(f"\n📖 Found: {match.name}\n" + "=" * 80)
    print(match.read_text(encoding="utf-8"))
    print("=" * 80)

if __name__ == "__main__":
    main()
