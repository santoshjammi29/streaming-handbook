#!/usr/bin/env python3
"""
Interactive CLI Explorer for The Real-Time Event Streaming & Ingestion Handbook
"""

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
MODULES_DIR = BASE_DIR / "modules"
ARCH_DIR = BASE_DIR / "architectures"
RUNBOOKS_DIR = BASE_DIR / "runbooks"
INTERVIEW_DIR = BASE_DIR / "interview"

def list_all():
    print("\n📚 The Real-Time Event Streaming & Ingestion Handbook: Complete Catalog\n" + "=" * 80)
    if MODULES_DIR.exists():
        print("\n📦 Production Curriculum Modules:")
        for m in sorted([d for d in MODULES_DIR.iterdir() if d.is_dir()]):
            print(f"   📦 {m.name}")
    for p, title in [(ARCH_DIR, "Deep-Dive Architectures"), (RUNBOOKS_DIR, "Production Incident Runbooks"), (INTERVIEW_DIR, "Interview Guides")]:
        print(f"\n📂 {title}:")
        for f in sorted(list(p.glob("*.md"))):
            print(f"   ⚡ {f.name}")
    print("\n" + "=" * 80)

def list_modules():
    print("\n📦 Production Curriculum Modules:\n" + "=" * 80)
    if MODULES_DIR.exists():
        for m in sorted([d for d in MODULES_DIR.iterdir() if d.is_dir()]):
            readme = m / "README.md"
            title = ""
            if readme.exists():
                lines = readme.read_text(encoding="utf-8").splitlines()
                title = lines[0].replace("#", "").strip() if lines else ""
            print(f"   📦 {m.name:45} | {title}")
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

def show_doc(path: Path):
    if not path.exists():
        print(f"Error: {path} not found.")
        return
    print("\n" + "=" * 80)
    print(f"📄 Displaying: {path.name}")
    print("=" * 80 + "\n")
    print(path.read_text(encoding="utf-8"))
    print("\n" + "=" * 80)

def main():
    if len(sys.argv) < 2 or "--help" in sys.argv or "-h" in sys.argv:
        print("""
Usage: python scripts/interactive_shell.py [COMMAND]

Commands:
  --list             List all modules, architectures, runbooks, and interview tiers
  --modules          List all 10 production curriculum modules
  --architectures    List deep-dive architectural specifications
  --runbooks         List production incident triage runbooks
  --interview        List 4-tier progressive interview guides and flashcard deck
  --ecosystem        Show cross-repository deep links and platform URLs
  --view <file>      Display a specific markdown document in terminal
""")
        sys.exit(0)

    arg = sys.argv[1]
    if arg == "--list":
        list_all()
    elif arg == "--modules":
        list_modules()
    elif arg == "--architectures":
        list_architectures()
    elif arg == "--runbooks":
        list_runbooks()
    elif arg == "--interview":
        list_interviews()
    elif arg == "--ecosystem":
        list_ecosystem()
    elif arg == "--view" and len(sys.argv) > 2:
        target = Path(sys.argv[2])
        if not target.is_absolute():
            target = BASE_DIR / target
        show_doc(target)
    else:
        print(f"Unknown argument: {arg}. Use --help for options.")

if __name__ == "__main__":
    main()
