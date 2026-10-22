"""Usage:
    python -m generator.cli generate specs/card_transactions.yaml
    python -m generator.cli generate specs/card_transactions.yaml --check   # for CI
"""
import argparse
import sys
from pathlib import Path

from generator.render import ROOT, render_all
from spec_schema.pipeline_spec import load_spec


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="sdp")
    ap.add_argument("command", choices=["generate"])
    ap.add_argument("spec")
    ap.add_argument("--check", action="store_true",
                    help="write nothing; exit 1 if generated files are missing or stale")
    args = ap.parse_args(argv)

    spec_path = Path(args.spec).resolve()
    spec = load_spec(spec_path)                       # fails here if the spec is invalid
    files = render_all(spec, spec_path)
    out_dir = ROOT / "generated" / spec.pipeline

    stale = []
    for name, content in files.items():
        target = out_dir / name
        if args.check:
            if not target.exists() or target.read_text() != content:
                stale.append(str(target.relative_to(ROOT)))
        else:
            out_dir.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
            print(f"wrote {target.relative_to(ROOT)}")

    if stale:
        print("STALE (regenerate and commit):", *stale, sep="\n  ")
        return 1
    if args.check:
        print("generated files are up to date")
    return 0


if __name__ == "__main__":
    sys.exit(main())
