# Shunt — getting started on the dev box

1. Extract into a fresh repo:

       mkdir shunt && cd shunt && git init
       tar xzf ~/shunt-docs.tar.gz --strip-components=1
       git add -A && git commit -m "docs: design, POC track, reuse audit, context"

2. Fill in docs/CONTEXT.md. For POC-0 only four rows matter: Go version, container
   runtime, the wildcard domain (the cert script needs it), and the VAST lab endpoint
   with scratch bucket and env var names. Write `unknown` in any row you can't fill yet.
   Commit again.

3. Session 1 — from the repo root, start Claude Code and paste docs/prompts/POC-0.md.
   It stops after listing files and dependencies; paste that list back for review
   before saying go.

4. After it finishes, paste back: `make all` output, `find . -type f | grep -v .git | sort`,
   one invalid-sample `check-config` run, and the contents of CLAUDE.md and go.mod.

5. When clean: `git tag poc-0`. Every later session opens with the Kickoff prompt
   (docs/DESIGN.md §7), then R0 (docs/reuse.md), then POC-1 (docs/POC.md).

Files:
  docs/DESIGN.md              full design + phase prompts (P0–P8, G1–G4)
  docs/POC.md                 five-session POC track (POC-0 … POC-4)
  docs/reuse.md               what to lift, licenses, R0 audit prompt
  docs/CONTEXT.md             template — fill before POC-0
  docs/validation-report.md   every assumption checked
  docs/prompts/POC-0.md       the first prompt, ready to paste
