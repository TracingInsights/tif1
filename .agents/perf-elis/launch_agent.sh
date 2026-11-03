#!/usr/bin/env bash
# Launch one Elis hypothesis agent headless on another Claude account.
# usage: .agents/perf-elis/launch_agent.sh <ID> <account>   e.g. E1 ceo, E2 madhu, E3 info, E4 hi
# MODEL / EFFORT env vars override the defaults (claude-opus-5-5, high).
# The agent gets a scoped tool allowlist (edits inside its worktree, the repo's
# interpreter/ruff/ty, git and read-only shell commands); no permission bypass.
# E4 (native build) additionally gets maturin, uv venv/pip and its private venv.
set -u
ID="$1"; ACCT="$2"
MODEL="${MODEL:-claude-opus-5-5}"; EFFORT="${EFFORT:-high}"
WT="/tmp/tif1-elis/$ID"
LOG="/home/harish/Documents/tif1/.agents/perf-elis/logs/$ID.log"
DOCS="/home/harish/Documents/tif1/.agents/perf-elis"
PROMPT="Implement and measure hypothesis $ID of the Elis performance round of the tif1 library. Your git worktree is $WT (branch perf/elis-$ID); your private warm cache copy is /tmp/tif1-elis-warm-$ID. Read these files first: $DOCS/BRIEF.md (the shared protocol: environment, commands, measurement and parity rules, deliverable format), the $ID section of $DOCS/HYPOTHESES.md, $DOCS/DO_NOT_RETRY.md, and $WT/AGENTS.md. If $DOCS/${ID}_HANDOFF.md exists, read it too: it describes work already in the worktree and what remains. Edit only files inside $WT (and, for E4, the private venv the brief names). Measure with the interleaved A/B harness and the parity digest described in the brief, keep ruff, ty and the unit suite green, commit on the branch (or stage everything if commits are denied), and write the report $WT/.agents/perf-elis/$ID.md. End with a short summary of the verdict and the key numbers (control vs candidate medians, pairs won, parity)."
ALLOWED="Read,Edit,Write,Glob,Grep,Bash(/home/harish/Documents/tif1/.venv/bin/python *),Bash(/home/harish/Documents/tif1/.venv/bin/ruff *),Bash(/home/harish/Documents/tif1/.venv/bin/ty *),Bash(git *),Bash(cd *),Bash(ls *),Bash(cat *),Bash(head *),Bash(tail *),Bash(grep *),Bash(wc *),Bash(diff *),Bash(mkdir *),Bash(cp *),Bash(sed -n *),Bash(export *),Bash(PYTHONPATH=*),Bash(TIF1_CACHE_DIR=*),Bash(TIF1_OFFLINE_MODE=*),Bash(TIF1_DISABLE_NATIVE=*),Bash(PY=*),Bash(perf stat *)"
if [ "$ID" = "E4" ]; then
  ALLOWED="$ALLOWED,Bash(uvx maturin *),Bash(uv venv *),Bash(uv pip *),Bash(cargo *),Bash(/tmp/tif1-elis-venv-E4/bin/python *)"
fi
cd "$WT" || exit 1
CLAUDE_CONFIG_DIR="$HOME/.claude-$ACCT" nohup claude -p "$PROMPT" --model "$MODEL" --effort "$EFFORT" --name "elis-$ID" --permission-mode acceptEdits --allowedTools "$ALLOWED" --output-format json > "$LOG" 2>&1 &
echo "launched $ID on $ACCT with $MODEL/$EFFORT (pid $!), log: $LOG"
