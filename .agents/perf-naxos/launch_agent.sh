#!/usr/bin/env bash
# Launch one Naxos hypothesis agent headless on one Claude account.
# usage: .agents/perf-naxos/launch_agent.sh <ID> <account>   e.g. N1 hi, N3 madhu, N2 contact
# MODEL / EFFORT env vars override the defaults (claude-opus-5-5, high).
# The agent gets a scoped tool allowlist (edits inside its tree, the repo's
# interpreter/ruff/ty, git, profiling and read-only shell commands, web
# research); no permission bypass. N7 additionally gets cargo, maturin and a
# private venv.
set -u
ID="$1"; ACCT="$2"
MODEL="${MODEL:-claude-opus-5-5}"; EFFORT="${EFFORT:-high}"
TREE="/tmp/naxos/$ID"
DOCS="/home/harish/Documents/tif1/.agents/perf-naxos"
LOG="$DOCS/logs/$ID.log"
PROMPT="Implement and measure hypothesis $ID of the Naxos performance round of the tif1 library. Your tree is $TREE (a sparse git clone, branch perf/naxos-$ID); your warm cache copy is /tmp/naxos/warm-$ID; your HOME for runs is /tmp/naxos/home-$ID. Read these files first: $DOCS/BRIEF.md (the shared protocol: environment, commands, measurement and parity rules, deliverable format), the $ID section of $DOCS/HYPOTHESES.md, $DOCS/DO_NOT_RETRY.md, and $TREE/AGENTS.md. If $DOCS/${ID}_HANDOFF.md exists, read it too: it describes work already in the tree and what remains. Edit only files inside $TREE (and, for N3/N7, the private venv the hypothesis names). Write a progress line to $TREE/.agents/perf-naxos/${ID}_PROGRESS.md at each milestone. Measure with the interleaved A/B harness and the parity digest described in the brief, keep ruff, ty and the unit suite green, commit on the branch (or stage everything if commits are denied), and write the report $TREE/.agents/perf-naxos/$ID.md. End with a short summary of the verdict and the key numbers (control vs candidate medians, pairs won, parity)."
PYV=/home/harish/Documents/tif1/.venv/bin
ALLOWED="Read,Edit,Write,Glob,Grep,WebSearch,WebFetch,Bash($PYV/python *),Bash($PYV/ruff *),Bash($PYV/ty *),Bash($PYV/py-spy *),Bash(git *),Bash(cd *),Bash(ls *),Bash(cat *),Bash(head *),Bash(tail *),Bash(grep *),Bash(wc *),Bash(diff *),Bash(mkdir *),Bash(cp *),Bash(sed -n *),Bash(sort *),Bash(awk *),Bash(du *),Bash(export *),Bash(env *),Bash(PYTHONPATH=*),Bash(HOME=*),Bash(TIF1_*),Bash(OPENBLAS_*),Bash(PY=*),Bash(perf *),Bash(strace *),Bash(ltrace *),Bash(taskset *),Bash(/usr/bin/time *),Bash(curl *),Bash(nproc),Bash(rm /tmp/naxos/*),Bash(rm -rf /tmp/naxos/scratch-$ID*)"
if [ "$ID" = "N3" ] || [ "$ID" = "N7" ]; then
  ALLOWED="$ALLOWED,Bash(uvx maturin *),Bash(uv venv *),Bash(uv pip *),Bash(cargo *),Bash(/tmp/naxos/venv-$ID/bin/python *),Bash(CARGO_TARGET_DIR=*)"
fi
cd "$TREE" || exit 1
CLAUDE_CONFIG_DIR="$HOME/.claude-$ACCT" nohup claude -p "$PROMPT" --model "$MODEL" --effort "$EFFORT" --name "naxos-$ID" --permission-mode acceptEdits --allowedTools "$ALLOWED" --output-format json > "$LOG" 2>&1 &
echo "launched $ID on $ACCT with $MODEL/$EFFORT (pid $!), log: $LOG"
