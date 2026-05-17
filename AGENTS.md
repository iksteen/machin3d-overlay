# Agent Instructions

## Refactor Cleanup Gate

When a user asks to remove, simplify, or stop modeling a concept, the change is not done until the concept has been traced through the codebase.

Before finalizing:

1. Search for the removed concept and adjacent names with `rg`.
2. Classify every remaining hit as one of:
   - required runtime/API contract
   - raw fixture input that is required by a test
   - test-only helper
   - dead or unnecessary surface
3. Remove dead or unnecessary surface before running final verification. Raw fixture input is not automatically justified; keep it only when the test specifically needs that ignored or unknown input.
4. In the final answer, explicitly mention any intentional leftovers and why they remain.
5. Do not treat review output as proof that the patch is clean. Reviews can miss unnecessary additions; the agent owns this cleanup check.

When the user states a domain invariant, implement around it directly unless local code or primary documentation gives concrete evidence against it.
