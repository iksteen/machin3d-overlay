# Agent Instructions

## Refactor Architecture Gate

When a user asks for a refactor, do not treat moving files or flattening directories as the goal. The goal is a clearer ownership model.

Before editing:

1. Identify the responsibility boundary first: what concept owns the code, what code is only an implementation detail, and what code is coupled to a single caller.
2. Keep tightly coupled request/response mapping, formatting, and small helper logic with the endpoint or service that owns it unless it has a real independent domain role.
3. Do not create or preserve generic modules named only for shape, such as `payload`, `runtime`, `helper`, or `utils`, unless the module owns a clearly named concept and has more than one credible consumer.
4. Prefer names that describe domain responsibility over infrastructure shape. If a name would still make sense after moving the file, it is probably too vague.

Before finalizing:

1. Do a naming and ownership pass, not only a compile/test pass.
2. Check whether every new type, module, function, and public item is necessary. Remove temporary scaffolding that no longer carries its weight.
3. Review the diff for semantic clarity: the result should make the code easier to understand without already knowing the old layout.
4. Treat passing tests, clippy, and review output as necessary but insufficient. The agent owns judging whether the abstraction is justified.
5. Mention any intentionally retained awkwardness or boundary tradeoff in the final answer.

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
