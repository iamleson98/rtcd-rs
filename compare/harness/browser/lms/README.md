The LMS web app's call client, copied unchanged from `lms-web/src/lib/features/calls/`
(`call-client.ts`, `media.ts`, `quality.ts`, `sfu-channel.ts`). The browser test bundles these with
`../harness.ts` and runs them in headless Chrome against each server, so what is tested is the
code the LMS ships, not a re-implementation. Update them by copying the four files again.
