// Bundles harness.ts with the LMS web app's call client from ./lms (bun build.ts <out.js>).
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';

const [out] = process.argv.slice(2);
if (!out) throw new Error('usage: bun build.ts <out.js>');
const here = import.meta.dir;
const client = resolve(here, 'lms/call-client.ts');
const entry = resolve(here, '.entry.ts');
writeFileSync(entry, (await Bun.file(resolve(here, 'harness.ts')).text()).replace('LMS_CALL_CLIENT', client));

const result = await Bun.build({
	entrypoints: [entry],
	target: 'browser',
	plugins: [
		{
			name: 'realtime-stub',
			setup(build) {
				build.onResolve({ filter: /^\$lib\/realtime\/socket\.svelte$/ }, () => ({
					path: resolve(here, 'realtime-stub.ts'),
				}));
			},
		},
	],
});
if (!result.success) {
	console.error(result.logs.join('\n'));
	process.exit(1);
}
mkdirSync(dirname(resolve(out)), { recursive: true });
writeFileSync(out, await result.outputs[0].text());
