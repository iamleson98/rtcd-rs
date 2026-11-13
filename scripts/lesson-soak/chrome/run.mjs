// Opens the lesson receiver page (served by `lms-sim lesson --chrome`) in headless Chromium and
// keeps it open until the container is stopped:  node run.mjs 'http://127.0.0.1:8099/?group=clean&ws=8100'
import { chromium } from 'playwright';

const url = process.argv[2];
if (!url) {
	console.error('usage: node run.mjs URL');
	process.exit(2);
}

const browser = await chromium.launch({
	// Playwright's own Chromium (there is no Google Chrome for linux/arm64); `chromium` is the
	// full browser in its new headless mode, which decodes and plays out like a desktop one.
	channel: 'chromium',
	headless: true,
	args: [
		// Host candidates carry the address, not an mDNS name: the SFU cannot resolve those.
		'--disable-features=WebRtcHideLocalIpsWithMdns',
		'--autoplay-policy=no-user-gesture-required',
		'--mute-audio',
		'--disable-background-timer-throttling',
		'--disable-renderer-backgrounding',
		'--disable-backgrounding-occluded-windows'
	]
});
const page = await browser.newPage();
page.on('console', (m) => console.log(`[page ${m.type()}] ${m.text()}`));
page.on('pageerror', (e) => console.log(`[page error] ${e.message}`));
await page.goto(url);
console.log(`chromium ${browser.version()} on ${url}`);

const stop = async () => {
	await browser.close().catch(() => {});
	process.exit(0);
};
process.on('SIGTERM', stop);
process.on('SIGINT', stop);
browser.on('disconnected', () => process.exit(1));
await new Promise(() => {});
