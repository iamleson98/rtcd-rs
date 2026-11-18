// Tests of the picture score (crates/lms-sim/src/quality.js): node --test scripts/lesson-soak/chrome
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import test from 'node:test';

const quality = createRequire(import.meta.url)('../../../crates/lms-sim/src/quality.js');
const { W, H } = quality;

// A picture with a gradient that scrolls with `n` and a bar that moves: frames differ the way
// the lesson's test pattern's do.
function frame(n) {
	const y = new Uint8Array(W * H);
	for (let j = 0; j < H; j++) {
		for (let i = 0; i < W; i++) {
			const bar = Math.abs(i - ((n * 7) % W)) < 12 ? 90 : 0;
			y[j * W + i] = Math.min(255, ((i + n * 3 + (j >> 2)) % 160) + bar);
		}
	}
	return y;
}

// Deterministic noise of +-`amp`.
function noisy(y, amp) {
	let s = 12345;
	return y.map((v) => {
		s = (s * 1103515245 + 12345) & 0x7fffffff;
		return Math.max(0, Math.min(255, v + ((s >> 8) % (2 * amp + 1)) - amp));
	});
}

const clip = quality.prepare(Array.from({ length: 60 }, (_, n) => frame(n)));

test('an identical picture scores the cap and 1', () => {
	const s = quality.score(frame(17), clip);
	assert.equal(s.frame, 17);
	assert.equal(s.psnr, 60);
	assert.ok(Math.abs(s.ssim - 1) < 1e-9);
});

test('PSNR follows the damage', () => {
	const light = quality.score(noisy(frame(30), 3), clip);
	const heavy = quality.score(noisy(frame(30), 30), clip);
	assert.equal(light.frame, 30);
	assert.equal(heavy.frame, 30);
	assert.ok(light.psnr > heavy.psnr + 10, `${light.psnr} vs ${heavy.psnr}`);
	assert.ok(light.ssim > heavy.ssim, `${light.ssim} vs ${heavy.ssim}`);
	// Noise of +-3 is about 42 dB; +-30 about 23 dB.
	assert.ok(light.psnr > 40 && light.psnr < 44, `${light.psnr}`);
	assert.ok(heavy.psnr > 21 && heavy.psnr < 25, `${heavy.psnr}`);
});

test('the source frame is found among the others, damaged', () => {
	for (const n of [0, 1, 29, 44, 59]) assert.equal(quality.score(noisy(frame(n), 10), clip).frame, n);
});

test('a picture of nothing like the clip scores low', () => {
	const s = quality.score(new Uint8Array(W * H).fill(128), clip);
	assert.ok(s.psnr < 15 && s.ssim < 0.8, `${s.psnr} ${s.ssim}`);
});

test('luma is BT.601', () => {
	const out = quality.luma(new Uint8ClampedArray([255, 0, 0, 255, 0, 255, 0, 255, 255, 255, 255, 255, 0, 0, 0, 255]), new Uint8Array(4));
	assert.deepEqual([...out], [76, 149, 255, 0]);
});
