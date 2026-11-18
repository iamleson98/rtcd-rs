// The picture score of the lesson receiver page (see page.html): how close a frame the browser
// received is to the frame it was made from, in luma, as PSNR (dB) and SSIM (0 to 1). Plain
// functions on arrays of luma, so that `scripts/lesson-soak/chrome/quality.test.mjs` can run them
// in Node.
//
// A received frame is matched to its source by looking, not by counting: the sender starts its clip
// over on every keyframe request while its RTP timestamps run on, and a layer switch changes the
// frame rate, so neither the timestamp nor the arrival time says which frame of the clip a frame is.
// The frames of the clip (decoded by the page) are searched on small thumbnails, and the best few
// candidates are compared at full size; the score is the best of them, which is the usual way to
// score without a frame-accurate clock. It cannot tell a frame from its neighbour when the
// picture is destroyed, so under heavy damage it reads a little better than it looks.

// The size both pictures are compared at, and the thumbnail that finds the source frame.
const W = 320;
const H = 180;
const TW = 32;
const TH = 18;
// Candidates that are compared at full size after the thumbnail search.
const CANDIDATES = 3;
// A frame that is identical in luma would be infinite; the score stops here.
const PSNR_CAP = 60;

/** Luma (BT.601) of an RGBA image into `out` (W * H). */
function luma(rgba, out) {
	for (let i = 0, p = 0; p < out.length; p++, i += 4) {
		out[p] = (77 * rgba[i] + 150 * rgba[i + 1] + 29 * rgba[i + 2]) >> 8;
	}
	return out;
}

/** The thumbnail of a W x H luma picture: the mean of each (W / TW) x (H / TH) block. */
function thumb(y) {
	const bw = W / TW;
	const bh = H / TH;
	const out = new Float32Array(TW * TH);
	for (let ty = 0; ty < TH; ty++) {
		for (let tx = 0; tx < TW; tx++) {
			let sum = 0;
			for (let j = 0; j < bh; j++) {
				const row = (ty * bh + j) * W + tx * bw;
				for (let i = 0; i < bw; i++) sum += y[row + i];
			}
			out[ty * TW + tx] = sum / (bw * bh);
		}
	}
	return out;
}

function psnr(a, b) {
	let sum = 0;
	for (let i = 0; i < a.length; i++) {
		const d = a[i] - b[i];
		sum += d * d;
	}
	const mse = sum / a.length;
	return mse === 0 ? PSNR_CAP : Math.min(PSNR_CAP, 10 * Math.log10((255 * 255) / mse));
}

/** The mean SSIM of 8 x 8 windows every 4 pixels (uniform weights, K1 0.01, K2 0.03). */
function ssim(a, b) {
	const c1 = (0.01 * 255) ** 2;
	const c2 = (0.03 * 255) ** 2;
	let total = 0;
	let n = 0;
	for (let y0 = 0; y0 + 8 <= H; y0 += 4) {
		for (let x0 = 0; x0 + 8 <= W; x0 += 4) {
			let sa = 0, sb = 0, saa = 0, sbb = 0, sab = 0;
			for (let j = 0; j < 8; j++) {
				const row = (y0 + j) * W + x0;
				for (let i = 0; i < 8; i++) {
					const p = a[row + i];
					const q = b[row + i];
					sa += p;
					sb += q;
					saa += p * p;
					sbb += q * q;
					sab += p * q;
				}
			}
			const ma = sa / 64, mb = sb / 64;
			const va = saa / 64 - ma * ma, vb = sbb / 64 - mb * mb, cov = sab / 64 - ma * mb;
			total += ((2 * ma * mb + c1) * (2 * cov + c2)) / ((ma * ma + mb * mb + c1) * (va + vb + c2));
			n++;
		}
	}
	return total / n;
}

/** The thumbnails of the frames of a clip (each a W x H luma picture), for `score`. */
function prepare(frames) {
	return { frames, thumbs: frames.map(thumb) };
}

/** The indexes of the `count` thumbnails nearest to `t` (sum of absolute differences). */
function nearest(t, thumbs, count) {
	const best = [];
	for (let k = 0; k < thumbs.length; k++) {
		const ref = thumbs[k];
		let sad = 0;
		for (let i = 0; i < t.length; i++) sad += Math.abs(t[i] - ref[i]);
		if (best.length < count || sad < best[best.length - 1].sad) {
			best.push({ k, sad });
			best.sort((x, y) => x.sad - y.sad);
			if (best.length > count) best.pop();
		}
	}
	return best.map((e) => e.k);
}

/** Scores a W x H luma picture against the clip: its best matching frame, PSNR and SSIM. */
function score(y, clip) {
	let at = -1;
	let best = -1;
	for (const k of nearest(thumb(y), clip.thumbs, CANDIDATES)) {
		const p = psnr(y, clip.frames[k]);
		if (p > best) {
			best = p;
			at = k;
		}
	}
	return { frame: at, psnr: best, ssim: ssim(y, clip.frames[at]) };
}

const quality = { W, H, luma, thumb, psnr, ssim, prepare, nearest, score };
if (typeof module !== 'undefined') module.exports = quality;
