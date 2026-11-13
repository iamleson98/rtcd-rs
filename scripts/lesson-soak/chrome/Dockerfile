# Headless Chromium for the lesson soak: Playwright's image (multi-arch: linux/amd64 and
# linux/arm64, pinned by tag and index digest) with the matching npm package.
FROM mcr.microsoft.com/playwright:v1.65.0-noble@sha256:9beb546099e11f52d62e505537914870e01a67e0e6b00ae41da4775324e73dbe
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY run.mjs ./
ENTRYPOINT ["node", "run.mjs"]
