// Branding script for extracting brand design tokens from web pages
// Built at runtime using esbuild
import path from "path";
import fs from "fs";

let cachedScript: string | null = null;

export const getBrandingScript = (): string => {
  if (cachedScript) {
    return cachedScript;
  }

  // Determine the correct path to the branding script source files
  // When running from dist/, __dirname is dist/src/.../fire-engine
  // When running from src/ (dev), __dirname is src/.../fire-engine
  let entryPoint = path.join(__dirname, "branding-script", "index.ts");

  // If the TypeScript file doesn't exist (running from dist), resolve to src/
  if (!fs.existsSync(entryPoint)) {
    // Navigate from dist/src/... to src/...
    const projectRoot = path.resolve(
      __dirname,
      "..",
      "..",
      "..",
      "..",
      "..",
      "..",
    );
    entryPoint = path.join(
      projectRoot,
      "src",
      "scraper",
      "scrapeURL",
      "engines",
      "fire-engine",
      "branding-script",
      "index.ts",
    );
  }

  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const esbuild = require("esbuild");

  const result = esbuild.buildSync({
    entryPoints: [entryPoint],
    bundle: true,
    minify: true,
    format: "iife",
    globalName: "__extractBrandDesign",
    target: ["es2020"],
    write: false,
  });

  const bundledCode = result.outputFiles[0].text;

  // Wrap in a self-executing function that returns the result
  cachedScript = `(function __extractBrandDesign() {
${bundledCode}
return __extractBrandDesign.extractBrandDesign();
})();`;

  return cachedScript;
};
