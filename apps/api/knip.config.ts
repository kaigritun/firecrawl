import type { KnipConfig } from "knip";

const config: KnipConfig = {
  workspaces: {
    ".": {
      entry: ["src/services/worker/**/*.ts", "src/services/**/*-worker.ts"],
      project: ["src/**/*.ts"],
    },
  },
  ignore: [
    "native/**",
    "src/services/search-index-db.ts",
    "src/lib/search-index-client.ts",
  ],
  ignoreDependencies: ["openai", "@pinecone-database/pinecone"],
};

export default config;
