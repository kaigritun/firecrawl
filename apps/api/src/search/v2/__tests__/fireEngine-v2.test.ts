import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  jest,
} from "@jest/globals";

const FIRE_ENGINE_URL = "https://fire-engine.example";

describe("fire_engine_search_v2", () => {
  const originalFireEngineUrl = process.env.FIRE_ENGINE_BETA_URL;
  const originalFetch = globalThis.fetch;

  beforeEach(() => {
    jest.resetModules();
    process.env.FIRE_ENGINE_BETA_URL = FIRE_ENGINE_URL;
  });

  afterEach(() => {
    if (originalFireEngineUrl === undefined) {
      delete process.env.FIRE_ENGINE_BETA_URL;
    } else {
      process.env.FIRE_ENGINE_BETA_URL = originalFireEngineUrl;
    }

    globalThis.fetch = originalFetch;
    jest.clearAllMocks();
  });

  it("does not retry when fire engine returns empty results", async () => {
    const fetchMock = jest.fn<typeof fetch>(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        ({
          ok: true,
          json: async () => ({}),
          status: 200,
          statusText: "OK",
          text: async () => "",
        }) as Response,
    );

    globalThis.fetch = fetchMock;

    const { fire_engine_search_v2 } = await import("../fireEngine-v2");

    const response = await fire_engine_search_v2("no-results", {
      numResults: 5,
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(response).toEqual({});
  });
});
